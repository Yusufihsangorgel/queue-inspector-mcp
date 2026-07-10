import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import type { BackendRegistry } from "./backends/index.js";
import { BackendError } from "./types.js";

const SERVER_VERSION = "0.1.1";

const backendArg = z
  .enum(["asynq", "bullmq"])
  .optional()
  .describe("Which backend owns the queue. Optional when the queue name is unique across backends.");
const queueArg = z.string().min(1).describe("Queue name, as reported by list_queues.");
const idArg = z.string().min(1).describe("Job or task id.");

type ToolResult = {
  content: Array<{ type: "text"; text: string }>;
  isError?: boolean;
};

function ok(data: unknown): ToolResult {
  return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
}

function fail(message: string): ToolResult {
  return { content: [{ type: "text", text: message }], isError: true };
}

/** Node syscall failures that mean the Redis server could not be reached. */
const CONNECTION_CODES = new Set(["ECONNREFUSED", "ENOTFOUND", "ETIMEDOUT", "ECONNRESET"]);

/** True for errors that mean Redis could not be reached at all, as opposed to
 *  a bad request. ioredis does not export the classes involved, so match by
 *  name, message and errno. */
function isConnectionError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  if (err.name === "MaxRetriesPerRequestError") return true;
  if (err.message === "Connection is closed.") return true;
  const code = (err as NodeJS.ErrnoException).code;
  return code !== undefined && CONNECTION_CODES.has(code);
}

/** REDIS_URL may embed credentials; strip them before echoing the URL back. */
function withoutCredentials(url: string): string {
  try {
    const parsed = new URL(url);
    parsed.username = "";
    parsed.password = "";
    return parsed.toString();
  } catch {
    return url;
  }
}

export interface ServerOptions {
  registry: BackendRegistry;
  readOnly: boolean;
  /** Used to name the Redis target in connection-failure messages. */
  redisUrl: string;
}

export interface ServerHandle {
  server: McpServer;
  /** Resolves once no tool call is running (immediately when idle), so the
   *  entrypoint can answer piped-in requests before shutting down. */
  drained: () => Promise<void>;
}

export function createServer({ registry, readOnly, redisUrl }: ServerOptions): ServerHandle {
  const server = new McpServer({ name: "queue-inspector-mcp", version: SERVER_VERSION });

  let inFlight = 0;
  const drainWaiters: Array<() => void> = [];

  /** Runs a handler, turning known backend errors and connection failures into
   *  tool errors instead of letting them crash the transport. */
  const guard = async (run: () => Promise<ToolResult>): Promise<ToolResult> => {
    inFlight += 1;
    try {
      return await run();
    } catch (err) {
      if (err instanceof BackendError) return fail(`${err.code}: ${err.message}`);
      if (isConnectionError(err)) {
        return fail(
          `redis_unavailable: cannot reach Redis at ${withoutCredentials(redisUrl)}; ` +
            "check that it is running and that REDIS_URL is correct",
        );
      }
      return fail(err instanceof Error ? err.message : String(err));
    } finally {
      inFlight -= 1;
      if (inFlight === 0) for (const wake of drainWaiters.splice(0)) wake();
    }
  };

  server.registerTool(
    "list_queues",
    {
      title: "List queues",
      description:
        "List every queue the inspector can see, tagged with its backend (asynq or bullmq).",
      inputSchema: { backend: backendArg },
      annotations: { readOnlyHint: true, openWorldHint: false },
    },
    async ({ backend }) =>
      guard(async () => {
        const queues = await registry.allQueues();
        const filtered = backend ? queues.filter((q) => q.backend === backend) : queues;
        return ok({ count: filtered.length, queues: filtered });
      }),
  );

  server.registerTool(
    "queue_stats",
    {
      title: "Queue stats",
      description:
        "Report the number of jobs in each state for a queue, using the backend's own state names.",
      inputSchema: { queue: queueArg, backend: backendArg },
      annotations: { readOnlyHint: true, openWorldHint: false },
    },
    async ({ queue, backend }) =>
      guard(async () => {
        const b = await registry.resolve(queue, backend);
        return ok({ queue, backend: b.name, states: await b.stats(queue) });
      }),
  );

  server.registerTool(
    "list_jobs",
    {
      title: "List jobs",
      description:
        "List jobs in a given state (paged). Returns id, type, attempts and a truncated last error. " +
        "Valid states depend on the backend: asynq uses pending/active/scheduled/retry/archived/completed; " +
        "bullmq uses waiting/active/delayed/prioritized/waiting-children/paused/completed/failed.",
      inputSchema: {
        queue: queueArg,
        state: z.string().min(1).describe("State to list, e.g. \"failed\" (bullmq) or \"archived\" (asynq)."),
        backend: backendArg,
        offset: z.number().int().min(0).default(0).describe("Number of jobs to skip."),
        limit: z.number().int().min(1).max(200).default(20).describe("Maximum jobs to return."),
      },
      annotations: { readOnlyHint: true, openWorldHint: false },
    },
    async ({ queue, state, backend, offset, limit }) =>
      guard(async () => {
        const b = await registry.resolve(queue, backend);
        const jobs = await b.listJobs(queue, state, { offset, limit });
        return ok({ queue, backend: b.name, state, offset, limit, count: jobs.length, jobs });
      }),
  );

  server.registerTool(
    "get_job",
    {
      title: "Get job",
      description:
        "Fetch full detail for one job: payload, attempts, retry ceiling, last error and timestamps. " +
        "Binary payloads are returned base64-encoded and flagged.",
      inputSchema: { queue: queueArg, id: idArg, backend: backendArg },
      annotations: { readOnlyHint: true, openWorldHint: false },
    },
    async ({ queue, id, backend }) =>
      guard(async () => {
        const b = await registry.resolve(queue, backend);
        const job = await b.getJob(queue, id);
        if (!job) return fail(`job_not_found: no job "${id}" in queue "${queue}"`);
        return ok(job);
      }),
  );

  if (!readOnly) {
    server.registerTool(
      "retry_job",
      {
        title: "Retry job",
        description:
          "Move a failed or dead job back to the pending/wait queue so it runs again. " +
          "Faithfully replicates the backend's own retry (asynq Inspector.RunTask, bullmq Job.retry).",
        inputSchema: { queue: queueArg, id: idArg, backend: backendArg },
        annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
      },
      async ({ queue, id, backend }) =>
        guard(async () => {
          const b = await registry.resolve(queue, backend);
          return ok(await b.retryJob(queue, id));
        }),
    );

    server.registerTool(
      "delete_job",
      {
        title: "Delete job",
        description:
          "Permanently delete a job from a queue. Active jobs cannot be deleted. " +
          "Faithfully replicates the backend's own delete (asynq Inspector.DeleteTask, bullmq Job.remove).",
        inputSchema: { queue: queueArg, id: idArg, backend: backendArg },
        annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: false },
      },
      async ({ queue, id, backend }) =>
        guard(async () => {
          const b = await registry.resolve(queue, backend);
          return ok(await b.deleteJob(queue, id));
        }),
    );
  }

  return {
    server,
    drained: () =>
      inFlight === 0 ? Promise.resolve() : new Promise((resolve) => drainWaiters.push(resolve)),
  };
}
