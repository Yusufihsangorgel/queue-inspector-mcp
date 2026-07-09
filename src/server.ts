import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import type { BackendRegistry } from "./backends/index.js";
import { BackendError } from "./types.js";

const SERVER_VERSION = "0.1.0";

const backendArg = z
  .enum(["asynq", "bullmq", "sidekiq"])
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

/** Runs a handler, turning known backend errors and connection failures into
 *  tool errors instead of letting them crash the transport. */
async function guard(run: () => Promise<ToolResult>): Promise<ToolResult> {
  try {
    return await run();
  } catch (err) {
    if (err instanceof BackendError) return fail(`${err.code}: ${err.message}`);
    return fail(err instanceof Error ? err.message : String(err));
  }
}

export interface ServerOptions {
  registry: BackendRegistry;
  readOnly: boolean;
}

export function createServer({ registry, readOnly }: ServerOptions): McpServer {
  const server = new McpServer({ name: "queue-inspector-mcp", version: SERVER_VERSION });

  server.registerTool(
    "list_queues",
    {
      title: "List queues",
      description:
        "List every queue the inspector can see, tagged with its backend (asynq, bullmq or sidekiq).",
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
        "bullmq uses waiting/active/delayed/prioritized/waiting-children/paused/completed/failed; " +
        "sidekiq uses enqueued/scheduled/retry/dead (scheduled/retry/dead are cluster-global sets).",
      inputSchema: {
        queue: queueArg,
        state: z.string().min(1).describe("State to list, e.g. \"failed\" (bullmq), \"archived\" (asynq) or \"dead\" (sidekiq)."),
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
          "Faithfully replicates the backend's own retry (asynq Inspector.RunTask, bullmq Job.retry, " +
          "sidekiq SortedEntry#retry).",
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
          "Faithfully replicates the backend's own delete (asynq Inspector.DeleteTask, bullmq Job.remove, " +
          "sidekiq JobRecord#delete / JobSet#delete_by_value).",
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

  return server;
}
