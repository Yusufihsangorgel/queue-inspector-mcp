import type { Redis } from "ioredis";
import {
  BackendError,
  type JobDetail,
  type JobSummary,
  type PageOpts,
  type QueueBackend,
  type StateCounts,
} from "../types.js";
import { decodePayload, isoFromMillis, truncate } from "../format.js";
import { scanKeys } from "../redis.js";
import { attachScripts, type Scripting } from "./scripting.js";

// State names as BullMQ itself uses them. "waiting" is the `wait` list;
// "prioritized" and "waiting-children" only hold jobs when priorities or flows
// are used, but are reported for completeness.
const STATES = [
  "waiting",
  "active",
  "delayed",
  "prioritized",
  "waiting-children",
  "paused",
  "completed",
  "failed",
] as const;
type State = (typeof STATES)[number];

// Which Redis structure backs each state, and how the state maps to a key suffix.
const LIST_STATES: Record<string, string> = {
  waiting: "wait",
  active: "active",
  paused: "paused",
};
const ZSET_STATES: Record<string, string> = {
  delayed: "delayed",
  prioritized: "prioritized",
  "waiting-children": "waiting-children",
  completed: "completed",
  failed: "failed",
};
// Terminal sets are shown most-recent-first; scheduling sets soonest-first.
const REVERSED_ZSETS = new Set(["completed", "failed"]);

// The classic wait-list marker id; never a real job.
const MARKER = "0";

const JOB_FIELDS = [
  "name",
  "data",
  "opts",
  "timestamp",
  "delay",
  "processedOn",
  "finishedOn",
  "atm",
  "failedReason",
] as const;

interface JobHash {
  name: string;
  data: string;
  opts: string;
  timestamp: string | null;
  delay: string | null;
  processedOn: string | null;
  finishedOn: string | null;
  atm: string | null;
  failedReason: string | null;
}

/**
 * Adapter for queues managed by BullMQ (github.com/taskforcesh/bullmq).
 *
 * Key layout, verified against a running BullMQ worker:
 *   bull:<q>:meta                         HASH  queue metadata (version, paused)
 *   bull:<q>:wait / :active / :paused     LIST  of job ids
 *   bull:<q>:delayed / :prioritized /     ZSET  of job ids
 *     :completed / :failed
 *   bull:<q>:<id>                         HASH  { name, data, opts, timestamp,
 *                                                 atm, failedReason, ... }
 * Mutations run BullMQ's own vendored scripts; see ./lua and ./scripting.ts.
 */
export class BullmqBackend implements QueueBackend {
  readonly name = "bullmq" as const;
  readonly states = STATES;
  private readonly scripts: Scripting;

  constructor(
    private readonly redis: Redis,
    private readonly prefix = "bull",
  ) {
    this.scripts = attachScripts(redis);
  }

  private key(queue: string, suffix: string): string {
    return `${this.prefix}:${queue}:${suffix}`;
  }

  private jobKey(queue: string, id: string): string {
    return `${this.prefix}:${queue}:${id}`;
  }

  async detectQueues(): Promise<string[]> {
    const metaKeys = await scanKeys(this.redis, `${this.prefix}:*:meta`);
    const head = `${this.prefix}:`;
    const names = metaKeys
      .map((k) => k.slice(head.length, k.length - ":meta".length))
      .filter(Boolean);
    return [...new Set(names)].sort();
  }

  private async assertQueue(queue: string): Promise<void> {
    const exists = await this.redis.exists(this.key(queue, "meta"));
    if (!exists) {
      throw new BackendError(`bullmq queue "${queue}" was not found`, "queue_not_found");
    }
  }

  async stats(queue: string): Promise<StateCounts> {
    await this.assertQueue(queue);
    const counts: StateCounts = {};
    for (const state of STATES) {
      if (state in LIST_STATES) {
        counts[state] = await this.redis.llen(this.key(queue, LIST_STATES[state]!));
      } else {
        counts[state] = await this.redis.zcard(this.key(queue, ZSET_STATES[state]!));
      }
    }
    return counts;
  }

  private assertState(state: string): State {
    if (!(STATES as readonly string[]).includes(state)) {
      throw new BackendError(
        `unknown bullmq state "${state}"; expected one of ${STATES.join(", ")}`,
        "invalid_state",
      );
    }
    return state as State;
  }

  private async idsForState(queue: string, state: State, page: PageOpts): Promise<string[]> {
    const stop = page.offset + page.limit - 1;
    if (state in LIST_STATES) {
      const ids = await this.redis.lrange(this.key(queue, LIST_STATES[state]!), page.offset, stop);
      return ids.filter((id) => id !== MARKER);
    }
    const key = this.key(queue, ZSET_STATES[state]!);
    return REVERSED_ZSETS.has(state)
      ? this.redis.zrevrange(key, page.offset, stop)
      : this.redis.zrange(key, page.offset, stop);
  }

  async listJobs(queue: string, rawState: string, page: PageOpts): Promise<JobSummary[]> {
    await this.assertQueue(queue);
    const state = this.assertState(rawState);
    const ids = await this.idsForState(queue, state, page);

    const summaries: JobSummary[] = [];
    for (const id of ids) {
      const hash = await this.readHash(queue, id);
      if (hash) summaries.push(this.toSummary(id, state, hash));
    }
    return summaries;
  }

  async getJob(queue: string, id: string): Promise<JobDetail | null> {
    await this.assertQueue(queue);
    const hash = await this.readHash(queue, id);
    if (!hash) return null;
    const state = await this.resolveState(queue, id);

    const decoded = decodePayload(Buffer.from(hash.data ?? "", "utf8"));
    const timestamps: Record<string, string | null> = {
      enqueuedAt: isoFromMillis(num(hash.timestamp)),
      processedAt: isoFromMillis(num(hash.processedOn)),
      finishedAt: isoFromMillis(num(hash.finishedOn)),
    };
    if (state === "delayed") {
      const base = num(hash.timestamp) ?? 0;
      const delay = num(hash.delay) ?? 0;
      timestamps.processAt = isoFromMillis(base + delay);
    }

    const summary = this.toSummary(id, state, hash);
    return {
      ...summary,
      queue,
      backend: this.name,
      fullError: hash.failedReason || null,
      timestamps,
      ...decoded,
    };
  }

  private async readHash(queue: string, id: string): Promise<JobHash | null> {
    const values = await this.redis.hmget(this.jobKey(queue, id), ...JOB_FIELDS);
    if (values.every((v) => v === null)) return null;
    const record = {} as Record<(typeof JOB_FIELDS)[number], string | null>;
    JOB_FIELDS.forEach((field, i) => {
      record[field] = values[i] ?? null;
    });
    return {
      name: record.name ?? "",
      data: record.data ?? "",
      opts: record.opts ?? "",
      timestamp: record.timestamp,
      delay: record.delay,
      processedOn: record.processedOn,
      finishedOn: record.finishedOn,
      atm: record.atm,
      failedReason: record.failedReason,
    };
  }

  private toSummary(id: string, state: State, hash: JobHash): JobSummary {
    return {
      id,
      type: hash.name,
      state,
      enqueuedAt: isoFromMillis(num(hash.timestamp)),
      attempts: num(hash.atm) ?? 0,
      maxRetries: this.maxRetries(hash.opts),
      lastError: hash.failedReason ? truncate(hash.failedReason) : null,
    };
  }

  private maxRetries(optsJson: string): number | null {
    try {
      const opts = JSON.parse(optsJson) as { attempts?: number };
      return typeof opts.attempts === "number" ? opts.attempts : null;
    } catch {
      return null;
    }
  }

  private isLifo(optsJson: string): boolean {
    try {
      return Boolean((JSON.parse(optsJson) as { lifo?: boolean }).lifo);
    } catch {
      return false;
    }
  }

  /** Determines a job's current state by membership, mirroring BullMQ's own
   *  getState: check the terminal and scheduling sets, then the lists. */
  private async resolveState(queue: string, id: string): Promise<State> {
    for (const state of ["completed", "failed", "delayed", "prioritized", "waiting-children"] as const) {
      if ((await this.redis.zscore(this.key(queue, ZSET_STATES[state]!), id)) !== null) return state;
    }
    for (const state of ["active", "paused", "waiting"] as const) {
      if ((await this.redis.lpos(this.key(queue, LIST_STATES[state]!), id)) !== null) return state;
    }
    // Present as a hash but not in any structure: being processed with a lock,
    // or momentarily between states. BullMQ reports this as "active".
    return "active";
  }

  async retryJob(queue: string, id: string): Promise<{ ok: true; message: string }> {
    await this.assertQueue(queue);
    const hash = await this.readHash(queue, id);
    if (!hash) throw new BackendError(`bullmq job "${id}" was not found in queue "${queue}"`, "job_not_found");

    const state = await this.resolveState(queue, id);
    if (state !== "failed" && state !== "completed") {
      throw new BackendError(
        `bullmq job "${id}" is in state "${state}"; only failed or completed jobs can be retried`,
        "invalid_state",
      );
    }

    const pushCmd = `${this.isLifo(hash.opts) ? "R" : "L"}PUSH`;
    const propVal = state === "failed" ? "failedReason" : "returnvalue";
    // Keys and args exactly as BullMQ's Scripts.reprocessJob builds them.
    const result = await this.scripts.reprocessJob(
      [
        this.jobKey(queue, id),
        this.key(queue, "events"),
        this.key(queue, state),
        this.key(queue, "wait"),
        this.key(queue, "meta"),
        this.key(queue, "paused"),
        this.key(queue, "active"),
        this.key(queue, "marker"),
      ],
      [id, pushCmd, propVal, state, "0", "0"],
    );
    switch (result) {
      case 1:
        return { ok: true, message: `bullmq job ${id} moved back to wait in queue "${queue}"` };
      case -1:
        throw new BackendError(`bullmq job "${id}" was not found in queue "${queue}"`, "job_not_found");
      case -3:
        throw new BackendError(
          `bullmq job "${id}" was not in the ${state} set; its state changed concurrently`,
          "invalid_state",
        );
      default:
        throw new BackendError(`bullmq retry returned unexpected code ${result}`, "invalid_state");
    }
  }

  async deleteJob(queue: string, id: string): Promise<{ ok: true; message: string }> {
    await this.assertQueue(queue);
    const hash = await this.readHash(queue, id);
    if (!hash) throw new BackendError(`bullmq job "${id}" was not found in queue "${queue}"`, "job_not_found");

    // removeChildren = 0: never cascade-delete a flow's children from here.
    const result = await this.scripts.removeJob(
      [this.jobKey(queue, id), this.key(queue, "repeat")],
      [id, 0, `${this.prefix}:${queue}:`],
    );
    switch (result) {
      case 1:
        return { ok: true, message: `bullmq job ${id} deleted from queue "${queue}"` };
      case 0:
        throw new BackendError(
          `bullmq job "${id}" is locked (active) and cannot be deleted; let it finish or move it first`,
          "invalid_state",
        );
      case -8:
        throw new BackendError(
          `bullmq job "${id}" is a job-scheduler entry and cannot be deleted this way`,
          "not_allowed",
        );
      default:
        throw new BackendError(`bullmq delete returned unexpected code ${result}`, "invalid_state");
    }
  }
}

function num(value: string | null): number | null {
  if (value === null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
