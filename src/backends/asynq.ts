import type { Redis } from "ioredis";
import {
  BackendError,
  type JobDetail,
  type JobSummary,
  type PageOpts,
  type QueueBackend,
  type StateCounts,
} from "../types.js";
import { decodePayload, isoFromMillis, isoFromUnixSeconds, truncate } from "../format.js";
import { decodeTaskMessage, type AsynqTaskMessage } from "./asynq-proto.js";
import { attachScripts, type Scripting } from "./scripting.js";

// State names as Asynq itself uses them, in a natural lifecycle order. Group
// aggregation ("aggregating") is an advanced feature not surfaced in v0.1.
const STATES = ["pending", "active", "scheduled", "retry", "archived", "completed"] as const;
type State = (typeof STATES)[number];

const LIST_STATES = new Set<State>(["pending", "active"]);

/**
 * Adapter for queues managed by Asynq (github.com/hibiken/asynq).
 *
 * Key layout, verified against a running Asynq worker:
 *   asynq:queues                     SET   of queue names
 *   asynq:{<q>}:pending / :active    LIST  of task ids
 *   asynq:{<q>}:scheduled/:retry/    ZSET  task id -> unix seconds (process-at,
 *     :archived/:completed                 next retry, last failure, or expiry)
 *   asynq:{<q>}:t:<id>               HASH  { msg: protobuf TaskMessage, state,
 *                                            pending_since }
 * Task metadata is a protobuf TaskMessage; see ./asynq-proto.ts.
 */
export class AsynqBackend implements QueueBackend {
  readonly name = "asynq" as const;
  readonly states = STATES;
  private readonly scripts: Scripting;

  constructor(
    private readonly redis: Redis,
    private readonly prefix = "asynq",
  ) {
    this.scripts = attachScripts(redis);
  }

  private queuesKey(): string {
    return `${this.prefix}:queues`;
  }

  private queuePrefix(queue: string): string {
    return `${this.prefix}:{${queue}}:`;
  }

  private taskKey(queue: string, id: string): string {
    return `${this.queuePrefix(queue)}t:${id}`;
  }

  private stateKey(queue: string, state: State): string {
    return `${this.queuePrefix(queue)}${state}`;
  }

  async detectQueues(): Promise<string[]> {
    const queues = await this.redis.smembers(this.queuesKey());
    return queues.sort();
  }

  private async assertQueue(queue: string): Promise<void> {
    const known = await this.redis.sismember(this.queuesKey(), queue);
    if (!known) {
      throw new BackendError(`asynq queue "${queue}" was not found`, "queue_not_found");
    }
  }

  async stats(queue: string): Promise<StateCounts> {
    await this.assertQueue(queue);
    const counts: StateCounts = {};
    for (const state of STATES) {
      const key = this.stateKey(queue, state);
      counts[state] = LIST_STATES.has(state)
        ? await this.redis.llen(key)
        : await this.redis.zcard(key);
    }
    return counts;
  }

  private assertState(state: string): State {
    if (!(STATES as readonly string[]).includes(state)) {
      throw new BackendError(
        `unknown asynq state "${state}"; expected one of ${STATES.join(", ")}`,
        "invalid_state",
      );
    }
    return state as State;
  }

  async listJobs(queue: string, rawState: string, page: PageOpts): Promise<JobSummary[]> {
    await this.assertQueue(queue);
    const state = this.assertState(rawState);
    const key = this.stateKey(queue, state);
    const stop = page.offset + page.limit - 1;
    const ids = LIST_STATES.has(state)
      ? await this.redis.lrange(key, page.offset, stop)
      : await this.redis.zrange(key, page.offset, stop);

    const summaries: JobSummary[] = [];
    for (const id of ids) {
      const detail = await this.readTask(queue, id, state);
      if (detail) summaries.push(detail);
    }
    return summaries;
  }

  async getJob(queue: string, id: string): Promise<JobDetail | null> {
    await this.assertQueue(queue);
    const state = (await this.redis.hget(this.taskKey(queue, id), "state")) as State | null;
    if (state === null) return null;
    return this.readTaskDetail(queue, id, state);
  }

  // A corrupt or partially written `msg` throws out of the decoder. Left
  // unguarded it would reject the whole listJobs/getJob call, making an entire
  // page unviewable over one bad task and hiding which task is at fault. Decode
  // failures are isolated here so the row still surfaces with its id.
  private decodeMsg(raw: Buffer): AsynqTaskMessage | { error: string } {
    try {
      return decodeTaskMessage(raw);
    } catch (err) {
      return { error: err instanceof Error ? err.message : String(err) };
    }
  }

  private async readTask(queue: string, id: string, state: State): Promise<JobSummary | null> {
    const raw = await this.redis.hgetBuffer(this.taskKey(queue, id), "msg");
    if (raw === null) return null;
    const msg = this.decodeMsg(raw);
    if ("error" in msg) {
      return {
        id,
        type: "(unreadable)",
        state,
        enqueuedAt: null,
        attempts: null,
        maxRetries: null,
        lastError: `could not decode task message: ${msg.error}`,
      };
    }
    return {
      id,
      type: msg.type,
      state,
      enqueuedAt: await this.enqueuedAt(queue, id, state),
      attempts: msg.retried,
      maxRetries: msg.maxRetry,
      lastError: msg.errorMsg ? truncate(msg.errorMsg) : null,
    };
  }

  private async readTaskDetail(queue: string, id: string, state: State): Promise<JobDetail | null> {
    const raw = await this.redis.hgetBuffer(this.taskKey(queue, id), "msg");
    if (raw === null) return null;
    const msg = this.decodeMsg(raw);
    if ("error" in msg) {
      const detail = `could not decode task message: ${msg.error}`;
      return {
        id,
        queue,
        backend: this.name,
        type: "(unreadable)",
        state,
        enqueuedAt: null,
        attempts: null,
        maxRetries: null,
        lastError: detail,
        fullError: detail,
        timestamps: {},
        payload: "",
        payloadEncoding: "utf8",
        payloadBytes: 0,
        payloadTruncated: false,
      };
    }
    const decoded = decodePayload(msg.payload);

    const timestamps: Record<string, string | null> = {
      enqueuedAt: await this.enqueuedAt(queue, id, state),
      lastFailedAt: isoFromUnixSeconds(msg.lastFailedAtUnix),
      completedAt: isoFromUnixSeconds(msg.completedAtUnix),
      deadline: isoFromUnixSeconds(msg.deadlineUnix),
    };
    if (state === "scheduled" || state === "retry") {
      const score = await this.redis.zscore(this.stateKey(queue, state), id);
      timestamps.nextProcessAt = score ? isoFromUnixSeconds(Number(score)) : null;
    }

    return {
      id,
      queue,
      backend: this.name,
      type: msg.type,
      state,
      enqueuedAt: timestamps.enqueuedAt ?? null,
      attempts: msg.retried,
      maxRetries: msg.maxRetry,
      lastError: msg.errorMsg ? truncate(msg.errorMsg) : null,
      fullError: msg.errorMsg || null,
      timestamps,
      ...decoded,
    };
  }

  /** Asynq's TaskMessage carries no enqueue time; a pending task does record a
   *  `pending_since` field in nanoseconds, which is the closest equivalent.
   *  Routed through isoFromMillis so a malformed value yields null rather than a
   *  RangeError out of Date.toISOString(). */
  private async enqueuedAt(queue: string, id: string, state: State): Promise<string | null> {
    if (state !== "pending") return null;
    const ns = await this.redis.hget(this.taskKey(queue, id), "pending_since");
    if (!ns) return null;
    return isoFromMillis(Number(ns) / 1e6);
  }

  async retryJob(queue: string, id: string): Promise<{ ok: true; message: string }> {
    await this.assertQueue(queue);
    const prefix = this.queuePrefix(queue);
    const result = await this.scripts.runTask(
      [this.taskKey(queue, id), `${prefix}pending`, `${prefix}groups`],
      [id, prefix, `${prefix}g:`],
    );
    switch (result) {
      case 1:
        return { ok: true, message: `asynq task ${id} moved to pending in queue "${queue}"` };
      case 0:
        throw new BackendError(`asynq task "${id}" was not found in queue "${queue}"`, "job_not_found");
      case -1:
        throw new BackendError(`asynq task "${id}" is currently active and cannot be retried`, "invalid_state");
      case -2:
        throw new BackendError(`asynq task "${id}" is already pending`, "invalid_state");
      default:
        throw new BackendError(`asynq retry returned unexpected code ${result}`, "invalid_state");
    }
  }

  async deleteJob(queue: string, id: string): Promise<{ ok: true; message: string }> {
    await this.assertQueue(queue);
    const prefix = this.queuePrefix(queue);
    const result = await this.scripts.deleteTask(
      [this.taskKey(queue, id), `${prefix}groups`],
      [id, prefix, `${prefix}g:`],
    );
    switch (result) {
      case 1:
        return { ok: true, message: `asynq task ${id} deleted from queue "${queue}"` };
      case 0:
        throw new BackendError(`asynq task "${id}" was not found in queue "${queue}"`, "job_not_found");
      case -1:
        throw new BackendError(`asynq task "${id}" is currently active and cannot be deleted`, "invalid_state");
      default:
        throw new BackendError(`asynq delete returned unexpected code ${result}`, "invalid_state");
    }
  }
}
