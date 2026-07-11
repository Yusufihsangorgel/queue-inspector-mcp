import type { Redis } from "ioredis";
import {
  BackendError,
  type JobDetail,
  type JobSummary,
  type PageOpts,
  type QueueBackend,
  type StateCounts,
} from "../types.js";
import { decodePayload, isoFromUnixSeconds, truncate } from "../format.js";

// State names as Sidekiq models them in Redis. "enqueued" is the per-queue
// list; "scheduled", "retry" and "dead" are global sorted sets shared by every
// queue. Sidekiq keeps no in-Redis "active" or "completed" state: an in-flight
// job is popped off its list and tracked only in per-process heartbeat hashes,
// which are out of scope for a queue inspector.
const STATES = ["enqueued", "scheduled", "retry", "dead"] as const;
type State = (typeof STATES)[number];

// The three global sorted sets and their Redis key names. The "scheduled" state
// is backed by the key `schedule` (singular), as Sidekiq names it.
const ZSET_KEY = { scheduled: "schedule", retry: "retry", dead: "dead" } as const;
type ZsetState = keyof typeof ZSET_KEY;
const ZSET_STATES = Object.keys(ZSET_KEY) as ZsetState[];
// Scheduled and retry jobs are read soonest-first; the dead set is shown
// most-recent-first, matching Sidekiq's own Web UI.
const REVERSED_ZSETS = new Set<ZsetState>(["dead"]);

// Sidekiq's retry ceiling when a job carries `retry: true` rather than an
// explicit integer (Sidekiq::JobRetry::DEFAULT_MAX_RETRY_ATTEMPTS).
const DEFAULT_MAX_RETRIES = 25;

/** The subset of a Sidekiq job hash this adapter reads. Every field is
 *  optional: a job in flight carries no failure fields, and a scheduled job has
 *  no `enqueued_at`. */
interface JobMsg {
  jid?: string;
  queue?: string;
  class?: string;
  args?: unknown;
  retry?: boolean | number;
  created_at?: number;
  enqueued_at?: number;
  retry_count?: number;
  error_message?: string;
  error_class?: string;
  failed_at?: number;
  retried_at?: number;
}

interface ZsetEntry {
  raw: string;
  msg: JobMsg;
}

interface Located extends ZsetEntry {
  state: State;
}

/**
 * Adapter for queues managed by Sidekiq (github.com/sidekiq/sidekiq).
 *
 * Key layout, verified against a running Sidekiq 6.5 worker:
 *   queues                SET   of queue names with pending jobs
 *   queue:<name>          LIST  of full job JSON (LPUSH on enqueue, BRPOP by the
 *                               worker, so index 0 is the newest job)
 *   schedule / retry /    ZSET  full job JSON -> unix seconds (scheduled-for,
 *     dead                      next-retry, or death time)
 *
 * Unlike BullMQ, Sidekiq stores each job's entire JSON inline in the list or
 * sorted-set member rather than as an id reference to a separate hash; the id
 * (`jid`) and the owning queue live inside that JSON. The schedule/retry/dead
 * sets are global and mix jobs from every queue, so per-queue views of those
 * states read the members and filter on the parsed `queue` field.
 */
export class SidekiqBackend implements QueueBackend {
  readonly name = "sidekiq" as const;
  readonly states = STATES;

  // Sidekiq's default install uses bare keys (queues, queue:default, schedule).
  // A prefix supports namespaced deployments (redis-namespace), which prepend
  // `<prefix>:` to every key.
  constructor(
    private readonly redis: Redis,
    private readonly prefix = "",
  ) {}

  private key(base: string): string {
    return this.prefix ? `${this.prefix}:${base}` : base;
  }

  private queuesKey(): string {
    return this.key("queues");
  }

  private queueKey(queue: string): string {
    return this.key(`queue:${queue}`);
  }

  private zsetKey(state: ZsetState): string {
    return this.key(ZSET_KEY[state]);
  }

  async detectQueues(): Promise<string[]> {
    const names = new Set(await this.redis.smembers(this.queuesKey()));
    // The `queues` set only records queues with immediately enqueued jobs;
    // scheduled jobs go straight to the global `schedule` set without touching
    // it, and a queue can hold only retry or dead jobs. Union in the queues
    // named inside those sets so every inspectable job is reachable. These are
    // Sidekiq's future and exception sets, bounded in normal operation (the
    // dead set is capped at 10k entries by default).
    for (const state of ZSET_STATES) {
      for (const { msg } of await this.readZset(state)) {
        if (msg.queue) names.add(msg.queue);
      }
    }
    return [...names].sort();
  }

  private async assertQueue(queue: string): Promise<void> {
    // Fast path: the canonical set and the queue's own list are single reads and
    // cover every queue that has ever been enqueued to. Only a scheduled-only
    // queue falls through to the O(n) scan of the global sets.
    if (await this.redis.sismember(this.queuesKey(), queue)) return;
    if (await this.redis.exists(this.queueKey(queue))) return;
    if ((await this.detectQueues()).includes(queue)) return;
    throw new BackendError(`sidekiq queue "${queue}" was not found`, "queue_not_found");
  }

  async stats(queue: string): Promise<StateCounts> {
    await this.assertQueue(queue);
    const counts: StateCounts = { enqueued: await this.redis.llen(this.queueKey(queue)) };
    for (const state of ZSET_STATES) {
      counts[state] = (await this.readZset(state)).filter((e) => e.msg.queue === queue).length;
    }
    return counts;
  }

  private assertState(state: string): State {
    if (!(STATES as readonly string[]).includes(state)) {
      throw new BackendError(
        `unknown sidekiq state "${state}"; expected one of ${STATES.join(", ")}`,
        "invalid_state",
      );
    }
    return state as State;
  }

  async listJobs(queue: string, rawState: string, page: PageOpts): Promise<JobSummary[]> {
    await this.assertQueue(queue);
    const state = this.assertState(rawState);

    if (state === "enqueued") {
      // The list is already scoped to this queue, so page directly in Redis.
      const stop = page.offset + page.limit - 1;
      const raws = await this.redis.lrange(this.queueKey(queue), page.offset, stop);
      return this.summaries(raws.map((raw) => this.parseMsg(raw)), state);
    }

    // Global set: read the members, keep this queue's, then page over the
    // filtered result. Paging cannot be pushed into Redis because the offsets
    // are relative to the queue's own jobs, not the shared set.
    const mine = (await this.readZset(state)).filter((e) => e.msg.queue === queue);
    const slice = mine.slice(page.offset, page.offset + page.limit);
    return this.summaries(
      slice.map((e) => e.msg),
      state,
    );
  }

  async getJob(queue: string, id: string): Promise<JobDetail | null> {
    await this.assertQueue(queue);
    const found = await this.locate(queue, id);
    if (!found) return null;
    const { state, msg, raw } = found;

    const decoded = decodePayload(Buffer.from(this.payloadJson(msg), "utf8"));
    const timestamps: Record<string, string | null> = {
      createdAt: isoFromUnixSeconds(msg.created_at),
      enqueuedAt: isoFromUnixSeconds(msg.enqueued_at),
      failedAt: isoFromUnixSeconds(msg.failed_at),
      retriedAt: isoFromUnixSeconds(msg.retried_at),
    };
    if (state !== "enqueued") {
      // The score carries the meaningful time for a set member: when a scheduled
      // or retry job next runs, or when a dead job was buried.
      const score = await this.redis.zscore(this.zsetKey(state), raw);
      const at = score ? isoFromUnixSeconds(Number(score)) : null;
      timestamps[state === "dead" ? "diedAt" : "nextRunAt"] = at;
    }

    return {
      ...this.toSummary(msg, state),
      queue,
      backend: this.name,
      fullError: msg.error_message ?? null,
      timestamps,
      ...decoded,
    };
  }

  /** Finds a job by jid across the queue's list and the three global sets. The
   *  global sets hold every queue's jobs, so a match there must also name this
   *  queue. Mirrors Sidekiq's own find_job, an intentionally O(n) scan. */
  private async locate(queue: string, id: string): Promise<Located | null> {
    const listRaws = await this.redis.lrange(this.queueKey(queue), 0, -1);
    for (const raw of listRaws) {
      const msg = this.parseMsg(raw);
      if (msg?.jid === id) return { state: "enqueued", raw, msg };
    }
    for (const state of ZSET_STATES) {
      for (const { raw, msg } of await this.readZset(state)) {
        if (msg.jid === id && msg.queue === queue) return { state, raw, msg };
      }
    }
    return null;
  }

  /** Reads a global set in display order and parses each member. Sidekiq stores
   *  the jid inside the JSON, so a member that fails to parse has no recoverable
   *  identity and is dropped rather than surfaced under a synthetic id. */
  private async readZset(state: ZsetState): Promise<ZsetEntry[]> {
    const key = this.zsetKey(state);
    const raws = REVERSED_ZSETS.has(state)
      ? await this.redis.zrevrange(key, 0, -1)
      : await this.redis.zrange(key, 0, -1);
    const out: ZsetEntry[] = [];
    for (const raw of raws) {
      const msg = this.parseMsg(raw);
      if (msg) out.push({ raw, msg });
    }
    return out;
  }

  private summaries(msgs: Array<JobMsg | null>, state: State): JobSummary[] {
    const out: JobSummary[] = [];
    for (const msg of msgs) {
      if (msg) out.push(this.toSummary(msg, state));
    }
    return out;
  }

  private toSummary(msg: JobMsg, state: State): JobSummary {
    return {
      id: msg.jid ?? "",
      type: msg.class ?? "",
      state,
      // A scheduled job has no enqueued_at; created_at is the nearest equivalent.
      enqueuedAt: isoFromUnixSeconds(msg.enqueued_at ?? msg.created_at),
      attempts: typeof msg.retry_count === "number" ? msg.retry_count : 0,
      maxRetries: this.maxRetries(msg.retry),
      lastError: msg.error_message ? truncate(msg.error_message) : null,
    };
  }

  private maxRetries(retry: JobMsg["retry"]): number | null {
    if (typeof retry === "number") return retry;
    if (retry === true) return DEFAULT_MAX_RETRIES;
    if (retry === false) return 0;
    return null;
  }

  /** The job's `args` array is the user payload, mirroring how the Asynq and
   *  BullMQ adapters surface the task data rather than the whole envelope. */
  private payloadJson(msg: JobMsg): string {
    return JSON.stringify(msg.args ?? []);
  }

  private parseMsg(raw: string): JobMsg | null {
    try {
      const value = JSON.parse(raw) as unknown;
      return value && typeof value === "object" ? (value as JobMsg) : null;
    } catch {
      return null;
    }
  }

  async retryJob(queue: string, id: string): Promise<{ ok: true; message: string }> {
    await this.assertQueue(queue);
    const found = await this.locate(queue, id);
    if (!found) {
      throw new BackendError(`sidekiq job "${id}" was not found in queue "${queue}"`, "job_not_found");
    }
    if (found.state !== "retry" && found.state !== "dead") {
      throw new BackendError(
        `sidekiq job "${id}" is in state "${found.state}"; only retry or dead jobs can be requeued`,
        "invalid_state",
      );
    }

    // Faithful to Sidekiq's SortedEntry#retry (RetrySet/DeadSet#retry_all): drop
    // the member, decrement retry_count, then re-enqueue via the same steps as
    // Sidekiq::Client (set enqueued_at, register the queue, LPUSH the job). ZREM
    // returning 0 means the job was retried or deleted concurrently, so we stop
    // rather than push a duplicate onto the queue.
    const removed = await this.redis.zrem(this.zsetKey(found.state), found.raw);
    if (removed === 0) {
      throw new BackendError(
        `sidekiq job "${id}" was no longer in the ${found.state} set; its state changed concurrently`,
        "invalid_state",
      );
    }
    const msg: JobMsg = { ...found.msg };
    if (typeof msg.retry_count === "number") msg.retry_count -= 1;
    msg.enqueued_at = Date.now() / 1000;
    await this.redis.sadd(this.queuesKey(), queue);
    await this.redis.lpush(this.queueKey(queue), JSON.stringify(msg));
    return { ok: true, message: `sidekiq job ${id} requeued to "${queue}"` };
  }

  async deleteJob(queue: string, id: string): Promise<{ ok: true; message: string }> {
    await this.assertQueue(queue);
    const found = await this.locate(queue, id);
    if (!found) {
      throw new BackendError(`sidekiq job "${id}" was not found in queue "${queue}"`, "job_not_found");
    }

    // Faithful to Sidekiq's own delete: LREM the exact member from the queue
    // list (JobRecord#delete) or ZREM it from the set (SortedEntry#delete).
    const removed =
      found.state === "enqueued"
        ? await this.redis.lrem(this.queueKey(queue), 1, found.raw)
        : await this.redis.zrem(this.zsetKey(found.state), found.raw);
    if (removed === 0) {
      throw new BackendError(
        `sidekiq job "${id}" was no longer present in queue "${queue}"`,
        "job_not_found",
      );
    }
    return { ok: true, message: `sidekiq job ${id} deleted from queue "${queue}"` };
  }
}
