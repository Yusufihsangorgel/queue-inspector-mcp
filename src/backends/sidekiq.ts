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
import { attachScripts, type Scripting } from "./scripting.js";

// State names as Sidekiq itself uses them. `enqueued` is per-queue; the other
// three are cluster-global sorted sets shared by every queue.
const STATES = ["enqueued", "scheduled", "retry", "dead"] as const;
type State = (typeof STATES)[number];
type GlobalState = Exclude<State, "enqueued">;

// Each global state maps to a single, unprefixed Redis sorted set. Note the key
// for `scheduled` is the singular `schedule`, matching Sidekiq's own naming.
const GLOBAL_KEY: Record<GlobalState, string> = {
  scheduled: "schedule",
  retry: "retry",
  dead: "dead",
};

// The dead set is a terminal set, shown most-recent-first (highest score). The
// scheduling sets (scheduled/retry) are shown soonest-first (lowest score),
// mirroring how the Asynq/BullMQ adapters order their equivalents.
const REVERSED = new Set<GlobalState>(["dead"]);

// How many members to pull per round-trip when scanning a list or set for a
// specific jid. Sidekiq has no jid index, so lookups walk the structure; this
// keeps a large set from being loaded all at once.
const SCAN_CHUNK = 200;

/** Shape of a Sidekiq job payload, which is the JSON stored as each list/set
 *  member. Only the fields this adapter reads are typed; the rest are preserved
 *  verbatim when a job is re-enqueued. */
interface SidekiqJob {
  class?: string;
  args?: unknown[];
  queue?: string;
  jid?: string;
  /** `true` (server default), `false`, or an explicit integer ceiling. */
  retry?: boolean | number;
  created_at?: number;
  enqueued_at?: number;
  error_message?: string;
  error_class?: string;
  failed_at?: number;
  retried_at?: number;
  retry_count?: number;
  [key: string]: unknown;
}

/**
 * Adapter for queues managed by Sidekiq (github.com/sidekiq/sidekiq), verified
 * against Sidekiq 6.5.12.
 *
 * Key layout, verified against real Sidekiq output (see verify/sidekiq-producer):
 *   queues            SET   of queue names
 *   queue:<name>      LIST  of job JSON (Sidekiq LPUSHes; workers BRPOP)
 *   schedule          ZSET  job JSON -> run-at         (cluster-global)
 *   retry             ZSET  job JSON -> next-retry-at  (cluster-global)
 *   dead              ZSET  job JSON -> died-at        (cluster-global)
 * Each member is the full job JSON: { class, args, queue, jid, created_at,
 * enqueued_at, retry, retry_count, error_message, error_class, failed_at,
 * retried_at }. Sidekiq stores no per-job hash and no jid index, so a job is
 * found by scanning the structure — exactly as Sidekiq's own API does.
 *
 * Unlike Asynq/BullMQ, `scheduled`/`retry`/`dead` are NOT per-queue: they are a
 * single global set each. `queue_stats` reports their global totals (identical
 * across queues), and listing them ignores the queue argument. This reflects
 * Sidekiq's real model rather than inventing per-queue dead sets.
 *
 * By default Sidekiq keys are unprefixed. A `prefix` (SIDEKIQ_PREFIX) mirrors a
 * redis-namespace deployment, prepending `<prefix>:` to every key.
 */
export class SidekiqBackend implements QueueBackend {
  readonly name = "sidekiq" as const;
  readonly states = STATES;
  private readonly scripts: Scripting;

  constructor(
    private readonly redis: Redis,
    private readonly prefix = "",
  ) {
    this.scripts = attachScripts(redis);
  }

  private key(base: string): string {
    return this.prefix ? `${this.prefix}:${base}` : base;
  }

  private queuesKey(): string {
    return this.key("queues");
  }

  private queueListKey(queue: string): string {
    return this.key(`queue:${queue}`);
  }

  private globalKey(state: GlobalState): string {
    return this.key(GLOBAL_KEY[state]);
  }

  async detectQueues(): Promise<string[]> {
    const queues = await this.redis.smembers(this.queuesKey());
    return queues.sort();
  }

  private async assertQueue(queue: string): Promise<void> {
    const known = await this.redis.sismember(this.queuesKey(), queue);
    if (!known) {
      throw new BackendError(`sidekiq queue "${queue}" was not found`, "queue_not_found");
    }
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

  /**
   * `enqueued` is this queue's own list length; `scheduled`/`retry`/`dead` are
   * the cluster-global set cardinalities, reported as Sidekiq's dashboard does —
   * the same totals appear under every queue because the sets are shared.
   */
  async stats(queue: string): Promise<StateCounts> {
    await this.assertQueue(queue);
    return {
      enqueued: await this.redis.llen(this.queueListKey(queue)),
      scheduled: await this.redis.zcard(this.globalKey("scheduled")),
      retry: await this.redis.zcard(this.globalKey("retry")),
      dead: await this.redis.zcard(this.globalKey("dead")),
    };
  }

  async listJobs(queue: string, rawState: string, page: PageOpts): Promise<JobSummary[]> {
    const state = this.assertState(rawState);
    const stop = page.offset + page.limit - 1;

    let raws: string[];
    if (state === "enqueued") {
      // Only the enqueued list is queue-scoped, so it is the only case that
      // requires the queue to exist.
      await this.assertQueue(queue);
      raws = await this.redis.lrange(this.queueListKey(queue), page.offset, stop);
    } else {
      // Global sets: the queue argument does not filter them.
      const key = this.globalKey(state);
      raws = REVERSED.has(state)
        ? await this.redis.zrevrange(key, page.offset, stop)
        : await this.redis.zrange(key, page.offset, stop);
    }

    const summaries: JobSummary[] = [];
    for (const raw of raws) {
      const job = parseJob(raw);
      const summary = job && toSummary(state, job);
      if (summary) summaries.push(summary);
    }
    return summaries;
  }

  async getJob(queue: string, id: string): Promise<JobDetail | null> {
    // Enqueued jobs are queue-scoped, so search the named queue's list; the
    // global sets are searched regardless of the queue argument.
    const enq = await this.findInList(queue, id);
    if (enq) return this.toDetail(queue, "enqueued", enq.member, null);

    for (const state of ["scheduled", "retry", "dead"] as const) {
      const found = await this.findInSet(state, id);
      if (found) return this.toDetail(queue, state, found.member, found.score);
    }
    return null;
  }

  async retryJob(queue: string, id: string): Promise<{ ok: true; message: string }> {
    // Only jobs in the retry or dead set can be retried, mirroring Sidekiq's
    // Web UI "Retry Now" on the Retries and Dead pages.
    for (const state of ["retry", "dead"] as const) {
      const found = await this.findInSet(state, id);
      if (!found) continue;

      const job = parseJob(found.member);
      if (!job) {
        throw new BackendError(`sidekiq job "${id}" has unreadable JSON and cannot be retried`, "invalid_state");
      }
      const target = job.queue ?? queue;

      // Reproduce Sidekiq::SortedEntry#retry: decrement retry_count (a manual
      // retry should not consume an attempt), then Client.push, which stamps a
      // fresh enqueued_at. Every other field is preserved verbatim.
      if (typeof job.retry_count === "number") job.retry_count -= 1;
      job.enqueued_at = Date.now() / 1000;
      const newMember = JSON.stringify(job);

      const result = await this.scripts.sidekiqRequeue(
        [this.globalKey(state), this.queuesKey(), this.queueListKey(target)],
        [found.member, newMember, target],
      );
      if (result === 1) {
        return { ok: true, message: `sidekiq job ${id} re-enqueued to "${target}" from the ${state} set` };
      }
      // The exact member was gone by the time the script ran.
      throw new BackendError(
        `sidekiq job "${id}" changed concurrently and was not re-enqueued`,
        "invalid_state",
      );
    }

    // Give a precise reason when the job exists but is not retryable.
    if (await this.findInList(queue, id)) {
      throw new BackendError(`sidekiq job "${id}" is already enqueued and does not need retrying`, "invalid_state");
    }
    if (await this.findInSet("scheduled", id)) {
      throw new BackendError(
        `sidekiq job "${id}" is scheduled; it is already queued for a future run, not failed`,
        "invalid_state",
      );
    }
    throw new BackendError(`sidekiq job "${id}" was not found in the retry or dead set`, "job_not_found");
  }

  async deleteJob(queue: string, id: string): Promise<{ ok: true; message: string }> {
    // Enqueued: LREM the exact member, as Sidekiq::JobRecord#delete does.
    const enq = await this.findInList(queue, id);
    if (enq) {
      const removed = await this.redis.lrem(this.queueListKey(queue), 1, enq.member);
      if (removed > 0) return { ok: true, message: `sidekiq job ${id} deleted from queue "${queue}"` };
      throw new BackendError(`sidekiq job "${id}" changed concurrently and was not deleted`, "job_not_found");
    }

    // Global sets: ZREM the exact member, as Sidekiq::JobSet#delete_by_value does.
    for (const state of ["scheduled", "retry", "dead"] as const) {
      const found = await this.findInSet(state, id);
      if (found) {
        const removed = await this.redis.zrem(this.globalKey(state), found.member);
        if (removed > 0) return { ok: true, message: `sidekiq job ${id} deleted from the ${state} set` };
        throw new BackendError(`sidekiq job "${id}" changed concurrently and was not deleted`, "job_not_found");
      }
    }

    throw new BackendError(
      `sidekiq job "${id}" was not found in queue "${queue}" or any global set`,
      "job_not_found",
    );
  }

  /** Walks a queue list in pages looking for a job by jid, as Sidekiq's own
   *  Queue#find_job does (documented there as slow but the only way, since
   *  Sidekiq keeps no jid index). Returns the exact member bytes. */
  private async findInList(queue: string, id: string): Promise<{ member: string } | null> {
    const key = this.queueListKey(queue);
    const len = await this.redis.llen(key);
    for (let start = 0; start < len; start += SCAN_CHUNK) {
      const batch = await this.redis.lrange(key, start, start + SCAN_CHUNK - 1);
      for (const member of batch) {
        if (parseJob(member)?.jid === id) return { member };
      }
      if (batch.length < SCAN_CHUNK) break;
    }
    return null;
  }

  /** Walks a global sorted set in pages looking for a job by jid, mirroring
   *  Sidekiq::JobSet#find_job. Returns the exact member bytes and its score. */
  private async findInSet(
    state: GlobalState,
    id: string,
  ): Promise<{ member: string; score: string } | null> {
    const key = this.globalKey(state);
    const card = await this.redis.zcard(key);
    for (let start = 0; start < card; start += SCAN_CHUNK) {
      const batch = await this.redis.zrange(key, start, start + SCAN_CHUNK - 1, "WITHSCORES");
      for (let i = 0; i < batch.length; i += 2) {
        const member = batch[i]!;
        if (parseJob(member)?.jid === id) return { member, score: batch[i + 1]! };
      }
      if (batch.length < SCAN_CHUNK * 2) break;
    }
    return null;
  }

  private toDetail(queue: string, state: State, raw: string, score: string | null): JobDetail | null {
    const job = parseJob(raw);
    if (!job) return null;
    const summary = toSummary(state, job);
    if (!summary) return null;

    // The payload is the job's arguments; Sidekiq args are always JSON text.
    const decoded = decodePayload(Buffer.from(JSON.stringify(job.args ?? []), "utf8"));

    const timestamps: Record<string, string | null> = {
      createdAt: isoFromUnixSeconds(job.created_at),
      enqueuedAt: isoFromUnixSeconds(job.enqueued_at),
      failedAt: isoFromUnixSeconds(job.failed_at),
      retriedAt: isoFromUnixSeconds(job.retried_at),
    };
    if (score !== null) {
      const at = isoFromUnixSeconds(Number(score));
      if (state === "scheduled") timestamps.scheduledFor = at;
      else if (state === "retry") timestamps.nextRetryAt = at;
      else if (state === "dead") timestamps.diedAt = at;
    }

    return {
      ...summary,
      queue: job.queue ?? queue,
      backend: this.name,
      fullError: job.error_message ?? null,
      timestamps,
      ...decoded,
    };
  }
}

/** Parses a job member, tolerating malformed JSON by returning null so the
 *  caller skips it rather than crashing the whole listing. */
function parseJob(raw: string): SidekiqJob | null {
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (parsed && typeof parsed === "object") return parsed as SidekiqJob;
    return null;
  } catch {
    return null;
  }
}

/** Sidekiq's `retry` field is `true` (use the server default, not stored in
 *  Redis), `false` (never retry), or an explicit integer ceiling. */
function maxRetries(retry: SidekiqJob["retry"]): number | null {
  if (typeof retry === "number") return retry;
  if (retry === false) return 0;
  return null;
}

function toSummary(state: State, job: SidekiqJob): JobSummary | null {
  if (!job.jid) return null;
  return {
    id: job.jid,
    type: job.class ?? "",
    state,
    enqueuedAt: isoFromUnixSeconds(job.enqueued_at),
    attempts: typeof job.retry_count === "number" ? job.retry_count : null,
    maxRetries: maxRetries(job.retry),
    lastError: job.error_message ? truncate(job.error_message) : null,
  };
}
