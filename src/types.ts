export type BackendName = "asynq" | "bullmq";

export interface QueueRef {
  name: string;
  backend: BackendName;
}

/** Counts keyed by each backend's own state names. */
export type StateCounts = Record<string, number>;

export interface JobSummary {
  id: string;
  /** Asynq task type or BullMQ job name. */
  type: string;
  /** The backend's own state name (e.g. "retry", "archived", "failed"). */
  state: string;
  /** ISO 8601, or null when the backend does not record an enqueue time. */
  enqueuedAt: string | null;
  /** Retries performed so far (Asynq `retried`, BullMQ `attemptsMade`). */
  attempts: number | null;
  /** Configured retry ceiling (Asynq `retry`, BullMQ `opts.attempts`). */
  maxRetries: number | null;
  /** Last failure message, truncated for list views. */
  lastError: string | null;
}

export interface JobDetail extends JobSummary {
  queue: string;
  backend: BackendName;
  /** Payload decoded as UTF-8 text, or base64 when the bytes are not valid text. */
  payload: string;
  payloadEncoding: "utf8" | "base64";
  /** Size of the original payload in bytes, before any truncation. */
  payloadBytes: number;
  payloadTruncated: boolean;
  /** Backend-specific timestamps (ISO 8601), null when not set. */
  timestamps: Record<string, string | null>;
  /** Full, untruncated last failure message. */
  fullError: string | null;
}

export interface PageOpts {
  offset: number;
  limit: number;
}

export interface MutationResult {
  ok: true;
  message: string;
}

export interface QueueBackend {
  readonly name: BackendName;
  /** State names this backend reports, in a sensible display order. */
  readonly states: readonly string[];
  detectQueues(): Promise<string[]>;
  stats(queue: string): Promise<StateCounts>;
  listJobs(queue: string, state: string, page: PageOpts): Promise<JobSummary[]>;
  getJob(queue: string, id: string): Promise<JobDetail | null>;
  retryJob(queue: string, id: string): Promise<MutationResult>;
  deleteJob(queue: string, id: string): Promise<MutationResult>;
}

/**
 * Raised for conditions a caller can act on: an unknown queue or job, or a job
 * whose current state does not allow the requested operation. The server turns
 * these into tool errors rather than crashing.
 */
export class BackendError extends Error {
  constructor(
    message: string,
    readonly code:
      | "queue_not_found"
      | "job_not_found"
      | "invalid_state"
      | "not_allowed",
  ) {
    super(message);
    this.name = "BackendError";
  }
}
