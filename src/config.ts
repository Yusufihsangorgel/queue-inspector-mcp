import type { BackendName } from "./types.js";

export interface Config {
  redisUrl: string;
  asynqPrefix: string;
  bullPrefix: string;
  /** Sidekiq is unprefixed by default; a value here mirrors a redis-namespace prefix. */
  sidekiqPrefix: string;
  backends: BackendName[];
  readOnly: boolean;
}

const ALL_BACKENDS: BackendName[] = ["asynq", "bullmq", "sidekiq"];

function parseBackends(raw: string | undefined): BackendName[] {
  if (!raw) return [...ALL_BACKENDS];
  const wanted = raw
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  const picked = ALL_BACKENDS.filter((b) => wanted.includes(b));
  if (picked.length === 0) {
    throw new Error(
      `QUEUE_INSPECTOR_BACKENDS did not name any known backend (asynq, bullmq, sidekiq): "${raw}"`,
    );
  }
  return picked;
}

function envFlag(value: string | undefined): boolean {
  if (!value) return false;
  const v = value.trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes";
}

/** Reads configuration from the environment and CLI flags. */
export function loadConfig(argv: string[] = process.argv.slice(2)): Config {
  const readOnly = argv.includes("--read-only") || envFlag(process.env.QUEUE_INSPECTOR_READ_ONLY);

  return {
    redisUrl: process.env.REDIS_URL || "redis://localhost:6379",
    asynqPrefix: process.env.ASYNQ_PREFIX || "asynq",
    bullPrefix: process.env.BULL_PREFIX || "bull",
    // Sidekiq uses bare keys by default; SIDEKIQ_PREFIX mirrors a redis-namespace.
    sidekiqPrefix: process.env.SIDEKIQ_PREFIX || "",
    backends: parseBackends(process.env.QUEUE_INSPECTOR_BACKENDS),
    readOnly,
  };
}
