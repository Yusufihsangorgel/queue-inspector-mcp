import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { Redis } from "ioredis";

export const TEST_DB = 15;
export const TEST_URL = `redis://127.0.0.1:6379/${TEST_DB}`;

const verifyDir = fileURLToPath(new URL("../verify/", import.meta.url));

/** Runs the BullMQ producer, which seeds real jobs and clears its own prefix first. */
export function seedBullmq(): void {
  execFileSync("node", ["index.mjs"], {
    cwd: `${verifyDir}bullmq-producer`,
    env: { ...process.env, REDIS_DB: String(TEST_DB) },
    stdio: "ignore",
  });
}

/** Runs the Asynq producer (Go). It enqueues tasks and runs a worker briefly. */
export function seedAsynq(): void {
  execFileSync("go", ["run", "."], {
    cwd: `${verifyDir}asynq-producer`,
    env: { ...process.env, REDIS_DB: String(TEST_DB) },
    stdio: "ignore",
  });
}

/** Seeds a fixed Sidekiq dataset directly. Sidekiq stores each job as plain
 *  JSON, so no producer process is needed: these are the exact key shapes
 *  Sidekiq writes (the `queues` set, per-queue lists, and the global
 *  schedule/retry/dead sorted sets, each member a job JSON scored by time). */
export async function seedSidekiq(): Promise<void> {
  const redis = testRedis();
  await redis.del("queues", "schedule", "retry", "dead", "queue:default", "queue:critical");

  const job = (over: Record<string, unknown>): string =>
    JSON.stringify({
      class: "HardJob",
      args: [1, "x"],
      retry: 5,
      queue: "default",
      created_at: 1700000000.1,
      ...over,
    });

  await redis.sadd("queues", "default", "critical");
  await redis.rpush(
    "queue:default",
    job({ jid: "enq00000000000000000001", enqueued_at: 1700000001, args: [1] }),
    job({ jid: "enq00000000000000000002", enqueued_at: 1700000002, args: [2] }),
  );
  await redis.rpush(
    "queue:critical",
    job({ queue: "critical", jid: "crit000000000000000001", enqueued_at: 1700000003 }),
  );
  await redis.zadd(
    "schedule",
    "1700003600",
    job({ jid: "sch0000000000000000001", retry: true, args: [42] }),
  );
  await redis.zadd(
    "retry",
    "1700000300",
    job({
      jid: "ret0000000000000000001",
      enqueued_at: 1700000001,
      retry_count: 1,
      error_message: "boom",
      error_class: "RuntimeError",
      failed_at: 1700000200,
      retried_at: 1700000250,
    }),
  );
  await redis.zadd(
    "dead",
    "1700000400",
    job({
      jid: "dead000000000000000001",
      retry_count: 3,
      error_message: "gave up",
      error_class: "StandardError",
      failed_at: 1700000350,
    }),
  );
  redis.disconnect();
}

export function testRedis(): Redis {
  return new Redis(TEST_URL);
}
