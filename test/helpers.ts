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

/** Runs the Sidekiq producer (Ruby). It seeds enqueued/scheduled jobs via the
 *  real client and retry/dead entries via Sidekiq's own retry handler, clearing
 *  only its own keys first. Requires `bundle install` in verify/sidekiq-producer. */
export function seedSidekiq(): void {
  execFileSync("bundle", ["exec", "ruby", "producer.rb"], {
    cwd: `${verifyDir}sidekiq-producer`,
    env: { ...process.env, REDIS_DB: String(TEST_DB) },
    stdio: "ignore",
  });
}

export function testRedis(): Redis {
  return new Redis(TEST_URL);
}
