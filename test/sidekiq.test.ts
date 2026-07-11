import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Redis } from "ioredis";
import { SidekiqBackend } from "../dist/backends/sidekiq.js";
import { seedSidekiq, testRedis } from "./helpers.js";

describe("SidekiqBackend against seeded Sidekiq data", () => {
  let redis: Redis;
  let backend: SidekiqBackend;

  beforeAll(async () => {
    await seedSidekiq();
    redis = testRedis();
    backend = new SidekiqBackend(redis);
  });

  afterAll(() => {
    redis.disconnect();
  });

  it("detects the seeded queues", async () => {
    const queues = await backend.detectQueues();
    expect(queues).toContain("default");
    expect(queues).toContain("critical");
  });

  it("counts jobs per state, filtering the global sets by queue", async () => {
    const d = await backend.stats("default");
    expect(d.enqueued).toBe(2);
    expect(d.scheduled).toBe(1);
    expect(d.retry).toBe(1);
    expect(d.dead).toBe(1);

    // The schedule/retry/dead sets are global; none of default's jobs may leak
    // into critical's counts.
    const c = await backend.stats("critical");
    expect(c.enqueued).toBe(1);
    expect(c.scheduled).toBe(0);
    expect(c.retry).toBe(0);
    expect(c.dead).toBe(0);
  });

  it("throws a typed error for an unknown queue", async () => {
    await expect(backend.stats("nope")).rejects.toMatchObject({
      name: "BackendError",
      code: "queue_not_found",
    });
  });

  it("lists jobs in a state using the state's own name", async () => {
    const retry = await backend.listJobs("default", "retry", { offset: 0, limit: 20 });
    expect(retry).toHaveLength(1);
    expect(retry[0]).toMatchObject({
      id: "ret0000000000000000001",
      type: "HardJob",
      state: "retry",
      attempts: 1,
    });
    expect(retry[0]?.lastError).toBe("boom");

    const enqueued = await backend.listJobs("default", "enqueued", { offset: 0, limit: 20 });
    expect(enqueued).toHaveLength(2);
  });

  it("rejects an unknown state", async () => {
    await expect(
      backend.listJobs("default", "completed", { offset: 0, limit: 20 }),
    ).rejects.toMatchObject({ code: "invalid_state" });
  });

  it("maps retry:true to Sidekiq's default ceiling of 25", async () => {
    const scheduled = await backend.listJobs("default", "scheduled", { offset: 0, limit: 20 });
    expect(scheduled).toHaveLength(1);
    expect(scheduled[0]?.maxRetries).toBe(25);
  });

  it("fetches one dead job by jid with its error and death time", async () => {
    const job = await backend.getJob("default", "dead000000000000000001");
    expect(job).not.toBeNull();
    expect(job?.state).toBe("dead");
    expect(job?.type).toBe("HardJob");
    expect(job?.fullError).toBe("gave up");
    expect(job?.maxRetries).toBe(5);
    expect(job?.timestamps.diedAt).not.toBeNull();
  });

  it("returns null for a job id that does not exist", async () => {
    const missing = await backend.getJob("default", "does-not-exist");
    expect(missing).toBeNull();
  });
});
