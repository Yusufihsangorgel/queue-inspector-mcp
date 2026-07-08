import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Redis } from "ioredis";
import { BullmqBackend } from "../dist/backends/bullmq.js";
import { seedBullmq, testRedis } from "./helpers.js";

describe("BullmqBackend against real BullMQ data", () => {
  let redis: Redis;
  let backend: BullmqBackend;

  beforeAll(() => {
    seedBullmq();
    redis = testRedis();
    backend = new BullmqBackend(redis, "bull");
  });

  afterAll(() => {
    redis.disconnect();
  });

  it("detects the seeded queues", async () => {
    const queues = await backend.detectQueues();
    expect(queues).toContain("emails");
    expect(queues).toContain("reports");
  });

  it("counts jobs per state using BullMQ's own state names", async () => {
    const emails = await backend.stats("emails");
    expect(emails.completed).toBe(4);
    expect(emails.failed).toBe(2);
    expect(emails.delayed).toBe(1);
    expect(emails.waiting).toBe(0);

    const reports = await backend.stats("reports");
    expect(reports.waiting).toBe(2);
  });

  it("throws a typed error for an unknown queue", async () => {
    await expect(backend.stats("does-not-exist")).rejects.toMatchObject({
      name: "BackendError",
      code: "queue_not_found",
    });
  });

  it("lists failed jobs with attempts and a truncated last error", async () => {
    const jobs = await backend.listJobs("emails", "failed", { offset: 0, limit: 20 });
    expect(jobs).toHaveLength(2);
    for (const job of jobs) {
      expect(job.type).toBe("send");
      expect(job.state).toBe("failed");
      expect(job.attempts).toBe(2);
      expect(job.maxRetries).toBe(2);
      expect(job.lastError).toContain("delivery refused");
    }
  });

  it("returns an empty list for a state with no jobs", async () => {
    expect(await backend.listJobs("reports", "failed", { offset: 0, limit: 20 })).toEqual([]);
  });

  it("reads full detail for a delayed job", async () => {
    const [summary] = await backend.listJobs("emails", "delayed", { offset: 0, limit: 5 });
    expect(summary).toBeDefined();
    const job = await backend.getJob("emails", summary!.id);
    expect(job).not.toBeNull();
    expect(job!.state).toBe("delayed");
    expect(job!.payloadEncoding).toBe("utf8");
    expect(job!.payload).toContain("later@example.test");
    expect(job!.timestamps.processAt).not.toBeNull();
  });

  it("returns null for a missing job id", async () => {
    expect(await backend.getJob("emails", "does-not-exist")).toBeNull();
  });

  describe("mutations", () => {
    beforeAll(() => {
      // Fresh state so retry/delete assertions are deterministic.
      seedBullmq();
    });

    it("retries a failed job back into wait (faithful Job.retry)", async () => {
      const before = await backend.stats("emails");
      const [failed] = await backend.listJobs("emails", "failed", { offset: 0, limit: 1 });
      expect(failed).toBeDefined();

      const res = await backend.retryJob("emails", failed!.id);
      expect(res.ok).toBe(true);

      const after = await backend.stats("emails");
      expect(after.failed).toBe(before.failed - 1);
      expect(after.waiting).toBe(before.waiting + 1);

      const moved = await backend.getJob("emails", failed!.id);
      expect(moved!.state).toBe("waiting");
    });

    it("deletes a completed job (faithful Job.remove)", async () => {
      const [completed] = await backend.listJobs("emails", "completed", { offset: 0, limit: 1 });
      expect(completed).toBeDefined();
      const before = await backend.stats("emails");

      const res = await backend.deleteJob("emails", completed!.id);
      expect(res.ok).toBe(true);

      const after = await backend.stats("emails");
      expect(after.completed).toBe(before.completed - 1);
      expect(await backend.getJob("emails", completed!.id)).toBeNull();
    });

    it("reports job_not_found when deleting an unknown job", async () => {
      await expect(backend.deleteJob("emails", "nope")).rejects.toMatchObject({
        code: "job_not_found",
      });
    });
  });
});
