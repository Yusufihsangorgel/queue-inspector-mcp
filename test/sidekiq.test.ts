import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Redis } from "ioredis";
import { SidekiqBackend } from "../dist/backends/sidekiq.js";
import { seedSidekiq, testRedis } from "./helpers.js";

describe("SidekiqBackend against real Sidekiq data", () => {
  let redis: Redis;
  let backend: SidekiqBackend;

  beforeAll(() => {
    seedSidekiq();
    redis = testRedis();
    backend = new SidekiqBackend(redis, "");
  });

  afterAll(() => {
    redis.disconnect();
  });

  it("detects the seeded queues, including a drained one", async () => {
    const queues = await backend.detectQueues();
    expect(queues).toContain("emails");
    expect(queues).toContain("critical");
    // "reports" was pushed to and then drained, so it stays in the queues set.
    expect(queues).toContain("reports");
  });

  it("counts enqueued per-queue and scheduled/retry/dead as global totals", async () => {
    const emails = await backend.stats("emails");
    expect(emails.enqueued).toBe(2);
    expect(emails.scheduled).toBe(1);
    expect(emails.retry).toBe(2);
    expect(emails.dead).toBe(1);

    const critical = await backend.stats("critical");
    expect(critical.enqueued).toBe(1);

    // The global sets are shared, so their totals are identical across queues.
    expect(critical.scheduled).toBe(emails.scheduled);
    expect(critical.retry).toBe(emails.retry);
    expect(critical.dead).toBe(emails.dead);
  });

  it("throws a typed error for an unknown queue", async () => {
    await expect(backend.stats("does-not-exist")).rejects.toMatchObject({
      name: "BackendError",
      code: "queue_not_found",
    });
  });

  it("lists enqueued jobs with class, retry ceiling and enqueue time", async () => {
    const jobs = await backend.listJobs("emails", "enqueued", { offset: 0, limit: 20 });
    expect(jobs).toHaveLength(2);
    for (const job of jobs) {
      expect(job.type).toBe("EmailWorker");
      expect(job.state).toBe("enqueued");
      expect(job.enqueuedAt).not.toBeNull();
      // Never-run jobs have no retry_count.
      expect(job.attempts).toBeNull();
    }
    // retry: 5 maps to a numeric ceiling; retry: true stays null (server default).
    const ceilings = jobs.map((j) => j.maxRetries);
    expect(ceilings).toContain(5);
    expect(ceilings).toContain(null);
  });

  it("returns an empty list for a drained queue", async () => {
    expect(await backend.listJobs("reports", "enqueued", { offset: 0, limit: 20 })).toEqual([]);
  });

  it("lists retry jobs with attempts and a truncated last error", async () => {
    const jobs = await backend.listJobs("emails", "retry", { offset: 0, limit: 20 });
    expect(jobs).toHaveLength(2);
    for (const job of jobs) {
      expect(job.type).toBe("EmailWorker");
      expect(job.state).toBe("retry");
      expect(job.attempts).toBe(0); // retry_count is 0 after the first failure
      expect(job.maxRetries).toBe(5);
      expect(job.lastError).toContain("delivery refused");
    }
  });

  it("reads full detail for a retry job, including next retry time", async () => {
    const [summary] = await backend.listJobs("emails", "retry", { offset: 0, limit: 1 });
    expect(summary).toBeDefined();
    const job = await backend.getJob("emails", summary!.id);
    expect(job).not.toBeNull();
    expect(job!.state).toBe("retry");
    expect(job!.queue).toBe("emails");
    expect(job!.payloadEncoding).toBe("utf8");
    expect(job!.payload).toContain("retry-me");
    expect(job!.fullError).toContain("delivery refused");
    expect(job!.timestamps.failedAt).not.toBeNull();
    expect(job!.timestamps.nextRetryAt).not.toBeNull();
  });

  it("reads the dead set as a global, terminal set", async () => {
    const jobs = await backend.listJobs("critical", "dead", { offset: 0, limit: 20 });
    expect(jobs).toHaveLength(1);
    expect(jobs[0]!.maxRetries).toBe(0); // retry: 0 sends a job straight to dead
    const detail = await backend.getJob("critical", jobs[0]!.id);
    expect(detail!.state).toBe("dead");
    expect(detail!.queue).toBe("critical");
    expect(detail!.timestamps.diedAt).not.toBeNull();
    expect(detail!.fullError).toContain("permanent failure");
  });

  it("reads a scheduled job's future run time", async () => {
    const [summary] = await backend.listJobs("emails", "scheduled", { offset: 0, limit: 5 });
    expect(summary).toBeDefined();
    const job = await backend.getJob("emails", summary!.id);
    expect(job!.state).toBe("scheduled");
    expect(job!.payload).toContain("later@example.test");
    expect(job!.timestamps.scheduledFor).not.toBeNull();
    // Scheduled jobs have not been enqueued yet.
    expect(job!.enqueuedAt).toBeNull();
  });

  it("returns null for a missing job id", async () => {
    expect(await backend.getJob("emails", "no-such-id")).toBeNull();
  });

  it("rejects an unknown state", async () => {
    await expect(backend.listJobs("emails", "failed", { offset: 0, limit: 5 })).rejects.toMatchObject({
      code: "invalid_state",
    });
  });

  describe("mutations", () => {
    beforeAll(() => {
      // Fresh state so retry/delete assertions are deterministic.
      seedSidekiq();
    });

    it("retries a dead job back to its own queue (faithful SortedEntry#retry)", async () => {
      const [dead] = await backend.listJobs("critical", "dead", { offset: 0, limit: 1 });
      expect(dead).toBeDefined();
      const before = await backend.stats("critical");

      const res = await backend.retryJob("critical", dead!.id);
      expect(res.ok).toBe(true);

      const after = await backend.stats("critical");
      expect(after.dead).toBe(before.dead - 1);
      // The dead job's own queue is "critical", so it lands back there.
      expect(after.enqueued).toBe(before.enqueued + 1);

      const moved = await backend.getJob("critical", dead!.id);
      expect(moved!.state).toBe("enqueued");
      expect(moved!.timestamps.enqueuedAt).not.toBeNull();
    });

    it("retries a job from the retry set back into its queue", async () => {
      const [retry] = await backend.listJobs("emails", "retry", { offset: 0, limit: 1 });
      expect(retry).toBeDefined();
      const before = await backend.stats("emails");

      const res = await backend.retryJob("emails", retry!.id);
      expect(res.ok).toBe(true);

      const after = await backend.stats("emails");
      expect(after.retry).toBe(before.retry - 1);
      expect(after.enqueued).toBe(before.enqueued + 1);
      expect((await backend.getJob("emails", retry!.id))!.state).toBe("enqueued");
    });

    it("deletes a scheduled job (faithful delete_by_value)", async () => {
      const [scheduled] = await backend.listJobs("emails", "scheduled", { offset: 0, limit: 1 });
      expect(scheduled).toBeDefined();
      const before = await backend.stats("emails");

      const res = await backend.deleteJob("emails", scheduled!.id);
      expect(res.ok).toBe(true);

      expect((await backend.stats("emails")).scheduled).toBe(before.scheduled - 1);
      expect(await backend.getJob("emails", scheduled!.id)).toBeNull();
    });

    it("deletes an enqueued job from its list (faithful JobRecord#delete)", async () => {
      const [enqueued] = await backend.listJobs("emails", "enqueued", { offset: 0, limit: 1 });
      expect(enqueued).toBeDefined();
      const before = await backend.stats("emails");

      const res = await backend.deleteJob("emails", enqueued!.id);
      expect(res.ok).toBe(true);

      expect((await backend.stats("emails")).enqueued).toBe(before.enqueued - 1);
      expect(await backend.getJob("emails", enqueued!.id)).toBeNull();
    });

    it("reports job_not_found when deleting an unknown job", async () => {
      await expect(backend.deleteJob("emails", "nope")).rejects.toMatchObject({
        code: "job_not_found",
      });
    });

    it("refuses to retry an enqueued job", async () => {
      const [enqueued] = await backend.listJobs("emails", "enqueued", { offset: 0, limit: 1 });
      expect(enqueued).toBeDefined();
      await expect(backend.retryJob("emails", enqueued!.id)).rejects.toMatchObject({
        code: "invalid_state",
      });
    });

    it("reports job_not_found when retrying an unknown job", async () => {
      await expect(backend.retryJob("emails", "nope")).rejects.toMatchObject({
        code: "job_not_found",
      });
    });
  });
});
