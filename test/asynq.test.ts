import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Redis } from "ioredis";
import { AsynqBackend } from "../dist/backends/asynq.js";
import { seedAsynq, testRedis } from "./helpers.js";

describe("AsynqBackend against real Asynq data", () => {
  let redis: Redis;
  let backend: AsynqBackend;

  beforeAll(() => {
    seedAsynq();
    redis = testRedis();
    backend = new AsynqBackend(redis, "asynq");
  });

  afterAll(() => {
    redis.disconnect();
  });

  it("detects the seeded queues", async () => {
    const queues = await backend.detectQueues();
    expect(queues).toContain("default");
    expect(queues).toContain("low");
  });

  it("counts tasks per state using Asynq's own state names", async () => {
    const def = await backend.stats("default");
    expect(def.scheduled).toBe(1);
    expect(def.retry).toBe(1);
    expect(def.archived).toBe(1);
    expect(def.completed).toBe(1);
    expect(def.pending).toBe(0);

    const low = await backend.stats("low");
    expect(low.pending).toBe(2);
  });

  it("decodes the protobuf message for an archived task", async () => {
    const [job] = await backend.listJobs("default", "archived", { offset: 0, limit: 5 });
    expect(job).toBeDefined();
    expect(job!.type).toBe("email:archive");
    expect(job!.maxRetries).toBe(0);
    expect(job!.lastError).toContain("simulated permanent failure");

    // Full detail must recover last_failed_at, which lives at proto field 11.
    const detail = await backend.getJob("default", job!.id);
    expect(detail!.fullError).toContain("simulated permanent failure");
    expect(detail!.timestamps.lastFailedAt).not.toBeNull();
  });

  it("reads a retry task's configured ceiling and next process time", async () => {
    const [job] = await backend.listJobs("default", "retry", { offset: 0, limit: 5 });
    expect(job!.type).toBe("email:retry");
    expect(job!.maxRetries).toBe(5);
    const detail = await backend.getJob("default", job!.id);
    expect(detail!.timestamps.nextProcessAt).not.toBeNull();
  });

  it("returns a base64 payload for a task with non-UTF8 bytes", async () => {
    const pending = await backend.listJobs("low", "pending", { offset: 0, limit: 10 });
    const blob = pending.find((j) => j.type === "blob:binary");
    expect(blob).toBeDefined();
    const detail = await backend.getJob("low", blob!.id);
    expect(detail!.payloadEncoding).toBe("base64");
    // 0x00 0x01 0x02 0xff 0xfe 'h' 'i' -> base64
    expect(detail!.payload).toBe(Buffer.from([0, 1, 2, 0xff, 0xfe, 0x68, 0x69]).toString("base64"));
  });

  it("reads a plain JSON payload as UTF-8 text", async () => {
    const pending = await backend.listJobs("low", "pending", { offset: 0, limit: 10 });
    const plain = pending.find((j) => j.type === "email:pending");
    const detail = await backend.getJob("low", plain!.id);
    expect(detail!.payloadEncoding).toBe("utf8");
    expect(detail!.payload).toContain("queued@example.test");
  });

  it("returns null for a missing task id", async () => {
    expect(await backend.getJob("default", "no-such-id")).toBeNull();
  });

  describe("mutations", () => {
    beforeAll(() => {
      seedAsynq();
    });

    it("retries an archived task back to pending (faithful RunTask)", async () => {
      const [archived] = await backend.listJobs("default", "archived", { offset: 0, limit: 1 });
      expect(archived).toBeDefined();
      const before = await backend.stats("default");

      const res = await backend.retryJob("default", archived!.id);
      expect(res.ok).toBe(true);

      const after = await backend.stats("default");
      expect(after.archived).toBe(before.archived - 1);
      expect(after.pending).toBe(before.pending + 1);
      const moved = await backend.getJob("default", archived!.id);
      expect(moved!.state).toBe("pending");
    });

    it("deletes a scheduled task (faithful DeleteTask)", async () => {
      const [scheduled] = await backend.listJobs("default", "scheduled", { offset: 0, limit: 1 });
      expect(scheduled).toBeDefined();
      const before = await backend.stats("default");

      const res = await backend.deleteJob("default", scheduled!.id);
      expect(res.ok).toBe(true);

      const after = await backend.stats("default");
      expect(after.scheduled).toBe(before.scheduled - 1);
      expect(await backend.getJob("default", scheduled!.id)).toBeNull();
    });
  });
});
