import { describe, expect, it } from "vitest";
import type { Redis } from "ioredis";
import { AsynqBackend } from "../dist/backends/asynq.js";

// getJob reads a pending task's enqueue time from its `pending_since` field
// (nanoseconds). A malformed value must not crash the read. This exercises that
// path with a fake Redis, so no live Redis is needed; the integration suite
// (asynq.test.ts) covers the happy path against a real Asynq producer.
const MSG = Buffer.from([0x0a, 0x01, 0x78]); // a TaskMessage with just type = "x"

class FakeRedis {
  constructor(private readonly fields: { state: string | null; pending_since?: string }) {}
  defineCommand(): void {} // attachScripts registers scripts at construction
  async sismember(): Promise<number> {
    return 1; // the queue exists
  }
  async hget(_key: string, field: string): Promise<string | null> {
    if (field === "state") return this.fields.state;
    if (field === "pending_since") return this.fields.pending_since ?? null;
    return null;
  }
  async hgetBuffer(_key: string, field: string): Promise<Buffer | null> {
    return field === "msg" ? MSG : null;
  }
}

function backend(fields: { state: string | null; pending_since?: string }): AsynqBackend {
  return new AsynqBackend(new FakeRedis(fields) as unknown as Redis, "asynq");
}

describe("AsynqBackend.getJob enqueue time", () => {
  it("reads pending_since nanoseconds as the enqueue time", async () => {
    const job = await backend({ state: "pending", pending_since: "1700000000000000000" }).getJob("default", "j1");
    expect(job?.state).toBe("pending");
    expect(job?.enqueuedAt).toBe("2023-11-14T22:13:20.000Z");
  });

  it("returns null enqueue time for a malformed pending_since instead of throwing", async () => {
    // Non-numeric bytes would make new Date(NaN).toISOString() throw RangeError.
    const job = await backend({ state: "pending", pending_since: "not-a-number" }).getJob("default", "j1");
    expect(job).not.toBeNull();
    expect(job?.enqueuedAt).toBeNull();
    expect(job?.timestamps.enqueuedAt).toBeNull();
  });

  it("returns null enqueue time for an out-of-range pending_since instead of throwing", async () => {
    const job = await backend({ state: "pending", pending_since: "9999999999999999999999999" }).getJob("default", "j1");
    expect(job?.enqueuedAt).toBeNull();
  });

  it("returns null for a task that has no state field", async () => {
    expect(await backend({ state: null }).getJob("default", "j1")).toBeNull();
  });
});
