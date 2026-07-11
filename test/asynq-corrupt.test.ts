import { describe, expect, it } from "vitest";
import type { Redis } from "ioredis";
import { AsynqBackend } from "../dist/backends/asynq.js";

// A valid TaskMessage carrying just type = "ok".
const GOOD = Buffer.from([0x0a, 0x02, 0x6f, 0x6b]);
// type = "abc", then tag 0x28 (field 5, wire type 0 varint) whose only byte has
// the continuation bit set and runs off the end. decodeTaskMessage throws
// "truncated varint" on this, standing in for a partial write or schema drift.
const CORRUPT = Buffer.from([0x0a, 0x03, 0x61, 0x62, 0x63, 0x28, 0x80]);

class FakeRedis {
  constructor(private readonly msgs: Record<string, Buffer>) {}
  defineCommand(): void {}
  async sismember(): Promise<number> {
    return 1;
  }
  async lrange(): Promise<string[]> {
    return Object.keys(this.msgs);
  }
  async hget(_key: string, field: string): Promise<string | null> {
    return field === "state" ? "archived" : null;
  }
  async hgetBuffer(key: string, field: string): Promise<Buffer | null> {
    if (field !== "msg") return null;
    const id = key.slice(key.lastIndexOf(":") + 1);
    return this.msgs[id] ?? null;
  }
}

function backend(msgs: Record<string, Buffer>): AsynqBackend {
  return new AsynqBackend(new FakeRedis(msgs) as unknown as Redis, "asynq");
}

describe("AsynqBackend corrupt task isolation", () => {
  it("lists the rest of the page when one task message is corrupt", async () => {
    const jobs = await backend({ a: GOOD, bad: CORRUPT, c: GOOD }).listJobs("default", "pending", {
      offset: 0,
      limit: 10,
    });
    expect(jobs.map((j) => j.id)).toEqual(["a", "bad", "c"]);
    expect(jobs.find((j) => j.id === "a")?.type).toBe("ok");
    const flagged = jobs.find((j) => j.id === "bad");
    expect(flagged?.type).toBe("(unreadable)");
    expect(flagged?.lastError).toMatch(/could not decode/);
  });

  it("returns a flagged detail for a corrupt task instead of throwing", async () => {
    const job = await backend({ bad: CORRUPT }).getJob("default", "bad");
    expect(job).not.toBeNull();
    expect(job?.type).toBe("(unreadable)");
    expect(job?.payload).toBe("");
    expect(job?.payloadEncoding).toBe("utf8");
    expect(job?.fullError).toMatch(/could not decode/);
  });
});
