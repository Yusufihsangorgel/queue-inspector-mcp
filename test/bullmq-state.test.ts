import { describe, expect, it } from "vitest";
import type { Redis } from "ioredis";
import { BullmqBackend } from "../dist/backends/bullmq.js";

// resolveState decides a job's state purely from which Redis structure holds its
// id, in the exact order BullMQ's own getState (getStateV2-8.lua) uses. That
// order is deterministic, so a fake that reports membership by key exercises it
// with no Redis — and the integration suite (bullmq.test.ts) covers the wiring.
class FakeRedis {
  constructor(private readonly holds: Record<string, string[]>) {}
  defineCommand(): void {} // attachScripts registers scripts at construction
  async zscore(key: string, id: string): Promise<string | null> {
    return this.holds[key]?.includes(id) ? "0" : null; // score 0 is still present
  }
  async lpos(key: string, id: string): Promise<number | null> {
    const at = this.holds[key]?.indexOf(id) ?? -1;
    return at >= 0 ? at : null; // index 0 is a valid position, not "absent"
  }
}

const key = (suffix: string): string => `bull:emails:${suffix}`;

function resolveState(holds: Record<string, string[]>): Promise<string> {
  const backend = new BullmqBackend(new FakeRedis(holds) as unknown as Redis, "bull");
  return (backend as unknown as { resolveState(q: string, id: string): Promise<string> }).resolveState(
    "emails",
    "j1",
  );
}

describe("BullmqBackend.resolveState mirrors getState", () => {
  it("maps each structure to the state BullMQ getState reports", async () => {
    expect(await resolveState({ [key("completed")]: ["j1"] })).toBe("completed");
    expect(await resolveState({ [key("failed")]: ["j1"] })).toBe("failed");
    expect(await resolveState({ [key("delayed")]: ["j1"] })).toBe("delayed");
    expect(await resolveState({ [key("prioritized")]: ["j1"] })).toBe("prioritized");
    expect(await resolveState({ [key("active")]: ["j1"] })).toBe("active");
    expect(await resolveState({ [key("wait")]: ["j1"] })).toBe("waiting");
    expect(await resolveState({ [key("waiting-children")]: ["j1"] })).toBe("waiting-children");
  });

  it("collapses a paused-list job to waiting, exactly as BullMQ does", async () => {
    // BullMQ's getState returns "waiting" (not "paused") for a job in the
    // paused list; the paused list is still listable, but the state name isn't.
    expect(await resolveState({ [key("paused")]: ["j1"] })).toBe("waiting");
  });

  it("returns unknown for a job present in no structure", async () => {
    // A job hash with no set membership (mid-lock / orphaned). BullMQ returns
    // "unknown" here, not "active".
    expect(await resolveState({})).toBe("unknown");
  });

  it("honours the terminal-before-list precedence", async () => {
    // If two structures somehow hold the id mid-transition, the earlier check
    // wins — "failed" (a zset checked first) over "active" (a list).
    expect(await resolveState({ [key("failed")]: ["j1"], [key("active")]: ["j1"] })).toBe("failed");
  });

  it("checks active before waiting-children", async () => {
    // getState checks the active list before the waiting-children set. The old
    // implementation checked waiting-children with the zsets (first) and would
    // have returned "waiting-children" here.
    expect(await resolveState({ [key("active")]: ["j1"], [key("waiting-children")]: ["j1"] })).toBe(
      "active",
    );
  });
});
