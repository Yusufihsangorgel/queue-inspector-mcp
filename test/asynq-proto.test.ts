import { describe, expect, it } from "vitest";
import { decodeTaskMessage } from "../dist/backends/asynq-proto.js";

// Unit coverage for the hand-rolled protobuf reader. asynq.test.ts already
// pins it against messages a real Asynq worker wrote; this file covers the
// wire-format edges that are awkward to provoke through a live worker: unknown
// fields from a newer schema, a task that never failed (so field 11 is absent),
// timestamps past 2**31, and malformed buffers.
//
// The fixtures are built by a tiny local encoder so they read as structured
// data rather than opaque bytes. To keep that from being a mirror-test of the
// decoder, the first case decodes a literal byte string instead.

const WIRE = { VARINT: 0, I64: 1, LEN: 2, I32: 5 } as const;

// Base-128 varint. Uses %/Math.floor rather than bitwise so values above 2**31
// round-trip, the same reason the reader multiplies instead of shifting.
function varint(value: number): number[] {
  const out: number[] = [];
  let n = value;
  while (n > 0x7f) {
    out.push((n % 128) | 0x80);
    n = Math.floor(n / 128);
  }
  out.push(n);
  return out;
}

function tag(field: number, wire: number): number[] {
  return varint((field << 3) | wire);
}

function varField(field: number, value: number): number[] {
  return [...tag(field, WIRE.VARINT), ...varint(value)];
}

function lenField(field: number, bytes: number[]): number[] {
  return [...tag(field, WIRE.LEN), ...varint(bytes.length), ...bytes];
}

function strField(field: number, text: string): number[] {
  return lenField(field, [...Buffer.from(text, "utf8")]);
}

function message(...parts: number[][]): Buffer {
  return Buffer.from(parts.flat());
}

describe("decodeTaskMessage", () => {
  it("decodes a literal wire-format fixture", () => {
    // type="email:welcome" (field 1), retry=5 (field 5), retried=2 (field 6).
    const buf = Buffer.from(
      "0a0d" + // field 1, length 13
        "656d61696c3a77656c636f6d65" + // "email:welcome"
        "2805" + // field 5 = 5
        "3002", // field 6 = 2
      "hex",
    );
    const msg = decodeTaskMessage(buf);
    expect(msg.type).toBe("email:welcome");
    expect(msg.maxRetry).toBe(5);
    expect(msg.retried).toBe(2);
    // Untouched fields keep their zero values.
    expect(msg.queue).toBe("");
    expect(msg.lastFailedAtUnix).toBe(0);
  });

  it("reads every field, including the non-sequential field 11", () => {
    const buf = message(
      strField(1, "email:send"),
      lenField(2, [0x7b, 0x7d]), // payload {}
      strField(3, "task-abc"),
      strField(4, "critical"),
      varField(5, 25), // maxRetry
      varField(6, 3), // retried
      strField(7, "smtp timeout"),
      varField(8, 30), // timeoutSecs
      varField(9, 1_700_000_000), // deadline
      strField(10, "unique:email:send"), // uniqueKey — not surfaced, must be skipped
      varField(11, 1_699_999_000), // lastFailedAt, deliberately after field 10
      varField(12, 86_400), // retention
      varField(13, 1_700_000_500), // completedAt
      strField(14, "batch-42"), // groupKey
    );
    const msg = decodeTaskMessage(buf);
    expect(msg).toEqual({
      type: "email:send",
      payload: Buffer.from([0x7b, 0x7d]),
      id: "task-abc",
      queue: "critical",
      maxRetry: 25,
      retried: 3,
      errorMsg: "smtp timeout",
      timeoutSecs: 30,
      deadlineUnix: 1_700_000_000,
      lastFailedAtUnix: 1_699_999_000,
      retentionSecs: 86_400,
      completedAtUnix: 1_700_000_500,
      groupKey: "batch-42",
    });
  });

  it("leaves lastFailedAt at zero for a task that never failed", () => {
    // A freshly-enqueued task carries no field 11 at all.
    const msg = decodeTaskMessage(message(strField(1, "email:new"), varField(5, 5)));
    expect(msg.type).toBe("email:new");
    expect(msg.maxRetry).toBe(5);
    expect(msg.retried).toBe(0);
    expect(msg.lastFailedAtUnix).toBe(0);
  });

  it("skips unknown fields of every wire type without losing its place", () => {
    // Simulates a task written by a newer Asynq that added fields the inspector
    // does not model. If skip() miscounts any wire type, the trailing queue tag
    // lands mid-field and this decodes to garbage.
    const buf = message(
      strField(1, "keep:type"),
      varField(20, 99_999), // unknown varint
      [...tag(21, WIRE.I64), 1, 2, 3, 4, 5, 6, 7, 8], // unknown fixed64
      lenField(22, [0xaa, 0xbb, 0xcc]), // unknown length-delimited
      [...tag(23, WIRE.I32), 9, 9, 9, 9], // unknown fixed32
      strField(4, "keep:queue"),
    );
    const msg = decodeTaskMessage(buf);
    expect(msg.type).toBe("keep:type");
    expect(msg.queue).toBe("keep:queue");
  });

  it("reads multi-byte and beyond-32-bit varints", () => {
    const buf = message(
      varField(5, 300), // two-byte varint
      varField(13, 2_200_000_000), // > 2**31, exercises the float-safe path
    );
    const msg = decodeTaskMessage(buf);
    expect(msg.maxRetry).toBe(300);
    expect(msg.completedAtUnix).toBe(2_200_000_000);
  });

  it("preserves a non-UTF8 payload as raw bytes and copies it", () => {
    const raw = [0x00, 0x01, 0x02, 0xff, 0xfe];
    const buf = message(strField(1, "blob:binary"), lenField(2, raw));
    const msg = decodeTaskMessage(buf);
    expect(Buffer.compare(msg.payload, Buffer.from(raw))).toBe(0);

    // The payload must be a copy: mutating the source buffer afterwards must
    // not reach into the decoded message.
    buf.fill(0);
    expect(Buffer.compare(msg.payload, Buffer.from(raw))).toBe(0);
  });

  it("returns an all-default message for an empty buffer", () => {
    const msg = decodeTaskMessage(Buffer.alloc(0));
    expect(msg.type).toBe("");
    expect(msg.payload.length).toBe(0);
    expect(msg.maxRetry).toBe(0);
    expect(msg.completedAtUnix).toBe(0);
  });

  it("throws on a varint truncated by the end of the buffer", () => {
    // field 5 tag, then a continuation byte with nothing after it.
    expect(() => decodeTaskMessage(Buffer.from([0x28, 0x80]))).toThrow(/truncated varint/);
  });

  it("throws when a length-delimited field runs past the buffer", () => {
    // field 1, claims 5 bytes, only 2 follow.
    expect(() => decodeTaskMessage(Buffer.from([0x0a, 0x05, 0x61, 0x62]))).toThrow(/past end/);
  });
});
