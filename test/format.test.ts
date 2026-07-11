import { describe, expect, it } from "vitest";
import { decodePayload, isoFromMillis, isoFromUnixSeconds, truncate } from "../dist/format.js";

describe("decodePayload", () => {
  it("returns plain JSON as UTF-8 text", () => {
    const buf = Buffer.from('{"user":"queued@example.test"}', "utf8");
    const out = decodePayload(buf);
    expect(out.payloadEncoding).toBe("utf8");
    expect(out.payload).toContain("queued@example.test");
    expect(out.payloadBytes).toBe(buf.length);
    expect(out.payloadTruncated).toBe(false);
  });

  it("keeps multibyte UTF-8 as text", () => {
    const out = decodePayload(Buffer.from("café ☕ 日本語", "utf8"));
    expect(out.payloadEncoding).toBe("utf8");
    expect(out.payload).toBe("café ☕ 日本語");
  });

  it("base64-encodes binary bytes", () => {
    const raw = Buffer.from([0x00, 0x01, 0x02, 0xff, 0xfe]);
    const out = decodePayload(raw);
    expect(out.payloadEncoding).toBe("base64");
    expect(out.payload).toBe(raw.toString("base64"));
  });

  it("treats tab, newline and carriage return as text", () => {
    const out = decodePayload(Buffer.from("a\tb\nc\r\n", "utf8"));
    expect(out.payloadEncoding).toBe("utf8");
  });

  it("treats other C0 control bytes as binary", () => {
    const out = decodePayload(Buffer.from("hi\x01there", "utf8"));
    expect(out.payloadEncoding).toBe("base64");
  });

  it("truncates to maxBytes and still reports the full size", () => {
    const buf = Buffer.from("x".repeat(20), "utf8");
    const out = decodePayload(buf, 10);
    expect(out.payloadTruncated).toBe(true);
    expect(out.payload).toBe("x".repeat(10));
    expect(out.payloadBytes).toBe(20);
  });

  it("does not flip to base64 when the cut splits a multibyte character", () => {
    // 9 ASCII bytes, then a 3-byte '€' straddling the 10-byte cut. Regression:
    // the pre-boundary slice ended mid-character and the whole payload was
    // misclassified as base64.
    const buf = Buffer.concat([Buffer.from("a".repeat(9)), Buffer.from("€bbbb", "utf8")]);
    const out = decodePayload(buf, 10);
    expect(out.payloadEncoding).toBe("utf8");
    expect(out.payload).toBe("a".repeat(9));
    expect(out.payloadTruncated).toBe(true);
    expect(out.payloadBytes).toBe(buf.length);
  });

  it("keeps an oversize all-continuation-byte payload as base64 rather than erasing it", () => {
    // 9000 bytes of 0x80 (a realistic run inside a >8KB binary job payload). The
    // cut at 8192 lands on a continuation byte; an uncapped backup walked end to
    // 0 and returned an empty "utf8" payload, silently dropping the binary blob.
    const out = decodePayload(Buffer.alloc(9000, 0x80));
    expect(out.payloadEncoding).toBe("base64");
    expect(out.payload.length).toBeGreaterThan(0);
    expect(out.payloadBytes).toBe(9000);
    expect(out.payloadTruncated).toBe(true);
  });

  it("backs up at most three bytes off the cut point", () => {
    // 'A' * 5, then a run of continuation bytes that spans the 10-byte cut. The
    // backup must stop 3 bytes back (index 7), not chase the whole run down to 4.
    const buf = Buffer.alloc(20, 0x41);
    for (let i = 5; i < 15; i++) buf[i] = 0x80;
    const out = decodePayload(buf, 10);
    expect(out.payloadEncoding).toBe("base64");
    expect(Buffer.from(out.payload, "base64").length).toBe(7);
  });

  it("handles an empty buffer as empty text", () => {
    const out = decodePayload(Buffer.alloc(0));
    expect(out.payloadEncoding).toBe("utf8");
    expect(out.payload).toBe("");
    expect(out.payloadBytes).toBe(0);
  });
});

describe("truncate", () => {
  it("leaves a short string unchanged", () => {
    expect(truncate("already short")).toBe("already short");
  });

  it("leaves a string exactly at the limit unchanged", () => {
    const s = "y".repeat(240);
    expect(truncate(s)).toBe(s);
  });

  it("cuts an overlong string and notes the original length", () => {
    const out = truncate("z".repeat(300));
    expect(out).toBe(`${"z".repeat(240)}… (300 chars)`);
  });
});

describe("isoFromUnixSeconds", () => {
  it("converts seconds to an ISO string", () => {
    expect(isoFromUnixSeconds(1_700_000_000)).toBe("2023-11-14T22:13:20.000Z");
  });

  it("returns null for zero, negative, null and undefined", () => {
    expect(isoFromUnixSeconds(0)).toBeNull();
    expect(isoFromUnixSeconds(-5)).toBeNull();
    expect(isoFromUnixSeconds(null)).toBeNull();
    expect(isoFromUnixSeconds(undefined)).toBeNull();
  });

  it("returns null instead of throwing on an out-of-range timestamp", () => {
    // A misdecoded field can be enormous; toISOString() would throw RangeError.
    expect(isoFromUnixSeconds(2 ** 53)).toBeNull();
    expect(isoFromUnixSeconds(Infinity)).toBeNull();
  });
});

describe("isoFromMillis", () => {
  it("converts milliseconds to an ISO string", () => {
    expect(isoFromMillis(1_700_000_000_000)).toBe("2023-11-14T22:13:20.000Z");
  });

  it("returns null for zero, negative and null", () => {
    expect(isoFromMillis(0)).toBeNull();
    expect(isoFromMillis(-1)).toBeNull();
    expect(isoFromMillis(null)).toBeNull();
  });

  it("returns null instead of throwing on an out-of-range value", () => {
    expect(isoFromMillis(9e18)).toBeNull();
  });
});
