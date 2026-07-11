const utf8Fatal = new TextDecoder("utf-8", { fatal: true });

export interface DecodedPayload {
  payload: string;
  payloadEncoding: "utf8" | "base64";
  payloadBytes: number;
  payloadTruncated: boolean;
}

/** True when the buffer decodes as UTF-8 and holds no C0 control bytes other
 *  than tab, newline and carriage return. Anything else is treated as binary. */
function looksLikeText(buf: Buffer): boolean {
  try {
    utf8Fatal.decode(buf);
  } catch {
    return false;
  }
  for (const byte of buf) {
    if (byte < 0x20 && byte !== 0x09 && byte !== 0x0a && byte !== 0x0d) return false;
  }
  return true;
}

/** Returns the first `maxBytes` of `buf`, stepped back off any trailing
 *  continuation byte so the cut never lands inside a multibyte UTF-8 sequence.
 *  Without this, truncating a text payload mid-character makes the text check
 *  below reject the whole thing and render valid text as base64. */
function utf8Boundary(buf: Buffer, maxBytes: number): Buffer {
  let end = maxBytes;
  // A UTF-8 character carries at most three continuation bytes, so never back
  // up further than that. On binary data a longer run of 0x80-0xBF bytes is not
  // a split character; trimming through it would erase real bytes and, at the
  // extreme, return an empty slice that then gets mislabelled as text.
  const floor = Math.max(0, maxBytes - 3);
  while (end > floor && (buf[end]! & 0xc0) === 0x80) end--;
  return buf.subarray(0, end);
}

/**
 * Renders a raw payload for display. Text is returned as UTF-8; binary or
 * otherwise non-text bytes are returned base64-encoded. Either way the result
 * is capped at `maxBytes` of source data so a large payload cannot flood a tool
 * response.
 */
export function decodePayload(buf: Buffer, maxBytes = 8 * 1024): DecodedPayload {
  const truncated = buf.length > maxBytes;
  const slice = truncated ? utf8Boundary(buf, maxBytes) : buf;
  const isText = looksLikeText(slice);
  return {
    payload: isText ? slice.toString("utf8") : slice.toString("base64"),
    payloadEncoding: isText ? "utf8" : "base64",
    payloadBytes: buf.length,
    payloadTruncated: truncated,
  };
}

export function truncate(text: string, max = 240): string {
  if (text.length <= max) return text;
  return `${text.slice(0, max)}… (${text.length} chars)`;
}

export function isoFromUnixSeconds(seconds: number | null | undefined): string | null {
  if (!seconds || seconds <= 0) return null;
  return isoFromMillis(seconds * 1000);
}

export function isoFromMillis(ms: number | null | undefined): string | null {
  if (!ms || ms <= 0) return null;
  // A malformed or misdecoded message can carry a timestamp past the ±8.64e15ms
  // Date range; toISOString() throws RangeError on such a value, so fall back to
  // null rather than letting one bad field abort the whole job read.
  const date = new Date(ms);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}
