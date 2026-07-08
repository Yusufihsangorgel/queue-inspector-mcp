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

/**
 * Renders a raw payload for display. Text is returned as UTF-8; binary or
 * otherwise non-text bytes are returned base64-encoded. Either way the result
 * is capped at `maxBytes` of source data so a large payload cannot flood a tool
 * response.
 */
export function decodePayload(buf: Buffer, maxBytes = 8 * 1024): DecodedPayload {
  const truncated = buf.length > maxBytes;
  const slice = truncated ? buf.subarray(0, maxBytes) : buf;
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
  return new Date(seconds * 1000).toISOString();
}

export function isoFromMillis(ms: number | null | undefined): string | null {
  if (!ms || ms <= 0) return null;
  return new Date(ms).toISOString();
}
