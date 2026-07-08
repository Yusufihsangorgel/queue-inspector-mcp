// Asynq stores each task's metadata as a protobuf-encoded TaskMessage in the
// `msg` field of its task hash. Rather than pull in a full protobuf runtime for
// one flat message, this reads the handful of scalar fields the inspector needs
// straight off the wire. Field numbers come from the vendored schema in
// ./proto/asynq_task.proto (from hibiken/asynq, MIT). They are deliberately
// non-sequential upstream (last_failed_at is 11, not 8), so they are pinned by
// number here, not inferred from order.

export interface AsynqTaskMessage {
  type: string;
  payload: Buffer;
  id: string;
  queue: string;
  maxRetry: number;
  retried: number;
  errorMsg: string;
  timeoutSecs: number;
  deadlineUnix: number;
  lastFailedAtUnix: number;
  retentionSecs: number;
  completedAtUnix: number;
  groupKey: string;
}

const FIELD = {
  type: 1,
  payload: 2,
  id: 3,
  queue: 4,
  retry: 5,
  retried: 6,
  errorMsg: 7,
  timeout: 8,
  deadline: 9,
  uniqueKey: 10,
  lastFailedAt: 11,
  retention: 12,
  completedAt: 13,
  groupKey: 14,
} as const;

class Reader {
  private pos = 0;
  constructor(private readonly buf: Buffer) {}

  get done(): boolean {
    return this.pos >= this.buf.length;
  }

  /** Reads a base-128 varint. Values used here (ids, counts, unix seconds) stay
   *  well inside Number.MAX_SAFE_INTEGER, so a plain number is safe. */
  varint(): number {
    let result = 0;
    let shift = 0;
    for (;;) {
      if (this.pos >= this.buf.length) throw new Error("truncated varint");
      const byte = this.buf[this.pos++]!;
      result += (byte & 0x7f) * 2 ** shift;
      if ((byte & 0x80) === 0) return result;
      shift += 7;
    }
  }

  bytes(): Buffer {
    const len = this.varint();
    const end = this.pos + len;
    if (end > this.buf.length) throw new Error("length-delimited field runs past end of buffer");
    const out = this.buf.subarray(this.pos, end);
    this.pos = end;
    return Buffer.from(out);
  }

  /** Advances past a field whose number we do not care about. */
  skip(wireType: number): void {
    switch (wireType) {
      case 0:
        this.varint();
        return;
      case 1:
        this.pos += 8;
        return;
      case 2:
        this.pos += this.varint();
        return;
      case 5:
        this.pos += 4;
        return;
      default:
        throw new Error(`unsupported protobuf wire type ${wireType}`);
    }
  }
}

export function decodeTaskMessage(buf: Buffer): AsynqTaskMessage {
  const msg: AsynqTaskMessage = {
    type: "",
    payload: Buffer.alloc(0),
    id: "",
    queue: "",
    maxRetry: 0,
    retried: 0,
    errorMsg: "",
    timeoutSecs: 0,
    deadlineUnix: 0,
    lastFailedAtUnix: 0,
    retentionSecs: 0,
    completedAtUnix: 0,
    groupKey: "",
  };

  const r = new Reader(buf);
  while (!r.done) {
    const tag = r.varint();
    const field = tag >> 3;
    const wireType = tag & 0x07;

    switch (field) {
      case FIELD.type:
        msg.type = r.bytes().toString("utf8");
        break;
      case FIELD.payload:
        msg.payload = r.bytes();
        break;
      case FIELD.id:
        msg.id = r.bytes().toString("utf8");
        break;
      case FIELD.queue:
        msg.queue = r.bytes().toString("utf8");
        break;
      case FIELD.retry:
        msg.maxRetry = r.varint();
        break;
      case FIELD.retried:
        msg.retried = r.varint();
        break;
      case FIELD.errorMsg:
        msg.errorMsg = r.bytes().toString("utf8");
        break;
      case FIELD.timeout:
        msg.timeoutSecs = r.varint();
        break;
      case FIELD.deadline:
        msg.deadlineUnix = r.varint();
        break;
      case FIELD.lastFailedAt:
        msg.lastFailedAtUnix = r.varint();
        break;
      case FIELD.retention:
        msg.retentionSecs = r.varint();
        break;
      case FIELD.completedAt:
        msg.completedAtUnix = r.varint();
        break;
      case FIELD.groupKey:
        msg.groupKey = r.bytes().toString("utf8");
        break;
      default:
        r.skip(wireType);
    }
  }

  return msg;
}
