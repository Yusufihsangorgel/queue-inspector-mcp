import type { Redis } from "ioredis";
import type { Config } from "../config.js";
import { BackendError, type BackendName, type QueueBackend, type QueueRef } from "../types.js";
import { AsynqBackend } from "./asynq.js";
import { BullmqBackend } from "./bullmq.js";

export { AsynqBackend } from "./asynq.js";
export { BullmqBackend } from "./bullmq.js";

/** Holds the enabled backends and maps a queue name to the backend that owns it. */
export class BackendRegistry {
  private readonly backends = new Map<BackendName, QueueBackend>();

  constructor(redis: Redis, config: Config) {
    for (const name of config.backends) {
      if (name === "asynq") this.backends.set(name, new AsynqBackend(redis, config.asynqPrefix));
      if (name === "bullmq") this.backends.set(name, new BullmqBackend(redis, config.bullPrefix));
    }
  }

  list(): QueueBackend[] {
    return [...this.backends.values()];
  }

  async allQueues(): Promise<QueueRef[]> {
    const refs: QueueRef[] = [];
    for (const backend of this.list()) {
      for (const name of await backend.detectQueues()) {
        refs.push({ name, backend: backend.name });
      }
    }
    return refs;
  }

  /**
   * Resolves which backend a queue belongs to. When `hint` is given it wins;
   * otherwise the queue is looked up across enabled backends. An ambiguous name
   * (present in more than one backend) is an error the caller must resolve by
   * passing `backend` explicitly.
   */
  async resolve(queue: string, hint?: BackendName): Promise<QueueBackend> {
    if (hint) {
      const backend = this.backends.get(hint);
      if (!backend) {
        throw new BackendError(`backend "${hint}" is not enabled`, "not_allowed");
      }
      return backend;
    }

    const matches: QueueBackend[] = [];
    for (const backend of this.list()) {
      const queues = await backend.detectQueues();
      if (queues.includes(queue)) matches.push(backend);
    }
    if (matches.length === 1) return matches[0]!;
    if (matches.length === 0) {
      throw new BackendError(`queue "${queue}" was not found in any enabled backend`, "queue_not_found");
    }
    throw new BackendError(
      `queue "${queue}" exists in multiple backends (${matches.map((m) => m.name).join(", ")}); pass "backend" to choose one`,
      "invalid_state",
    );
  }
}
