import type { Redis } from "ioredis";
import { scripts } from "./lua.js";

type Caller = (keys: string[], args: Array<string | number>) => Promise<number>;

export interface Scripting {
  reprocessJob: Caller;
  removeJob: Caller;
  runTask: Caller;
  deleteTask: Caller;
  sidekiqRequeue: Caller;
}

const METHOD: Record<keyof typeof scripts, string> = {
  reprocessJob: "qiReprocessJob",
  removeJob: "qiRemoveJob",
  runTask: "qiRunTask",
  deleteTask: "qiDeleteTask",
  sidekiqRequeue: "qiSidekiqRequeue",
};

type DynamicRedis = Record<string, (...a: Array<string | number>) => Promise<unknown>>;

/**
 * Registers the vendored scripts on the client with `defineCommand` (so ioredis
 * uses EVALSHA and only ships each script body once) and returns typed callers.
 */
export function attachScripts(redis: Redis): Scripting {
  const dyn = redis as unknown as DynamicRedis;
  for (const key of Object.keys(scripts) as Array<keyof typeof scripts>) {
    const method = METHOD[key];
    if (typeof dyn[method] !== "function") {
      redis.defineCommand(method, {
        numberOfKeys: scripts[key].numberOfKeys,
        lua: scripts[key].source,
      });
    }
  }

  const call =
    (method: string): Caller =>
    async (keys, args) => {
      const res = await dyn[method]!(...keys, ...args);
      return Number(res);
    };

  return {
    reprocessJob: call(METHOD.reprocessJob),
    removeJob: call(METHOD.removeJob),
    runTask: call(METHOD.runTask),
    deleteTask: call(METHOD.deleteTask),
    sidekiqRequeue: call(METHOD.sidekiqRequeue),
  };
}
