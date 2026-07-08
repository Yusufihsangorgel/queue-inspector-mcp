import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

// Vendored Redis scripts live next to the compiled adapters (copied into dist by
// the build step). They are loaded once at module init and registered on the
// ioredis client with `defineCommand`, which handles EVALSHA caching.
const luaDir = fileURLToPath(new URL("./lua/", import.meta.url));

function load(name: string): string {
  return readFileSync(`${luaDir}${name}.lua`, "utf8");
}

export const scripts = {
  // BullMQ: exact assembled scripts from the library.
  reprocessJob: { source: load("reprocessJob"), numberOfKeys: 8 },
  removeJob: { source: load("removeJob"), numberOfKeys: 2 },
  // Asynq: exact scripts from the library's rdb inspector.
  runTask: { source: load("runTask"), numberOfKeys: 3 },
  deleteTask: { source: load("deleteTask"), numberOfKeys: 2 },
} as const;
