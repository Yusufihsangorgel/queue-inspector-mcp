// Copies the vendored Lua scripts into dist so the compiled adapters can load
// them at runtime. tsc only emits .js/.d.ts, so non-TS assets are copied here.
import { cpSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const from = resolve(root, "src/backends/lua");
const to = resolve(root, "dist/backends/lua");

mkdirSync(to, { recursive: true });
cpSync(from, to, { recursive: true });
process.stdout.write(`copied lua scripts -> ${to}\n`);
