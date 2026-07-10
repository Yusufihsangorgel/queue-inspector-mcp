#!/usr/bin/env node
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { loadConfig } from "./config.js";
import { createRedis } from "./redis.js";
import { BackendRegistry } from "./backends/index.js";
import { createServer } from "./server.js";

async function main(): Promise<void> {
  const config = loadConfig();
  const redis = createRedis(config.redisUrl);
  const registry = new BackendRegistry(redis, config);
  const { server, drained } = createServer({
    registry,
    readOnly: config.readOnly,
    redisUrl: config.redisUrl,
  });

  let closing = false;
  const shutdown = async () => {
    try {
      await server.close();
    } finally {
      redis.disconnect();
    }
  };
  const closeAndExit = () => {
    if (closing) return;
    closing = true;
    void shutdown().then(() => process.exit(0));
  };
  process.on("SIGINT", closeAndExit);
  process.on("SIGTERM", closeAndExit);

  const transport = new StdioServerTransport();
  await server.connect(transport);
  // Stdio clients signal shutdown by closing the pipe, not by signal; without
  // these hooks the Redis connection keeps the process alive after the client
  // has gone. Piped-in request handlers start on the microtask queue, so give
  // them a turn before sampling the in-flight count; once it drains, dropping
  // Redis lets the process exit naturally with every response flushed.
  server.server.onclose = closeAndExit;
  process.stdin.on("end", () => {
    setImmediate(() => void drained().then(() => redis.disconnect()));
  });
  // stderr is safe for logs; stdout carries the MCP protocol.
  const mode = config.readOnly ? " (read-only)" : "";
  process.stderr.write(
    `queue-inspector-mcp: connected, backends=[${config.backends.join(", ")}]${mode}\n`,
  );
}

main().catch((err: unknown) => {
  process.stderr.write(`queue-inspector-mcp: fatal: ${err instanceof Error ? err.stack ?? err.message : String(err)}\n`);
  process.exit(1);
});
