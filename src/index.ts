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
  const server = createServer({ registry, readOnly: config.readOnly });

  const shutdown = async () => {
    try {
      await server.close();
    } finally {
      redis.disconnect();
    }
  };
  process.on("SIGINT", () => void shutdown().then(() => process.exit(0)));
  process.on("SIGTERM", () => void shutdown().then(() => process.exit(0)));

  const transport = new StdioServerTransport();
  await server.connect(transport);
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
