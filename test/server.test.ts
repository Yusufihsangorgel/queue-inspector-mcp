import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Redis } from "ioredis";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import type { BackendName } from "../dist/types.js";
import { BackendRegistry } from "../dist/backends/index.js";
import { createServer } from "../dist/server.js";
import { TEST_URL, testRedis } from "./helpers.js";

/** Connects a real MCP client to a server with only `backends` enabled, over an
 *  in-memory transport pair, exactly as a real client would call the tool. */
async function connect(redis: Redis, backends: BackendName[]) {
  const registry = new BackendRegistry(redis, {
    redisUrl: TEST_URL,
    asynqPrefix: "asynq",
    bullPrefix: "bull",
    sidekiqPrefix: "",
    backends,
    readOnly: true,
  });
  const server = createServer({ registry, readOnly: true });
  const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
  const client = new Client({ name: "test-client", version: "0.0.0" });
  await Promise.all([server.connect(serverTransport), client.connect(clientTransport)]);
  return { client, server };
}

describe("list_queues backend filter", () => {
  let redis: Redis;

  beforeAll(() => {
    redis = testRedis();
  });

  afterAll(() => {
    redis.disconnect();
  });

  it("errors like every other tool when asked about a backend that isn't enabled", async () => {
    const { client, server } = await connect(redis, ["bullmq"]);
    try {
      const result = await client.callTool({ name: "list_queues", arguments: { backend: "asynq" } });
      expect(result.isError).toBe(true);
      expect(result.content).toMatchObject([
        { type: "text", text: 'not_allowed: backend "asynq" is not enabled' },
      ]);
    } finally {
      await client.close();
      await server.close();
    }
  });

  it("still lists queues normally for a backend that is enabled", async () => {
    const { client, server } = await connect(redis, ["bullmq"]);
    try {
      const result = await client.callTool({ name: "list_queues", arguments: { backend: "bullmq" } });
      expect(result.isError).toBeFalsy();
      const [entry] = result.content as Array<{ type: "text"; text: string }>;
      expect(entry).toBeDefined();
      expect(JSON.parse(entry!.text)).toMatchObject({ count: expect.any(Number) });
    } finally {
      await client.close();
      await server.close();
    }
  });

  it("still lists queues normally with no backend filter at all", async () => {
    const { client, server } = await connect(redis, ["bullmq"]);
    try {
      const result = await client.callTool({ name: "list_queues", arguments: {} });
      expect(result.isError).toBeFalsy();
    } finally {
      await client.close();
      await server.close();
    }
  });
});
