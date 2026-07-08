import { Redis } from "ioredis";

/**
 * Creates an ioredis client. `lazyConnect` keeps startup cheap: the connection
 * opens on first command, so the process can register tools before Redis is
 * reachable. `SCAN`-based discovery below never uses `KEYS`, so a large
 * production database is walked in bounded batches.
 */
export function createRedis(url: string): Redis {
  return new Redis(url, {
    lazyConnect: true,
    maxRetriesPerRequest: 3,
    enableReadyCheck: true,
  });
}

/**
 * Iterates keys matching a pattern with `SCAN`, collecting the results. Uses a
 * generous `COUNT` hint but never blocks Redis the way `KEYS` would.
 */
export async function scanKeys(redis: Redis, match: string, count = 500): Promise<string[]> {
  const found: string[] = [];
  let cursor = "0";
  do {
    const [next, batch] = await redis.scan(cursor, "MATCH", match, "COUNT", count);
    cursor = next;
    for (const key of batch) found.push(key);
  } while (cursor !== "0");
  return found;
}
