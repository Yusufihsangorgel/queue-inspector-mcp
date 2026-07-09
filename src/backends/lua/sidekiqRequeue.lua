-- Re-enqueue a Sidekiq job from the retry or dead set, atomically.
--
-- Sidekiq ships no Lua for this; its Web UI does it in Ruby
-- (Sidekiq::SortedEntry#retry -> Sidekiq::Client.push), pipelined but not
-- atomic. This reproduces the exact end state Sidekiq would leave behind:
--   1. remove the exact original member from the retry/dead sorted set,
--   2. register the destination queue in the `queues` set,
--   3. LPUSH the (transformed) job onto queue:<name>, the head of the FIFO,
--      exactly as Sidekiq::Client#atomic_push does.
-- The caller supplies the transformed member (retry_count decremented,
-- enqueued_at refreshed) so Redis-side JSON manipulation is unnecessary.
--
-- KEYS[1] source sorted set   (retry or dead)
-- KEYS[2] queues set          (`queues`)
-- KEYS[3] destination list    (`queue:<name>`)
-- ARGV[1] original member     (exact bytes to ZREM)
-- ARGV[2] new member          (transformed job JSON to LPUSH)
-- ARGV[3] destination queue name (to SADD into `queues`)
--
-- Returns 1 on success, 0 if the original member was gone (changed or already
-- removed concurrently), in which case nothing is enqueued.
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then
  return 0
end
redis.call('SADD', KEYS[2], ARGV[3])
redis.call('LPUSH', KEYS[3], ARGV[2])
return 1
