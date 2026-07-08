-- Transcribed verbatim from Asynq (github.com/hibiken/asynq, MIT license),
-- internal/rdb/inspect.go `runTaskCmd`, version v0.25.1. This is the exact
-- atomic script Inspector.RunTask() runs to move a task from scheduled, retry,
-- archived, completed or aggregating state back to pending. Kept as-is so retry
-- behaviour matches the library.
--
-- KEYS[1] -> task key            (asynq:{<qname>}:t:<task_id>)
-- KEYS[2] -> pending list        (asynq:{<qname>}:pending)
-- KEYS[3] -> all groups set      (asynq:{<qname>}:groups)
-- ARGV[1] -> task id
-- ARGV[2] -> queue key prefix    (asynq:{<qname>}:)
-- ARGV[3] -> group key prefix    (asynq:{<qname>}:g:)
--
-- Returns 1 on success, 0 if not found, -1 if active, -2 if already pending.
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local state, group = unpack(redis.call("HMGET", KEYS[1], "state", "group"))
if state == "active" then
	return -1
elseif state == "pending" then
	return -2
elseif state == "aggregating" then
	local n = redis.call("ZREM", ARGV[3] .. group, ARGV[1])
	if n == 0 then
		return redis.error_reply("internal error: task id not found in zset " .. tostring(ARGV[3] .. group))
	end
	if redis.call("ZCARD", ARGV[3] .. group) == 0 then
		redis.call("SREM", KEYS[3], group)
	end
else
	local n = redis.call("ZREM", ARGV[2] .. state, ARGV[1])
	if n == 0 then
		return redis.error_reply("internal error: task id not found in zset " .. tostring(ARGV[2] .. state))
	end
end
redis.call("LPUSH", KEYS[2], ARGV[1])
redis.call("HSET", KEYS[1], "state", "pending")
return 1
