# Seeds a Redis database with real Sidekiq jobs in a known mix of states so the
# adapter and tests can be checked against genuine library output rather than a
# guess at the key layout. Enqueued and scheduled jobs go through the real
# Sidekiq::Client; retry and dead entries are produced by Sidekiq's own
# server-side retry handler (Sidekiq::JobRetry#global), so the bytes in Redis
# are exactly what a failing worker would write. Verified against Sidekiq 6.5.12.
#
# Env:
#   REDIS_URL default redis://127.0.0.1:6379
#   REDIS_DB  default 15  (experiment DB, kept away from real data)
#
# Safe to run repeatedly: it removes only the keys Sidekiq owns on the target DB
# (queues, queue:*, schedule, retry, dead), never the whole DB, so it can share a
# database with the Asynq (asynq:*) and BullMQ (bull:*) producers.
#
# Prints a JSON summary of what it created.
require "sidekiq"
require "sidekiq/job_retry"
require "json"
require "securerandom"

REDIS_URL = ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379")
DB = Integer(ENV.fetch("REDIS_DB", "15"))

base = REDIS_URL.sub(%r{/\d+\z}, "")
Sidekiq.redis = {url: "#{base}/#{DB}"}
Sidekiq.logger.level = Logger::FATAL

# A worker class only needs to exist so the client can normalize options.
class EmailWorker
  include Sidekiq::Worker
end

def wipe
  Sidekiq.redis do |conn|
    keys = %w[queues schedule retry dead]
    cursor = "0"
    loop do
      cursor, batch = conn.scan(cursor, match: "queue:*", count: 500)
      keys.concat(batch)
      break if cursor == "0"
    end
    conn.del(*keys) unless keys.empty?
  end
end

def now
  Time.now.to_f
end

wipe

# --- enqueued: two in "emails", one in "critical" (no worker runs) -----------
enqueued = []
enqueued << Sidekiq::Client.push(
  "class" => "EmailWorker",
  "args" => [{"to" => "queued@example.test", "subject" => "hello"}],
  "queue" => "emails",
  "retry" => 5,
)
enqueued << Sidekiq::Client.push(
  "class" => "EmailWorker",
  "args" => ["second", 2],
  "queue" => "emails",
  "retry" => true,
)
critical_jid = Sidekiq::Client.push(
  "class" => "EmailWorker",
  "args" => [42],
  "queue" => "critical",
  "retry" => 3,
)

# --- an empty-but-registered queue: push then drop the list ------------------
# Leaves "reports" in the `queues` set with a zero-length list, matching a queue
# that has been fully drained.
Sidekiq::Client.push("class" => "EmailWorker", "args" => [], "queue" => "reports")
Sidekiq.redis { |c| c.del("queue:reports") }

# --- scheduled: one job with a future run time ------------------------------
scheduled_jid = Sidekiq::Client.push(
  "class" => "EmailWorker",
  "args" => ["later@example.test"],
  "queue" => "emails",
  "retry" => true,
  "at" => now + 3600,
)

# --- retry: two entries via Sidekiq's real server retry path -----------------
retrier = Sidekiq::JobRetry.new(Sidekiq)
retry_jids = []
2.times do |i|
  jid = SecureRandom.hex(12)
  retry_jids << jid
  msg = {
    "class" => "EmailWorker",
    "args" => ["retry-me", i],
    "queue" => "emails",
    "retry" => 5,
    "jid" => jid,
    "created_at" => now,
  }
  begin
    retrier.global(JSON.dump(msg), "emails") { raise RuntimeError, "delivery refused (attempt #{i})" }
  rescue Sidekiq::JobRetry::Handled
  end
end

# --- dead: one entry (retry => 0 goes straight to the morgue) ----------------
dead_jid = SecureRandom.hex(12)
dead_msg = {
  "class" => "EmailWorker",
  "args" => [99],
  "queue" => "critical",
  "retry" => 0,
  "jid" => dead_jid,
  "created_at" => now,
}
begin
  retrier.global(JSON.dump(dead_msg), "critical") { raise ArgumentError, "permanent failure: bad address" }
rescue Sidekiq::JobRetry::Handled
end

summary = {
  "db" => DB,
  "queues" => Sidekiq.redis { |c| c.smembers("queues") }.sort,
  "enqueued" => {"emails" => enqueued, "critical" => [critical_jid]},
  "scheduled" => [scheduled_jid],
  "retry" => retry_jids,
  "dead" => [dead_jid],
  "counts" => {
    "emails" => Sidekiq.redis { |c| c.llen("queue:emails") },
    "critical" => Sidekiq.redis { |c| c.llen("queue:critical") },
    "reports" => Sidekiq.redis { |c| c.llen("queue:reports") },
    "schedule" => Sidekiq.redis { |c| c.zcard("schedule") },
    "retry" => Sidekiq.redis { |c| c.zcard("retry") },
    "dead" => Sidekiq.redis { |c| c.zcard("dead") },
  },
}
puts JSON.pretty_generate(summary)
