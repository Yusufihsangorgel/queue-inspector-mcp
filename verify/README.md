# verify/

These producers exist so the adapters are written against observed behavior
rather than assumptions about Redis key layouts. They are not part of the
published package; they seed a local Redis with real jobs from the real
libraries, which the integration tests then read back.

Both producers default to database 15 and clear only their own keys, so they do
not disturb anything on database 0.

## bullmq-producer

A small Node project that installs the real `bullmq`, enqueues jobs into two
queues (some completing, some failing until their attempts are exhausted, some
delayed, some left waiting), and runs a worker briefly so the jobs actually move
through their states.

```bash
cd bullmq-producer
npm install
REDIS_DB=15 node index.mjs
```

## asynq-producer

A small Go module that installs the real `github.com/hibiken/asynq`, enqueues
tasks (one succeeding with retention, one archived immediately, one left in
retry, one scheduled, two pending, one with a non-UTF8 payload), and runs a
worker briefly. It prints the state its own Inspector reports, which is the
ground truth the adapter is checked against.

```bash
cd asynq-producer
REDIS_DB=15 go run .
```

## sidekiq-producer

A small Ruby project (Gemfile pins the real `sidekiq ~> 6.5`) that seeds a known
mix of states: two enqueued jobs in `emails` and one in `critical` via the real
`Sidekiq::Client`, a drained-but-registered `reports` queue, one scheduled job,
two retry-set entries and one dead-set entry. The retry and dead entries are
written by Sidekiq's own server-side retry handler (`Sidekiq::JobRetry#global`),
so the bytes in Redis are exactly what a failing worker would produce — not a
hand-rolled guess at the retry payload. Verified against **Sidekiq 6.5.12** (the
version `bundle install` resolves on Ruby 2.6).

```bash
cd sidekiq-producer
bundle install            # installs sidekiq into vendor/bundle
REDIS_DB=15 bundle exec ruby producer.rb
```

## What this established

- Asynq stores task metadata as a protobuf `TaskMessage` in the `msg` field of
  `asynq:{<queue>}:t:<id>`. The field numbers are not sequential (for example
  `last_failed_at` is field 11, not 8), so they are pinned by number in the
  decoder. See `../src/backends/proto/asynq_task.proto`.
- BullMQ stores each job as a hash and decides its state by which list or sorted
  set holds the id. `attemptsMade` is the `atm` field; the configured ceiling is
  `opts.attempts`.
- Sidekiq stores each job as the JSON member of a list (`queue:<name>`, LPUSHed)
  or a global sorted set (`schedule`, `retry`, `dead`, scored by run/retry/death
  time). There is no per-job hash and no jid index, so a job is found by scanning
  the structure, exactly as `Sidekiq::Queue#find_job` / `JobSet#find_job` do. A
  retry entry adds `error_message`, `error_class`, `retry_count` (0 after the
  first failure) and `failed_at`; `retried_at` appears on subsequent failures.
- Retry and delete are done with each library's own scripts (Sidekiq ships none,
  so its retry is reproduced faithfully in a small vendored script), so the tool
  mutates state exactly the way the library would.
