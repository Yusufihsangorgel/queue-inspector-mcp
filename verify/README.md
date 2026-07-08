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

## What this established

- Asynq stores task metadata as a protobuf `TaskMessage` in the `msg` field of
  `asynq:{<queue>}:t:<id>`. The field numbers are not sequential (for example
  `last_failed_at` is field 11, not 8), so they are pinned by number in the
  decoder. See `../src/backends/proto/asynq_task.proto`.
- BullMQ stores each job as a hash and decides its state by which list or sorted
  set holds the id. `attemptsMade` is the `atm` field; the configured ceiling is
  `opts.attempts`.
- Retry and delete are done with each library's own scripts, so the tool mutates
  state exactly the way the library would.
