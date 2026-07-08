# Contributing

Thanks for considering a contribution. This is a small, deliberately focused
project, so the bar is less about volume and more about keeping each backend
faithful to the library it inspects.

## Running the tests

The tests are integration tests — they do not mock Redis. They stand up real
jobs with the real client libraries and read them back through the adapters, so
you need:

- Node.js 18+
- Go (for the Asynq producer)
- A reachable Redis (the suite uses a dedicated DB and cleans up after itself)

```bash
npm ci
npm install --prefix verify/bullmq-producer   # deps for the BullMQ producer
npm run build
npm test
```

`verify/asynq-producer` (Go) and `verify/bullmq-producer` (Node) seed real jobs;
the adapters are then asserted against what the libraries actually wrote. If you
change an adapter, the corresponding producer is the ground truth — update the
producer to reproduce the state you're handling, not just the test's
expectations.

## Adding or changing a backend

Each backend lives in `src/backends/` and implements the `QueueBackend`
interface. Two rules keep the adapters honest:

1. **Report the backend's own state names.** Don't map everything onto a shared
   vocabulary — Asynq's `archived` and BullMQ's `failed` are not the same thing,
   and flattening them hides information an operator needs during an incident.
2. **For mutations, run the library's own script.** `retry_job` and `delete_job`
   execute each library's vendored atomic Lua (Asynq's `RunTask`/`DeleteTask`,
   BullMQ's `reprocessJob`/`removeJob`) rather than a reimplementation, so the
   semantics can't drift from the source library. New mutating operations should
   follow the same approach; if you can't replicate a transition faithfully from
   outside the library, document the limitation instead of approximating it.

## Pull requests

- Keep changes focused; one backend or one feature per PR.
- `npm run build` and `npm test` must pass (CI runs both against a real Redis).
- Note in the PR which library version you verified against.
