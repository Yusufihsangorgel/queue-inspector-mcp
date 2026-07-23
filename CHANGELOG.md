# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Sidekiq backend (verified against Sidekiq 6.5.12). Reports the enqueued /
  scheduled / retry / dead states, decoding each job's JSON (class, args, jid,
  `retry_count`, `error_message`, timestamps). `enqueued` is per-queue; the
  `scheduled`, `retry` and `dead` sorted sets are cluster-global, as in Sidekiq
  itself, so their counts are reported as global totals rather than per-queue
  slices. Retry re-enqueues a job from the retry/dead set to its own queue,
  reproducing `Sidekiq::SortedEntry#retry` → `Sidekiq::Client.push` atomically;
  delete mirrors `Sidekiq::JobRecord#delete` / `JobSet#delete_by_value`.
- `SIDEKIQ_PREFIX` environment variable (default empty, matching Sidekiq's bare
  keys) for redis-namespace deployments, and `sidekiq` accepted in
  `QUEUE_INSPECTOR_BACKENDS`.

### Fixed

- `list_queues`'s `backend` filter now rejects a backend name that is not
  enabled, instead of silently reporting zero queues. Previously it only
  filtered the queues returned by already-constructed backends, so asking
  about a real but disabled backend (e.g. under `QUEUE_INSPECTOR_BACKENDS`)
  returned `{count: 0, queues: []}` instead of an error. It now raises the
  same `not_allowed: backend "X" is not enabled` error that `queue_stats`,
  `list_jobs`, `get_job`, `retry_job` and `delete_job` already raise for the
  identical condition.

## [0.1.0] - 2026-07-08

Initial release.

### Added

- MCP server (`stdio`) exposing six tools: `list_queues`, `queue_stats`,
  `list_jobs`, `get_job`, `retry_job`, `delete_job`.
- Asynq backend. Reads the protobuf `TaskMessage` stored in each task hash and
  reports the pending / active / scheduled / retry / archived / completed states.
  Retry and delete run Asynq's own `RunTask` and `DeleteTask` scripts.
- BullMQ backend. Reports the waiting / active / delayed / prioritized /
  waiting-children / paused / completed / failed states. Retry and delete run
  BullMQ's own `reprocessJob` and `removeJob` scripts.
- Read-only mode via `--read-only` or `QUEUE_INSPECTOR_READ_ONLY=1`, which skips
  the mutating tools.
- Configurable Redis URL and key prefixes (`REDIS_URL`, `ASYNQ_PREFIX`,
  `BULL_PREFIX`) and backend selection (`QUEUE_INSPECTOR_BACKENDS`).
