# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
