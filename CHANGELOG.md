# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-07-10

### Changed

- BullMQ job state now mirrors BullMQ's own `getState` exactly. A job in the
  paused list reports as `waiting`, and a job that exists as a hash but sits in
  no structure reports as `unknown` rather than `active`. Single-job state now
  agrees with what BullMQ itself would report.

### Fixed

- The server now answers the requests it has already read and then exits when
  the client closes stdin (or the transport closes). Previously the Redis
  client kept the process alive indefinitely, hanging piped one-shot
  invocations.
- Tool calls made while Redis is unreachable now report the configured target
  (`cannot reach Redis at <url>`, credentials stripped) and point at
  `REDIS_URL`, instead of surfacing ioredis internals such as "Reached the max
  retries per request limit".
- Asynq protobuf decoding no longer desyncs after skipping a length-delimited
  field it does not model (such as a unique-task key), which previously
  corrupted every field that followed it.
- A malformed or out-of-range timestamp (`pending_since`, deadline, and the
  like) now yields a null time instead of throwing and aborting the whole job
  read.
- A UTF-8 payload truncated mid-character at the display cap is no longer
  misclassified as base64.

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
