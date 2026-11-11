# Recovery Entry Point

- Document Class: Compatibility Note
- Status: Implemented
- Date: 2026-11-11
- Category: Library API
- Scope: Resync and redrive from a declared source list; `[recovery]` configuration
- Sources:
  - crates/kafkaman/src/recovery.rs
  - crates/kafkaman-config/src/sections.rs
  - crates/kafkaman-config/src/config.rs
  - crates/kafkaman-core/src/redrive.rs
  - crates/kafkaman-sqlx/src/operability.rs
  - kafkaman.example.toml
  - tests/durable-send/tests/recovery/
  - tests/observability/tests/admin_http.rs
- Related:
  - wiki/compatibility/state-sourced-republish-api.compat.md
  - wiki/compatibility/topic-convergence-api.compat.md

## Public surface and compatibility

Additive, minor release. Nothing existing changes signature.

- `kafkaman::recovery` (feature `rdkafka`): `Source::resync::<M, _, _>(name,
  keys, load)` and `Source::received::<M>(name)` declare sources;
  `Recovery::new` refuses an empty list, blank or repeated names, invalid
  descriptors and conflicting roles. `Recovery::request_from(&Config,
  RecoveryInputs)` checks operation, source, cursor and row count before any
  I/O, and `Recovery::run` prepares and runs the selection, returning a
  `Report` or `RecoveryError::Failed` carrying one.
- `[recovery]` in `kafkaman.toml`: `batch_size` (default 100), `pause_ms`
  (default 0) and `max_redrive_rows` (default and ceiling 10000), read through
  `Config::recovery()`. Unknown keys and out-of-range values are refused.
- `Config::with_schema` and `Config::with_worker_id` replace the two values a
  host sets per deployment or per process, so the rest stays in the file.
- `MAX_REDRIVE_ROWS` moves to `kafkaman-core` and is re-exported where it was.
- `service_table_access` now requires `USAGE` on the schema as well as the
  table privileges, and the `NoPrivilege` repair text names it.

## Behaviour

- A resync with no source runs every resync source in declaration order. A
  cursor (`start_after`) requires exactly one named source, since one cursor
  cannot resume a list.
- A redrive names one received source and a row count in
  `1..=max_redrive_rows`; a resync refuses a row count.
- Preparation verifies every selected topic (`TopicMode::Verify`, never
  `Create`) before migrating any table, then migrates each source with its own
  declared role. A resync needs brokers; a redrive never does.
- The first failure stops the run. The report lists completed sources, the
  failed one with its last committed key, and the sources that never ran.
  Nothing is retried and no checkpoint is stored: the operator resumes from
  the reported key, under a new run id.
- A redrive whose table is missing or unwritable is refused by name with
  `Failure::NotRedrivable` and changes nothing.
- Events go to the `kafkaman` target: `recovery started`, `recovery progress`
  (once per page), `recovery source completed` and `recovery failed`, each
  with `run_id`, `operation` and `source`.

## Operator impact

The admin redrive route answers a role without schema `USAGE` with 503 and a
repair naming the grant, where it used to pass the probe and fail the `UPDATE`
with a 500.
