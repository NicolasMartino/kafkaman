# M2 Change Engine Config Schema and API Changes

Document Class: Compatibility Note
Status: Draft
Date: 2026-06-21
Category: Schema and API compatibility
Scope: Compatibility impact from M2 migration-engine maturity and config loading.
Sources: wiki/plans/m2-change-engine-config.plan.md
Related: wiki/specs/m2-change-engine-config.spec.md

## Breaking API Changes

- `kafkaman_sqlx::migrate` now requires `&MigrationContext` and returns `MigrationReport` instead of `Result<()>`.
- Callers that previously ignored migration output must pass `MigrationContext::default()` or `MigrationContext::from_env()` and may ignore the returned report.
- `ResolvedConfig::from_config` is the config-loaded boot path for apps that use `kafkaman.toml`; hand-built `ResolvedConfig` remains available for tests and low-level construction.

## Schema Changes

- `<schema>.changelog_history` gains nullable `checksum TEXT` and nullable `applied_by TEXT`.
- M2 intentionally does not add a `NOT NULL` constraint to `checksum`, because existing M1 installs can already contain history rows.
- During bootstrap, missing `applied_by` values are backfilled to `unknown`; legacy NULL checksums are not backfilled and are skipped during checksum verification.

## Operational Changes

- Applied changesets now record stable source-declaration checksums hashed with SHA-256 and stored as `sha256:<64 hex chars>`. (Earlier maturity builds stored a non-cryptographic `fnv1a64:` value; this is the algorithm the plan specified. A history row applied under the interim format would report `ChecksumMismatch` against the SHA-256 source declaration; legacy NULL checksums remain skipped.)
- Re-applying a changeset with changed declared parameters returns `ChecksumMismatch` unless the stored checksum is NULL from a legacy row.
- `Replay::outbox` is auto-applied through the same changelog stream, but only as a bounded single-statement update with mandatory `max_rows` and optional `MigrationContext` targeting. The statement now also resets `attempts` to `0` and clears `last_error` on requeued rows, so a replayed row starts a fresh delivery budget rather than carrying stale failure state into the next relay pass.
- `migrate_dry_run` previews pending changes and replay row estimates inside a rolled-back transaction: it inserts no changelog rows, executes no changeset SQL, and does not persist bootstrap DDL or the legacy `applied_by` backfill. It now takes the advisory lock non-blocking (`pg_try_advisory_lock`) and returns `MigrationLockBusy` when a real migration holds the lock, so a preview can neither stall nor be stalled by a deploy migration.

## Config Contract Changes

- `RetryPolicy::errors_limit` (and the `RetryPolicyOverride` field) is now `u32` instead of `usize`, so the config contract no longer varies by platform pointer width.
- Duration strings must be greater than zero; `"0ms"` / `"0s"` are now rejected at parse time.
- When a `[retry]` section is present, it is validated on the `ResolvedConfig::from_config` boot path (before any database work), not only by the standalone `Config::retry_config` helper.
