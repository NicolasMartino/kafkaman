# M2 Change Engine + Config

Document Class: Spec
Status: Active
Date: 2026-06-21
Category: Change engine and configuration
Scope: Validated M2 behavior for config loading, migration reports, checksums/audit, changelog macro, dry-run, and guarded send-side replay.
Sources: wiki/plans/m2-change-engine-config.plan.md
Related: wiki/specs/m1-durable-send.spec.md, wiki/compatibility/m2-change-engine-config-schema-and-api.compat.md, wiki/roadmaps/path-to-v1.roadmap.md

## Validated Behavior

- `kafkaman-config` is a dedicated crate for `kafkaman.toml` parsing. It supports `Config::discover`, `from_path`, `from_str`, typed dotted-key access, typed `relay()` and `retry()` sections, and aggregated schema validation errors.
- `ResolvedConfig::from_config` validates required boot keys before opening or migrating a database when used through `Harness::connect_with_config` and the Axum example boot path. The Axum example discovers and resolves config before it opens a pool or touches the business schema.
- `retry` config parsing is validated for defaults, per-message overrides, merge behavior, unknown fields, unregistered message overrides, bad DLQ variants, non-finite or too-small multipliers, invalid numeric limits, and duration overflow. When a `[retry]` section is present, `ResolvedConfig::from_config` enforces this validation on the boot path, so an invalid policy is rejected before any database work. Runtime retry/DLQ processing is not implemented in M2.
- `migrate()` now takes a `MigrationContext` and returns a `MigrationReport`. Each newly applied changeset records `version`, `name`, `checksum`, `applied_by`, and `applied_at` in `changelog_history`.
- Existing M1-shaped `changelog_history` tables are upgraded with nullable `checksum` and `applied_by` columns. Existing NULL checksums are skipped by checksum verification, and missing `applied_by` values are backfilled as `unknown`.
- Changeset checksums are SHA-256 (hex) digests of the source-declared material, stored as `sha256:<64 hex chars>`. They are stable over source-declared parameters and exclude runtime config-derived values such as schema names. A changed declared parameter after application returns `ChecksumMismatch`.
- `changelog!` builds `Vec<Box<dyn Changeset>>` and rejects duplicate or disordered versions at construction (panicking with the ordering error). `try_changelog!` is the fallible sibling that returns the structured ordering error instead of panicking.
- `migrate_dry_run()` takes the schema advisory lock non-blocking (`pg_try_advisory_lock`) and returns `MigrationLockBusy` if a real migration holds it. It runs the entire preview inside one transaction that is always rolled back, so it bootstraps history shape and inspects rows without persisting any changes (no changelog rows, no `applied_by` backfill on legacy rows). It reports pending changes as `WouldApply` and checksum drift as a finding rather than applying or erroring.
- `Replay::outbox::<T>` is an operational changeset for the send-side outbox. It requires a positive `max_rows` cap, can target `MigrationContext` labels, dry-runs with an approximate row estimate, and applies one bounded `Published -> Pending` SQL statement that also resets `attempts` to `0` and clears `last_error`, so requeued rows start a fresh delivery budget.
- Re-running an already committed replay is a no-op through changelog history. A follow-up relay pass republishes the requeued rows.

## Evidence

Verified commands during M2 implementation:

- `cargo test -p kafkaman-config --lib`
- `cargo check -p kafkaman-sqlx`
- `cargo check -p kafkaman-test`
- `cargo test -p durable-send-tests --no-run`
- `cargo test -p durable-send-tests --test durable_send` (17 tests, Postgres testcontainers)
- `cargo test -p axum-outbox --test http` (Postgres testcontainer)
- `cargo check --workspace --all-features`

## Limitations

- Consume-side replay is not implemented in M2; it reuses the same mechanism in M3.
- Retry/backoff/DLQ runtime processing remains M4 scope. M2 validates only the config surface and parsing contract.
- `migrate_dry_run()` bootstraps the history table shape inside a rolled-back transaction so it can inspect history consistently; it does not persist that bootstrap, insert changelog rows, or execute changeset SQL.
