# M2 Change Engine + Config Implementation Review

- Document Class: Reference
- Status: Sourced
- Date: 2026-06-21
- Category: Code review
- Scope: Line-by-line post-implementation review of `wiki/plans/m2-change-engine-config.plan.md` against the landed Rust implementation, covering the `kafkaman-config` loader, matured `migrate()` engine, checksums/audit, `changelog!`, `MigrationReport`, dry-run, guarded `Replay`, the Harness, the Axum example, and M2 spec/compatibility docs.
- Sources:
  - `wiki/plans/m2-change-engine-config.plan.md`
  - `wiki/specs/m2-change-engine-config.spec.md`
  - `wiki/compatibility/m2-change-engine-config-schema-and-api.compat.md`
  - `crates/kafkaman-config/src/lib.rs`
  - `crates/kafkaman-sqlx/src/lib.rs`
  - `crates/kafkaman-test/src/lib.rs`
  - `apps/axum-outbox/src/main.rs`
  - `apps/axum-outbox/src/changelog.rs`
  - `apps/axum-outbox/tests/http.rs`
  - `tests/durable-send/tests/durable_send.rs`
  - `kafkaman.example.toml`

## Verdict

The M2 implementation is broadly solid: migration-safe nullable history upgrade, per-changeset checksums + `applied_by`, construction-time `changelog!` ordering checks, `MigrationReport`, `migrate_dry_run`, and bounded single-statement send-side `Replay` with `MigrationContext` are present and integration-tested.

The main issue is that the promoted plan/spec overstate a few boot-time and dry-run guarantees. Before treating the M2 spec as fully accurate, fix the Axum boot ordering, wire retry config validation into the real boot resolver, align checksum hashing with the plan, and make dry-run non-mutating for legacy history rows.

## Verification Performed

- `cargo fmt --check` - clean.
- `cargo clippy --workspace --all-features --all-targets -- -D warnings` - clean.
- `cargo check --workspace --all-features` - clean.
- `cargo test -p kafkaman-config -p kafkaman-sqlx --lib` - 13 passed.
- `cargo test -p durable-send-tests --test durable_send` - 15 passed at review time; 17 after the fixes added two gate tests. Both runs required Docker/testcontainers access.
- `cargo test -p axum-outbox --test http` - 1 passed with Docker/testcontainers access.

The first sandboxed durable-send run failed because container creation was denied by sandbox permissions. The suite passed after rerunning with Docker/testcontainers access.

## High Findings

### H1. Axum example does database work before config validation

The plan requires the example boot order to be:

`Config::discover()` -> `ResolvedConfig::from_config()` -> `MigrationContext::from_env()` -> `migrate()` -> `worker::run`

That is stated at `wiki/plans/m2-change-engine-config.plan.md:481-483`, and the spec claims `ResolvedConfig::from_config` validates required boot keys before opening or migrating a database when used through the Axum example path (`wiki/specs/m2-change-engine-config.spec.md:14`).

Actual `apps/axum-outbox/src/main.rs` reads `DATABASE_URL`, opens a `PgPool`, and runs `ensure_business_schema(&pool)` before config discovery:

- `apps/axum-outbox/src/main.rs:14-17` reads `DATABASE_URL` / `KAFKA_BROKERS` from the environment.
- `apps/axum-outbox/src/main.rs:19-22` connects to Postgres.
- `apps/axum-outbox/src/main.rs:24` runs business-schema DB work.
- `apps/axum-outbox/src/main.rs:26-30` only then discovers and resolves `kafkaman.toml`.

A missing or mistyped required config key can therefore still perform database work first. Move config discovery/resolution before pool creation and before `ensure_business_schema`.

### H2. Retry config validation is not invoked on the real boot path

`kafkaman-config` implements `retry()`, `retry_config()`, merge behavior, unknown-field rejection, unregistered-message override rejection, bad DLQ variants, bad numeric limits, non-finite multipliers, and duration overflow. Those are covered by unit tests in `crates/kafkaman-config/src/lib.rs`.

But `ResolvedConfig::from_config` validates only:

- `database.schema`
- `relay.worker_id`
- `relay.batch_limit`
- `relay.lease_for`
- `relay.retry_after`
- `relay.poll_interval`

See `crates/kafkaman-sqlx/src/lib.rs:97-102`. There is no production call to `Config::retry_config`; `rg "retry_config(" crates apps tests -g '*.rs'` finds only config-crate unit tests.

This misses the plan gate that retry/DLQ config "rejects invalid policy at boot" (`wiki/plans/m2-change-engine-config.plan.md:561-564`). Wire `cfg.retry_config(registered_message_types)` into `ResolvedConfig::from_config` or another mandatory boot resolver, and add a boot-path test where bad retry config fails before DB work.

### H3. Checksums use FNV-1a 64-bit, not the planned SHA-256 hex

The plan says checksum material is hashed with "SHA-256, hex" (`wiki/plans/m2-change-engine-config.plan.md:328`). The implementation uses the advisory-lock FNV-1a helper and stores strings like `fnv1a64:<hex>`:

- `crates/kafkaman-sqlx/src/lib.rs:903-906`
- `tests/durable-send/tests/durable_send.rs:417` now asserts the `fnv1a64:` prefix.

This is contract drift and weakens the audit/integrity property. Either implement SHA-256 hex as planned or explicitly revise the plan/spec/compatibility note to state the chosen weaker checksum format.

### H4. `migrate_dry_run()` mutates legacy `changelog_history`

The plan says dry-run "writes nothing to `changelog_history`" (`wiki/plans/m2-change-engine-config.plan.md:433-435`). The spec repeats that it "writes no changelog history rows" (`wiki/specs/m2-change-engine-config.spec.md:20`).

Actual dry-run calls `bootstrap_history(conn, cfg).await?` at `crates/kafkaman-sqlx/src/lib.rs:759`. `bootstrap_history` executes:

`UPDATE {history} SET applied_by = 'unknown' WHERE applied_by IS NULL`

at `crates/kafkaman-sqlx/src/lib.rs:937-938`.

That mutates legacy history rows during dry-run. Split bootstrap into a non-mutating inspect path for dry-run, or run the whole bootstrap in a transaction that is rolled back before returning the dry-run report.

## Medium Findings

### M1. `Replay` leaves stale `attempts` and `last_error`

`Replay` flips a bounded set of `Published` rows back to `Pending` in `replay_outbox_update_sql` (`crates/kafkaman-sqlx/src/lib.rs:636-659`). It resets status, scheduling/claim fields, and `published_at`, but does not reset `attempts` or `last_error`.

Claims increment attempts at `crates/kafkaman-sqlx/src/lib.rs:1133`. Publish failures persist `last_error` at `crates/kafkaman-sqlx/src/lib.rs:1198`. A replayed row can therefore carry stale failure state into the next relay pass. Once retry/DLQ runtime lands, a replayed row may be near or beyond a retry cap on its first post-replay claim.

Consider setting `attempts = 0` and `last_error = NULL` in replay, or document that replay preserves delivery history intentionally.

### M2. `kafkaman.example.toml` advertises keys the example app ignores

`kafkaman.example.toml` includes:

- `[database].url` at `kafkaman.example.toml:7-10`
- `[kafka].brokers` at `kafkaman.example.toml:13-14`

The Axum example reads `DATABASE_URL` and `KAFKA_BROKERS` from the process environment instead:

- `apps/axum-outbox/src/main.rs:14-17`

There is no typed `database()` or `kafka()` section validation for those file keys. Operators can edit the example TOML and reasonably expect those values to be honored, but they are ignored. Either consume and validate those keys, or remove/label them as host-owned sample values outside kafkaman's M2 config contract.

### M3. Dry-run uses the same exclusive advisory lock

Both `migrate` and `migrate_dry_run` use blocking `pg_advisory_lock`:

- `crates/kafkaman-sqlx/src/lib.rs:703-706`
- `crates/kafkaman-sqlx/src/lib.rs:733-736`

This is allowed by the current spec, which says dry-run acquires the same advisory lock. It is still an operational risk: a long dry-run can block a deploy migration, and a deploy migration can block an operator preview. Consider `pg_try_advisory_lock` for dry-run or a separate shared/read-style dry-run lock if preview availability matters.

### M4. Verification gates are missing a few targeted tests

The landed suites cover the main happy paths, but the plan's verification text calls for a few behaviors that are not directly pinned:

- A concurrent-migrate gate test proving checksum/audit writes stay inside the locked run. The current concurrency test is harness registration oriented.
- A tunable-config-change test proving schema or relay config changes do not cause false checksum mismatch.
- A dry-run legacy-history test proving dry-run does not mutate `applied_by` or insert changelog rows.
- A boot-path retry config test proving bad retry policy fails before DB work.
- A replay test asserting `attempts`/`last_error` semantics explicitly, whichever behavior is chosen.

## Low Findings

### L1. Nullable checksum decode conflates NULL with decode failure

`history_row` uses `row.try_get("checksum").ok()` (`crates/kafkaman-sqlx/src/lib.rs:990-991`). Because the query selects `checksum` after bootstrap, this mostly means NULL legacy values become `None`, which is intended. Still, `try_get::<Option<String>, _>("checksum")?` would make the NULL handling explicit and avoid hiding unrelated decode errors.

### L2. `changelog!` panics instead of returning detailed errors

`changelog!` calls:

`assert_changelog_order(&changesets).expect("invalid kafkaman changelog ordering")`

at `crates/kafkaman-sqlx/src/lib.rs:968`. That satisfies construction-time rejection, but callers lose the richer `DuplicateChangesetVersion` / `DisorderedChangesetVersion` detail unless they avoid the macro or inspect panic text. A `try_changelog!` helper or constructor function could preserve diagnostics.

### L3. Future value-carrying changesets have no bind-parameter path

`Replay::since` currently injects an RFC3339 timestamp via `sql_string_literal` (`crates/kafkaman-sqlx/src/lib.rs:677-691`). That input is typed and safe enough today, but the `Changeset::build` API only returns raw SQL strings. Future operational changesets with user-provided values will have to hand-escape or invent another mechanism. Consider a statement type that can carry binds before more value-carrying changesets land.

### L4. Duration grammar accepts zero despite "positive" wording

`parse_duration` rejects empty strings and overflow but accepts `"0ms"` / `"0s"` (`crates/kafkaman-config/src/lib.rs:585-624`). Some downstream config validation may reject invalid runtime values, but the parser's error text says "positive integer." Either reject zero in the parser or adjust wording and rely on section-level validation.

### L5. `RetryPolicy::errors_limit` uses `usize`

`RetryPolicy::errors_limit` is `usize` (`crates/kafkaman-config/src/lib.rs:462`). Config file behavior then varies by platform width. This is minor on supported targets, but a fixed-width `u32` or `u64` would be a cleaner config contract.

## Solidly Implemented

- `kafkaman-config` is a dedicated crate and keeps TOML/file IO out of `kafkaman-core`.
- `Config::discover`, `from_path`, `from_str`, typed dotted-key `get`/`get_opt`, typed `relay()` / `retry()` sections, `deny_unknown_fields`, and aggregated schema validation exist.
- Existing M1-shaped `changelog_history` upgrades with nullable `checksum` and `applied_by`.
- Legacy NULL checksum rows are skipped instead of rejected.
- Applied rows record checksum and `applied_by`.
- Mutated declared changeset parameters trigger `ChecksumMismatch`.
- `changelog!` removes boxing boilerplate and rejects duplicate/disordered versions at construction.
- `Replay` is bounded by mandatory positive `max_rows`, context-targeted via `MigrationContext`, dry-runnable with an estimate, and applies one send-side outbox `Published -> Pending` statement.
- Re-running an applied `Replay` is a no-op through changelog history.
- Follow-up relay republishes requeued rows.
- `migrate()` signature break to `Result<MigrationReport>` plus `&MigrationContext` is implemented and documented in compatibility material.

## Recommended Fix Order

1. Fix H1: move Axum config discovery/resolution before any DB connection or business-schema work.
2. Fix H2: wire retry config validation into the boot resolver and add a boot-path failure test.
3. Fix H3: switch stored checksums to SHA-256 hex or explicitly revise docs/tests to the chosen algorithm.
4. Fix H4: make `migrate_dry_run` non-mutating for legacy history.
5. Decide M1 replay semantics and test `attempts` / `last_error`.
6. Resolve M2 example TOML drift.
7. Add missing gate tests from M4.
8. Address low-level API polish before stabilizing the public surface.

## Resolution (2026-06-21)

All findings except L3 were implemented; L3 is deferred as future API design. Spec and compatibility note were updated to match.

- H1 - Fixed. `apps/axum-outbox/src/main.rs` now discovers and resolves config before creating the pool or running `ensure_business_schema`.
- H2 - Fixed. When a `[retry]` section is present, `ResolvedConfig::from_config` (`crates/kafkaman-sqlx/src/lib.rs`) validates it via `Config::retry_config(registered_message_types)` on the boot path. New gate test `invalid_retry_config_fails_before_database_work` proves a bad policy fails before any DB connect. A `Config::contains` helper was added to `kafkaman-config`.
- H3 - Fixed. `stable_checksum` now emits SHA-256 hex as `sha256:<64 hex>` (new `sha2` dependency). The integration assertion now checks the `sha256:` prefix and 64-hex length. Documented in the compat note as an algorithm change from the interim `fnv1a64:` format.
- H4 - Fixed. `migrate_dry_run` runs the whole preview inside one transaction that is always rolled back (`dry_run_in_tx`), so bootstrap DDL and the legacy `applied_by` backfill no longer persist. New gate test `dry_run_does_not_mutate_legacy_history` proves the `applied_by` column and changelog rows are untouched afterwards.
- M1 - Fixed. `replay_outbox_update_sql` now also sets `attempts = 0` and `last_error = NULL`. The replay integration test asserts no requeued row carries stale `attempts`/`last_error`.
- M2 - Fixed. `kafkaman.example.toml` now labels `[database].url` and `[kafka].brokers` as host-owned values read from the environment, outside kafkaman's M2 config contract.
- M3 - Fixed. `migrate_dry_run` uses `pg_try_advisory_lock` and returns the new `Error::MigrationLockBusy` when a real migration holds the lock.
- M4 - Fixed. Added the boot-path retry test and the dry-run legacy-history test. The replay test now asserts `attempts`/`last_error` semantics.
- L1 - Fixed. `history_row` decodes `checksum` as `try_get::<Option<String>, _>(..)?`, distinguishing NULL from a decode error.
- L2 - Fixed. Added `try_changelog!`, which returns the structured ordering error; `changelog!` is now defined in terms of it.
- L3 - Deferred. A bind-parameter-carrying statement type is left for when more value-carrying changesets land.
- L4 - Fixed. `parse_duration` rejects zero (`"0ms"` / `"0s"`).
- L5 - Fixed. `RetryPolicy::errors_limit` and the override field are now `u32`.

Verification after fixes: `cargo fmt --check`, `cargo clippy --workspace --all-features --all-targets -- -D warnings`, and `cargo check --workspace --all-features` are clean; `cargo test -p kafkaman-config -p kafkaman-sqlx --lib` = 13 passed; `cargo test -p durable-send-tests --test durable_send` = 17 passed (Docker/testcontainers); `cargo test -p axum-outbox --test http` = 1 passed.
