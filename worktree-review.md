# Worktree Review

Date: 2026-09-03
Scope: 401 files from `git ls-files --cached --others --exclude-standard` before this report was generated. The generated `worktree-review.md` file is excluded from its own scope.

## Method

- Reviewed the complete tracked plus untracked worktree file list, excluding `.git/` and build outputs by using Git's project file inventory.
- Ran line-oriented static scans for panic/unwrap/expect/sleep/print/unsafe/TODO markers, then read the high-risk runtime, SQLx, rdkafka, worker, axum, root config, and wiki files in detail.
- Used `llm_wiki_read` for `wiki/index.md`; `llm_wiki_status` reports this project is not registered for wiki search, so exhaustive enumeration used the worktree inventory.
- Score scale: 10 means release-grade, cohesive, well-tested, and easy to maintain; 7 means solid with clear improvement work; 5 means local/generated/stale or hard to review.

## Validation

| Command | Result |
| --- | --- |
| `cargo fmt --check` | Passed |
| `cargo check --workspace --all-targets` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo test --workspace --all-targets --no-run` | Passed |
| `cargo test --workspace --lib --bins` | Passed |
| `cargo test --workspace --doc` | Passed |
| `just lint` | Passed |
| `just features` | Passed |

Dirty state before generating this report: `.serena/project.yml` was modified and `.claude/.headroom_wrap_marker.json` was untracked.

## Main Findings

| Severity | File | Lines | Finding / improvement |
| --- | --- | ---: | --- |
| Medium | `crates/kafkaman-sqlx/src/outbox_enqueue.rs` | 88-148 | Reserved kafkaman headers return before an audit row is inserted, while missing idempotency records a failed row. The integration test at `tests/durable-send/tests/durable_send/idempotency_and_headers.rs:113-118` pins the asymmetry rather than endorsing it. Decide whether symmetry is required before v1. |
| Medium | `crates/kafkaman-sqlx/src/schema_sql.rs` | 51-77 | The outbox persists `idempotency_key` but does not enforce a unique send-side idempotency index. This may be correct for an at-least-once outbox with receive-side dedup, but the public semantics should be explicit and regression-tested. |
| Low | `crates/kafkaman-rdkafka/src/consumer.rs` | 443-446 | The comment says 'Before the commit' after the DB transaction is already committed; it means before the Kafka offset commit. Fix the wording to avoid misleading future edits. |
| Low | `crates/kafkaman-sqlx/src/outbox_claim.rs` | 27-34 | `claim_batch` accepts any `i64` limit and relies on callers like `RelayConfig::validate` (`crates/kafkaman-core/src/relay_config.rs:50-52`) to reject non-positive values. Validate at the SQL helper boundary too. |
| Low | `crates/kafkaman-sqlx/src/operability.rs` | 360-410 | Admin status summaries return only statuses present in SQL rows, while queue metrics expands zero-depth buckets. Returning all statuses with zero counts would make dashboard clients simpler and more stable. |
| Low | `.mcp.json` | 8 | The MCP command is an absolute user path. Use an env-based wrapper or document the local setup so the repo remains portable. |
| Low | `.gitignore` | 33 | `.serena` is ignored while `.serena/project.yml` is tracked and currently modified. Pick either committed project metadata or local-only tool state. |
| Docs | `AGENTS.md / project_guidelines.md` | 3 / 13 | The inherited project description has grammar and spelling issues such as 'reliabilty'. Fixing it would improve every copied project metadata surface. |

## Scorecard

| File | Area | State | LOC | Score | Ideas |
| --- | --- | --- | ---: | ---: | --- |
| `.claude/.headroom_wrap_marker.json` | root/config | untracked | 1 | 5.4/10 | Untracked local marker; decide whether it belongs in VCS; add an ignore rule if it is session-only. |
| `.dockerignore` | root/config | clean | 47 | 8.1/10 | Good build-context hygiene; revisit rust-toolchain exclusion if the toolchain becomes pinned; keep generated artifacts excluded. |
| `.github/workflows/ci.yml` | root/config | clean | 65 | 8.6/10 | Strong CI coverage; pin GitHub actions by SHA for supply-chain hardening; add explicit cache/toolchain comments. |
| `.gitignore` | root/config | clean | 33 | 7.6/10 | Useful ignores; resolve the tension between ignoring .serena and tracking .serena/project.yml; keep local runtime files out. |
| `.llm_wiki/init.toml` | root/config | clean | 25 | 7.0/10 | Useful project metadata; fix copied description grammar; keep framework_version aligned with installed llm-wiki. |
| `.llm_wiki/search.toml` | root/config | clean | 12 | 6.4/10 | Search metadata is stale versus framework 0.2.15 and MCP reports unregistered; refresh/register the project; document ownership. |
| `.mcp.json` | root/config | clean | 13 | 6.2/10 | Absolute user path hurts portability; prefer an env-based wrapper or documented setup; keep MCP names stable. |
| `.serena/.gitignore` | root/config | clean | 2 | 7.2/10 | Keeps memories/logs local; clarify why generated project config is tracked; avoid committing tool noise. |
| `.serena/project.yml` | root/config | modified | 169 | 5.8/10 | Generated and currently modified; decide tracked versus local-only policy; trim noisy language lists if committed. |
| `AGENTS.md` | root/config | clean | 99 | 7.4/10 | Operationally useful; fix opening paragraph typos; keep wiki/MCP fallback rules in sync with project_guidelines.md. |
| `CLAUDE.md` | root/config | clean | 1 | 7.0/10 | Simple shim to AGENTS.md; add one sentence about source of truth; keep duplicate agent instructions out. |
| `Cargo.lock` | root/config | clean | 4418 | 8.4/10 | Committed lock improves reproducibility; refresh with toolchain updates; audit dependency drift before release. |
| `Cargo.toml` | root/config | clean | 130 | 8.8/10 | Excellent workspace lint posture; document MSRV implications of Rust 1.90; keep feature graph tested by just features. |
| `README.md` | root/config | clean | 110 | 8.0/10 | Clear concise overview; add a quick-start command path; spell out pre-v1 send-side idempotency semantics. |
| `crates/kafkaman-axum/Cargo.toml` | crate | clean | 32 | 8.0/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `crates/kafkaman-axum/src/lib.rs` | crate | clean | 1621 | 8.2/10 | Good redaction and destructive-route bounds; split route/query/tests as file keeps growing; return zero-count status buckets for dashboard stability. |
| `crates/kafkaman-config/Cargo.toml` | crate | clean | 20 | 8.0/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `crates/kafkaman-config/src/config.rs` | crate | clean | 340 | 8.4/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-config/src/duration.rs` | crate | clean | 86 | 8.4/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-config/src/error.rs` | crate | clean | 128 | 8.4/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-config/src/lib.rs` | crate | clean | 37 | 8.4/10 | Facade/module surface is clear; keep public docs current; minimize re-export churn before v1. |
| `crates/kafkaman-config/src/observability.rs` | crate | clean | 321 | 8.4/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-config/src/retry.rs` | crate | clean | 142 | 8.4/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-config/src/schema.rs` | crate | clean | 76 | 8.4/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-config/src/sections.rs` | crate | clean | 247 | 8.4/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-config/src/serde_enum.rs` | crate | clean | 35 | 8.4/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-config/src/tests.rs` | crate | clean | 36 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-config/src/tests/config_file.rs` | crate | clean | 101 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-config/src/tests/dispatcher.rs` | crate | clean | 116 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-config/src/tests/duration.rs` | crate | clean | 60 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-config/src/tests/observability.rs` | crate | clean | 303 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-config/src/tests/retention.rs` | crate | clean | 71 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-config/src/tests/retry.rs` | crate | clean | 166 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-config/src/tests/schema.rs` | crate | clean | 31 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-config/src/tests/topics.rs` | crate | clean | 47 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/Cargo.toml` | crate | clean | 27 | 8.0/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `crates/kafkaman-core/src/dispatcher_config.rs` | crate | clean | 95 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/enum_macros.rs` | crate | clean | 143 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/envelope.rs` | crate | clean | 64 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/error.rs` | crate | clean | 102 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/failure_kind.rs` | crate | clean | 225 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/idempotency.rs` | crate | clean | 273 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/identifier.rs` | crate | clean | 110 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/lib.rs` | crate | clean | 82 | 8.5/10 | Facade/module surface is clear; keep public docs current; minimize re-export churn before v1. |
| `crates/kafkaman-core/src/lifecycle.rs` | crate | clean | 132 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/message.rs` | crate | clean | 102 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/problem.rs` | crate | clean | 153 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/problem_type.rs` | crate | clean | 22 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/purge_config.rs` | crate | clean | 74 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/relay_config.rs` | crate | clean | 78 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/rfc9557.rs` | crate | clean | 76 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/rows.rs` | crate | clean | 281 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/span.rs` | crate | clean | 214 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/status.rs` | crate | clean | 52 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/tests.rs` | crate | clean | 20 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/dispatcher_config.rs` | crate | clean | 61 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/failure_kind.rs` | crate | clean | 54 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/idempotency.rs` | crate | clean | 118 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/identifier.rs` | crate | clean | 36 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/lifecycle.rs` | crate | clean | 110 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/message.rs` | crate | clean | 20 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/problem.rs` | crate | clean | 325 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/purge_config.rs` | crate | clean | 62 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/relay_config.rs` | crate | clean | 122 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/rfc9557.rs` | crate | clean | 75 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/rows.rs` | crate | clean | 236 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/status.rs` | crate | clean | 67 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/text.rs` | crate | clean | 86 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/topics.rs` | crate | clean | 332 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/tests/trace.rs` | crate | clean | 263 | 8.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-core/src/text.rs` | crate | clean | 63 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/topics.rs` | crate | clean | 321 | 8.5/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-core/src/trace.rs` | crate | clean | 533 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. File size is high, so prefer extracting cohesive helpers. |
| `crates/kafkaman-otel/Cargo.toml` | crate | clean | 37 | 8.0/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `crates/kafkaman-otel/src/lib.rs` | crate | clean | 653 | 8.0/10 | Facade/module surface is clear; keep public docs current; minimize re-export churn before v1. File size is high, so prefer extracting cohesive helpers. |
| `crates/kafkaman-otel/src/tests.rs` | crate | clean | 274 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-otel/tests/endpoint_installs_providers.rs` | crate | clean | 105 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-otel/tests/init_installs_a_subscriber.rs` | crate | clean | 55 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-rdkafka/Cargo.toml` | crate | clean | 47 | 8.0/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `crates/kafkaman-rdkafka/src/consumer.rs` | crate | clean | 619 | 8.1/10 | Strong ingest/offset ordering; fix the misleading 'Before the commit' comment; add one integration assertion around breaker-before-offset behavior. |
| `crates/kafkaman-rdkafka/src/error.rs` | crate | clean | 127 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-rdkafka/src/hooks.rs` | crate | clean | 22 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-rdkafka/src/ingest_record.rs` | crate | clean | 284 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-rdkafka/src/lib.rs` | crate | clean | 32 | 8.2/10 | Facade/module surface is clear; keep public docs current; minimize re-export churn before v1. |
| `crates/kafkaman-rdkafka/src/metrics.rs` | crate | clean | 217 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-rdkafka/src/publisher.rs` | crate | clean | 232 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-rdkafka/src/stats.rs` | crate | clean | 39 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-rdkafka/src/tests.rs` | crate | clean | 478 | 8.2/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-rdkafka/src/topics.rs` | crate | clean | 266 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/Cargo.toml` | crate | clean | 36 | 8.0/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `crates/kafkaman-sqlx/src/catch_panic.rs` | crate | clean | 156 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/changelog.rs` | crate | clean | 56 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/changeset.rs` | crate | clean | 243 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/changesets.rs` | crate | clean | 137 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/dispatch.rs` | crate | clean | 715 | 8.9/10 | Strong transaction/savepoint discipline; keep missing-handler/cache ordering tests close; split handler execution and convergence helpers if it grows. |
| `crates/kafkaman-sqlx/src/dispatch_cache.rs` | crate | clean | 284 | 8.5/10 | Thoughtful stale-origin handling; add concise docs for migration-origin cases; keep conflict classification tests exhaustive. |
| `crates/kafkaman-sqlx/src/dispatch_failure.rs` | crate | clean | 249 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/error.rs` | crate | clean | 262 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/generated_changelog.rs` | crate | clean | 252 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/hooks.rs` | crate | clean | 103 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/ingest_failure.rs` | crate | clean | 52 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/lib.rs` | crate | clean | 105 | 8.3/10 | Facade/module surface is clear; keep public docs current; minimize re-export churn before v1. |
| `crates/kafkaman-sqlx/src/lock_keys.rs` | crate | clean | 40 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/migration_runner.rs` | crate | clean | 336 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/operability.rs` | crate | clean | 637 | 8.1/10 | Good capped inspection queries; align zero-bucket behavior with queue metrics; consider query-plan regression tests. |
| `crates/kafkaman-sqlx/src/outbox_claim.rs` | crate | clean | 200 | 8.0/10 | Efficient collapse-plus-claim design; validate non-positive limits at this public helper boundary; keep cost test tied to EXPLAIN output. |
| `crates/kafkaman-sqlx/src/outbox_enqueue.rs` | crate | clean | 295 | 7.9/10 | Solid atomic enqueue path; resolve reserved-header audit asymmetry; document or enforce send-side idempotency-key uniqueness semantics. |
| `crates/kafkaman-sqlx/src/outbox_mark.rs` | crate | clean | 200 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/outbox_purge.rs` | crate | clean | 83 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/queries.rs` | crate | clean | 225 | 8.0/10 | Readable shared SQL builders; prefer bound timestamp filters where feasible; keep capped pagination behavior explicit. |
| `crates/kafkaman-sqlx/src/received_rows.rs` | crate | clean | 234 | 8.4/10 | Clean SKIP LOCKED claim/update helpers; keep bounded history tests; add concurrency smoke coverage if claims change. |
| `crates/kafkaman-sqlx/src/received_storage.rs` | crate | clean | 267 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/replay.rs` | crate | clean | 331 | 8.2/10 | Good destructive-operation bounds; keep unsafe outbox replay refused; add operator docs for filter combinations. |
| `crates/kafkaman-sqlx/src/resolved_config.rs` | crate | clean | 316 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/retry_backoff.rs` | crate | clean | 117 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/roles.rs` | crate | clean | 253 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/router.rs` | crate | clean | 271 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/schema_sql.rs` | crate | clean | 293 | 8.0/10 | Migration SQL is well centralized; document send-side idempotency index choice; keep generated changelog diff tests strict. |
| `crates/kafkaman-sqlx/src/tables.rs` | crate | clean | 166 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-sqlx/src/tests.rs` | crate | clean | 101 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/catch_panic.rs` | crate | clean | 157 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/changelog.rs` | crate | clean | 80 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/dispatch_cache.rs` | crate | clean | 72 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/generated_changelog.rs` | crate | clean | 331 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/lock_keys.rs` | crate | clean | 44 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/migration_runner.rs` | crate | clean | 34 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/problem.rs` | crate | clean | 284 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/replay.rs` | crate | clean | 77 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/resolved_config.rs` | crate | clean | 233 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/retry_backoff.rs` | crate | clean | 152 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/roles.rs` | crate | clean | 227 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/schema_sql.rs` | crate | clean | 61 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-sqlx/src/tests/tables.rs` | crate | clean | 136 | 8.3/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `crates/kafkaman-test/Cargo.toml` | crate | clean | 28 | 8.0/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `crates/kafkaman-test/src/envelope_ext.rs` | crate | clean | 24 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-test/src/harness.rs` | crate | clean | 321 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-test/src/hooks.rs` | crate | clean | 48 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-test/src/lib.rs` | crate | clean | 72 | 8.3/10 | Facade/module surface is clear; keep public docs current; minimize re-export churn before v1. |
| `crates/kafkaman-test/src/publisher.rs` | crate | clean | 79 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-worker/Cargo.toml` | crate | clean | 38 | 8.0/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `crates/kafkaman-worker/src/dispatcher.rs` | crate | clean | 186 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-worker/src/lib.rs` | crate | clean | 159 | 8.2/10 | Facade/module surface is clear; keep public docs current; minimize re-export churn before v1. |
| `crates/kafkaman-worker/src/metrics.rs` | crate | clean | 362 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-worker/src/purger.rs` | crate | clean | 71 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-worker/src/queue_metrics.rs` | crate | clean | 535 | 8.6/10 | Strong gauge lifecycle and stale-snapshot policy; expose/document first-sampler provider binding; keep zero-depth buckets aligned with admin API. |
| `crates/kafkaman-worker/src/relay.rs` | crate | clean | 286 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman-worker/src/run_loop.rs` | crate | clean | 21 | 8.2/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman/Cargo.toml` | crate | clean | 67 | 8.0/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `crates/kafkaman/src/axum.rs` | crate | clean | 360 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman/src/lib.rs` | crate | clean | 71 | 8.3/10 | Facade/module surface is clear; keep public docs current; minimize re-export churn before v1. |
| `crates/kafkaman/src/runtime/builder.rs` | crate | clean | 767 | 8.6/10 | Builder validates topology well; split loop assembly helpers; add more feature-gated runtime composition cases. |
| `crates/kafkaman/src/runtime/context.rs` | crate | clean | 134 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman/src/runtime/error.rs` | crate | clean | 157 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman/src/runtime/mod.rs` | crate | clean | 34 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman/src/runtime/subsystems.rs` | crate | clean | 124 | 8.3/10 | Implementation is readable; keep error paths tested; add docs where public behavior or invariants are non-obvious. |
| `crates/kafkaman/src/runtime/tasks.rs` | crate | clean | 247 | 8.4/10 | Good supervisor accounting; log aborted task names after drain timeout; keep shutdown race tests close. |
| `crates/kafkaman/src/runtime/tests.rs` | crate | clean | 587 | 8.0/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. File size is high, so prefer extracting cohesive helpers. |
| `examples/Dockerfile` | example | clean | 132 | 7.6/10 | Dockerfile is serviceable; pin base image deliberately; keep build cache and runtime layers separated. |
| `examples/README.md` | example | clean | 860 | 7.8/10 | Documentation is useful; fix minor wording drift; add concrete commands or examples where readers need a path. |
| `examples/compose.yaml` | example | clean | 419 | 7.7/10 | Infra config is practical; add healthchecks/timeouts; keep ports and credentials documented. |
| `examples/contracts/Cargo.toml` | example | clean | 19 | 7.8/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `examples/contracts/src/lib.rs` | example | clean | 206 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/faults.sh` | example | clean | 580 | 7.4/10 | Script is useful operational glue; run shellcheck; document required env vars and external tools. |
| `examples/kibana-dashboard.sh` | example | clean | 347 | 7.4/10 | Script is useful operational glue; run shellcheck; document required env vars and external tools. |
| `examples/kibana-data-view.sh` | example | clean | 80 | 7.4/10 | Script is useful operational glue; run shellcheck; document required env vars and external tools. |
| `examples/order/Cargo.toml` | example | clean | 45 | 7.8/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `examples/order/README.md` | example | clean | 44 | 7.8/10 | Documentation is useful; fix minor wording drift; add concrete commands or examples where readers need a path. |
| `examples/order/kafkaman.toml` | example | clean | 53 | 7.6/10 | Config is readable; keep examples validated by tests; document local-only versus committed fields. |
| `examples/order/src/boot.rs` | example | clean | 34 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/order/src/http.rs` | example | clean | 500 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/order/src/lib.rs` | example | clean | 222 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/order/src/main.rs` | example | clean | 99 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/order/src/service.rs` | example | clean | 51 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/order/tests/service.rs` | example | clean | 437 | 7.8/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `examples/otel-collector.yaml` | example | clean | 172 | 7.7/10 | Infra config is practical; add healthchecks/timeouts; keep ports and credentials documented. |
| `examples/product/Cargo.toml` | example | clean | 46 | 7.8/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `examples/product/README.md` | example | clean | 89 | 7.8/10 | Documentation is useful; fix minor wording drift; add concrete commands or examples where readers need a path. |
| `examples/product/kafkaman.toml` | example | clean | 52 | 7.6/10 | Config is readable; keep examples validated by tests; document local-only versus committed fields. |
| `examples/product/src/bin/worker.rs` | example | clean | 84 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/product/src/boot.rs` | example | clean | 148 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/product/src/faults.rs` | example | clean | 361 | 7.2/10 | Intentional panic/fault injection is clear; keep it test-only/example-only; add warnings in README around enabling faults. |
| `examples/product/src/http.rs` | example | clean | 479 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/product/src/lib.rs` | example | clean | 289 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/product/src/main.rs` | example | clean | 102 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/product/src/service.rs` | example | clean | 55 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/product/src/service_manual.rs` | example | clean | 178 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/product/tests/derive_availability.rs` | example | clean | 515 | 7.5/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. File size is high, so prefer extracting cohesive helpers. |
| `examples/provision/Cargo.toml` | example | clean | 31 | 7.8/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `examples/provision/README.md` | example | clean | 70 | 7.8/10 | Documentation is useful; fix minor wording drift; add concrete commands or examples where readers need a path. |
| `examples/provision/src/lib.rs` | example | clean | 354 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/provision/src/main.rs` | example | clean | 108 | 7.8/10 | Example code is approachable; keep production and demo-only concerns separated; add README notes for operational assumptions. |
| `examples/smoke.sh` | example | clean | 258 | 7.4/10 | Script is useful operational glue; run shellcheck; document required env vars and external tools. |
| `examples/trace-handoffs.sh` | example | clean | 332 | 7.4/10 | Script is useful operational glue; run shellcheck; document required env vars and external tools. |
| `justfile` | root/config | clean | 397 | 8.3/10 | Very useful local automation; list jq/docker prerequisites; consider moving long shell bodies into scripts for testability. |
| `kafkaman.example.toml` | root/config | clean | 209 | 8.7/10 | Strong annotated config; add a minimal companion config; keep comments covered by parser/resolve tests. |
| `project_guidelines.md` | root/config | clean | 535 | 7.8/10 | Comprehensive wiki model; fix copied project-description typos; prune repeated MCP wording when rules evolve. |
| `raw/design/2026-06-20-kafkaman-architecture-discussion.md` | raw | clean | 192 | 7.1/10 | Raw source is useful provenance; ingest durable facts into wiki; add manifest pointers and avoid editing source material. |
| `raw/design/2026-08-12-entity-first-propagation-discussion.md` | raw | clean | 361 | 7.1/10 | Raw source is useful provenance; ingest durable facts into wiki; add manifest pointers and avoid editing source material. |
| `raw/design/2026-08-12-restore-policy-and-schema-separation-discussion.md` | raw | clean | 368 | 7.1/10 | Raw source is useful provenance; ingest durable facts into wiki; add manifest pointers and avoid editing source material. |
| `raw/design/2026-08-13-offset-as-convergence-ordinal-discussion.md` | raw | clean | 244 | 7.1/10 | Raw source is useful provenance; ingest durable facts into wiki; add manifest pointers and avoid editing source material. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/manifest.md` | raw | clean | 81 | 7.1/10 | Raw source is useful provenance; ingest durable facts into wiki; add manifest pointers and avoid editing source material. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/research-summary.md` | raw | clean | 109 | 7.1/10 | Raw source is useful provenance; ingest durable facts into wiki; add manifest pointers and avoid editing source material. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/01-cqrs-fullstack-migration-evidence.md` | raw | clean | 111 | 6.9/10 | Research source is useful but archival; cite it from synthesized wiki pages; keep retrieval metadata visible. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/02-rdkafka-docs.md` | raw | clean | 33 | 6.9/10 | Research source is useful but archival; cite it from synthesized wiki pages; keep retrieval metadata visible. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/03-rskafka-docs.md` | raw | clean | 32 | 6.9/10 | Research source is useful but archival; cite it from synthesized wiki pages; keep retrieval metadata visible. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/04-outbox-pattern-processor.md` | raw | clean | 37 | 6.9/10 | Research source is useful but archival; cite it from synthesized wiki pages; keep retrieval metadata visible. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/05-microservices-patterns.md` | raw | clean | 32 | 6.9/10 | Research source is useful but archival; cite it from synthesized wiki pages; keep retrieval metadata visible. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/06-debezium-outbox-event-router.md` | raw | clean | 30 | 6.9/10 | Research source is useful but archival; cite it from synthesized wiki pages; keep retrieval metadata visible. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/07-eventuate-tram.md` | raw | clean | 32 | 6.9/10 | Research source is useful but archival; cite it from synthesized wiki pages; keep retrieval metadata visible. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/08-crates-io-outbox-search.json` | raw | clean | 1 | 6.2/10 | Raw JSON is archival and hard to diff; pretty-print or summarize when useful; ingest stable facts into wiki. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/09-crates-io-kafka-search.json` | raw | clean | 1 | 6.2/10 | Raw JSON is archival and hard to diff; pretty-print or summarize when useful; ingest stable facts into wiki. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/10-crates-io-rdkafka.json` | raw | clean | 1 | 6.2/10 | Raw JSON is archival and hard to diff; pretty-print or summarize when useful; ingest stable facts into wiki. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/11-crates-io-rskafka.json` | raw | clean | 1 | 6.2/10 | Raw JSON is archival and hard to diff; pretty-print or summarize when useful; ingest stable facts into wiki. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/12-masstransit-nservicebus-outbox.md` | raw | clean | 32 | 6.9/10 | Research source is useful but archival; cite it from synthesized wiki pages; keep retrieval metadata visible. |
| `rust-toolchain.toml` | root/config | clean | 14 | 8.0/10 | Explicit toolchain channel; document why this exact channel/MSRV is acceptable; revisit before public release. |
| `tests/distributed-cache/Cargo.toml` | integration test | clean | 35 | 7.8/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `tests/distributed-cache/src/lib.rs` | integration test | clean | 362 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/distributed-cache/tests/boot_surface.rs` | integration test | clean | 175 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/distributed-cache/tests/provision.rs` | integration test | clean | 232 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/distributed-cache/tests/runtime_builder.rs` | integration test | clean | 330 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/distributed-cache/tests/two_service_cache.rs` | integration test | clean | 352 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/Cargo.toml` | integration test | clean | 40 | 7.8/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `tests/durable-send/src/containers.rs` | integration test | clean | 133 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/src/fixtures.rs` | integration test | clean | 99 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/src/lib.rs` | integration test | clean | 141 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/atomic_outbox.rs` | integration test | clean | 329 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/dispatch_failure_fallback.rs` | integration test | clean | 141 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/dispatch_handler_failure.rs` | integration test | clean | 414 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/dispatch_handler_panic.rs` | integration test | clean | 565 | 8.1/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. File size is high, so prefer extracting cohesive helpers. |
| `tests/durable-send/tests/durable_receive/dispatch_retry_schedule.rs` | integration test | clean | 183 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/dispatch_success.rs` | integration test | clean | 154 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/dispatcher_loop.rs` | integration test | clean | 180 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/dlq_inspection.rs` | integration test | clean | 473 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/idempotent_redelivery.rs` | integration test | clean | 193 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/insert_and_harness.rs` | integration test | clean | 239 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/main.rs` | integration test | clean | 121 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/redrive.rs` | integration test | clean | 384 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/redrive_filters.rs` | integration test | clean | 82 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_receive/retry_budget.rs` | integration test | clean | 308 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_send/claim_lease.rs` | integration test | clean | 106 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_send/config_validation.rs` | integration test | clean | 78 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_send/idempotency_and_headers.rs` | integration test | clean | 173 | 8.0/10 | Good pinning of current error-row asymmetry; convert the TODO-like behavior note into a tracked decision; add the eventual symmetry test. |
| `tests/durable-send/tests/durable_send/inspection.rs` | integration test | clean | 154 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_send/main.rs` | integration test | clean | 97 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_send/migrations.rs` | integration test | clean | 120 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_send/relay_and_publish.rs` | integration test | clean | 227 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/durable_send/replay.rs` | integration test | clean | 68 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/entity_first_outbox_supersede/main.rs` | integration test | clean | 17 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/entity_first_outbox_supersede/publish_ordering.rs` | integration test | clean | 189 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/entity_first_outbox_supersede/supersede_on_enqueue.rs` | integration test | clean | 166 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/entity_first_propagation/cache_apply.rs` | integration test | clean | 283 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/entity_first_propagation/dispatch_ordering.rs` | integration test | clean | 374 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/entity_first_propagation/entity_key_resolution.rs` | integration test | clean | 104 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/entity_first_propagation/main.rs` | integration test | clean | 110 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/entity_first_propagation/retry_and_redrive.rs` | integration test | clean | 274 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/outbox_claim_cost.rs` | integration test | clean | 342 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/outbox_retention.rs` | integration test | clean | 334 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/redpanda_full_loop/ingest_dedup.rs` | integration test | clean | 414 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/redpanda_full_loop/ingest_failures.rs` | integration test | clean | 322 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/redpanda_full_loop/main.rs` | integration test | clean | 155 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/redpanda_full_loop/publish_and_consume.rs` | integration test | clean | 242 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/redpanda_full_loop/retry_dlq.rs` | integration test | clean | 135 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/redpanda_full_loop/run_ingester.rs` | integration test | clean | 63 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/durable-send/tests/topic_convergence.rs` | integration test | clean | 258 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/example-telemetry/Cargo.toml` | integration test | clean | 40 | 7.8/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `tests/example-telemetry/src/lib.rs` | integration test | clean | 786 | 8.1/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. File size is high, so prefer extracting cohesive helpers. |
| `tests/example-telemetry/tests/binary_telemetry.rs` | integration test | clean | 600 | 8.1/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. File size is high, so prefer extracting cohesive helpers. |
| `tests/observability/Cargo.toml` | integration test | clean | 84 | 7.8/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `tests/observability/src/lib.rs` | integration test | clean | 660 | 8.1/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. File size is high, so prefer extracting cohesive helpers. |
| `tests/observability/tests/admin_http.rs` | integration test | clean | 581 | 8.1/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. File size is high, so prefer extracting cohesive helpers. |
| `tests/observability/tests/dispatch_failure_status.rs` | integration test | clean | 203 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/exception_events.rs` | integration test | clean | 216 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/ingest_disjointness.rs` | integration test | clean | 222 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/ingest_span_covers_decode.rs` | integration test | clean | 209 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/lifecycle_events.rs` | integration test | clean | 257 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/metrics_surface.rs` | integration test | clean | 125 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/otlp_wire.rs` | integration test | clean | 243 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/provider_ordering.rs` | integration test | clean | 76 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/queue_gauge_ordering.rs` | integration test | clean | 108 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/queue_gauge_staleness.rs` | integration test | clean | 110 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/queue_gauges.rs` | integration test | clean | 165 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/single_cycle_silence.rs` | integration test | clean | 43 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/trace_absent.rs` | integration test | clean | 162 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/trace_parented_handoff.rs` | integration test | clean | 82 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/trace_propagation.rs` | integration test | clean | 114 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/observability/tests/trace_root_enqueue.rs` | integration test | clean | 69 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `tests/otlp-capture/Cargo.toml` | integration test | clean | 26 | 7.8/10 | Manifest is focused; keep features/dependencies minimal; document optional feature interactions. |
| `tests/otlp-capture/src/lib.rs` | integration test | clean | 271 | 8.4/10 | Good scenario coverage; reduce repeated setup with helpers; keep sleeps/timeouts bounded and named. |
| `wiki/compatibility/dispatch-handler-ordering.compat.md` | wiki | clean | 128 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/kafkaman-otel-surface.compat.md` | wiki | clean | 188 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/m1-durable-send-schema-and-api-changes.compatibility.md` | wiki | clean | 76 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/m2-change-engine-config-schema-and-api.compat.md` | wiki | clean | 34 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/m3-durable-receive-review-fix-api.compat.md` | wiki | clean | 95 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/m4-retry-backoff-runtime-api.compat.md` | wiki | clean | 51 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/m5-code-audit-remediation.compat.md` | wiki | clean | 136 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/m5-entity-first-cache-api.compat.md` | wiki | clean | 76 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/m5-entity-first-outbox-supersede.compat.md` | wiki | clean | 69 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/m5-outbox-retention.compat.md` | wiki | clean | 90 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/m6-observability-operability-api.compat.md` | wiki | clean | 1620 | 6.9/10 | Very complete but oversized; split public API, metrics, and admin HTTP compatibility notes; add an executive summary. |
| `wiki/compatibility/m7-hardening-api.compat.md` | wiki | clean | 258 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/module-test-separation-internal-hooks.compat.md` | wiki | clean | 148 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/runtime-builder-and-axum.compat.md` | wiki | clean | 222 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/topic-convergence-api.compat.md` | wiki | clean | 141 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/typed-idempotency-identity-api.compat.md` | wiki | clean | 166 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/compatibility/v1-legacy-removal.compat.md` | wiki | clean | 169 | 7.7/10 | Compatibility note is important; state semver impact crisply; keep migration/test references current. |
| `wiki/decisions/apm-waterfall-trace-shape.decision.md` | wiki | clean | 181 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/configuration-and-environment-model.decision.md` | wiki | clean | 104 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/consumer-test-tooling.decision.md` | wiki | clean | 182 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/dispatch-handler-ordering.decision.md` | wiki | clean | 214 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/dispatch-infrastructure-error-classification.decision.md` | wiki | clean | 46 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/dispatch-stats-semantics.decision.md` | wiki | clean | 40 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/entity-first-propagation-model.decision.md` | wiki | clean | 268 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/example-telemetry-integration-test-boundary.decision.md` | wiki | clean | 154 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/failure-taxonomy-and-blame-separation.decision.md` | wiki | clean | 204 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/failures-as-typed-exceptions.decision.md` | wiki | clean | 232 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/handler-panic-containment-and-fault-injection.decision.md` | wiki | clean | 216 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/ingest-poison-quarantine-policy.decision.md` | wiki | clean | 47 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/kafka-ingest-identity-and-ordering.decision.md` | wiki | clean | 47 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/kafkaman-otel-extraction.decision.md` | wiki | clean | 169 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/library-test-strategy.decision.md` | wiki | clean | 162 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/message-consumption-and-handler-model.decision.md` | wiki | clean | 254 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/message-identity-and-header-namespace.decision.md` | wiki | clean | 92 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/messaging-scope-and-receive-model.decision.md` | wiki | clean | 116 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/method-level-timing-and-span-depth.decision.md` | wiki | clean | 278 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/metric-instrument-and-attribute-schema.decision.md` | wiki | clean | 251 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/missing-handler-dispatch-policy.decision.md` | wiki | clean | 62 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/observability-operability-policy.decision.md` | wiki | clean | 149 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/outbox-retention-policy.decision.md` | wiki | clean | 99 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/receive-handler-surface-scope.decision.md` | wiki | clean | 41 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/retry-backoff-dlq-policy.decision.md` | wiki | clean | 98 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/runtime-builder-and-axum-composition.decision.md` | wiki | clean | 317 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/runtime-composition-and-topology.decision.md` | wiki | clean | 241 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/schema-and-change-management.decision.md` | wiki | clean | 203 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/telemetry-backend-and-example-topology.decision.md` | wiki | clean | 304 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/telemetry-pipeline-ownership.decision.md` | wiki | clean | 218 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/topic-convergence-and-rebuild.decision.md` | wiki | clean | 174 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md` | wiki | clean | 423 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md` | wiki | clean | 123 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/decisions/v1-roadmap-execution-policy.decision.md` | wiki | clean | 80 | 8.1/10 | Decision record is useful; keep outcome/status fresh; link tests or code that enforce it. |
| `wiki/index.md` | wiki | clean | 1038 | 7.2/10 | Comprehensive but heavy index; split or summarize long catalogs; fix stale search registration and keep links audited. |
| `wiki/log.md` | wiki | clean | 6223 | 7.0/10 | Valuable audit trail; archive older periods or add month anchors; keep newest entries concise and linked. |
| `wiki/plans/apm-waterfall-traces.plan.md` | wiki | clean | 486 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/entity-first-propagation.plan.md` | wiki | clean | 272 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/example-telemetry-integration-tests.plan.md` | wiki | clean | 354 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/failure-examples.plan.md` | wiki | clean | 223 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/failure-taxonomy-separation.plan.md` | wiki | clean | 111 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/first-poc-outbox-publisher.plan.md` | wiki | clean | 134 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/kafkaman-otel-extraction.plan.md` | wiki | clean | 213 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/m1-durable-send-implementation.plan.md` | wiki | clean | 487 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/m2-change-engine-config.plan.md` | wiki | clean | 611 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/m3-durable-completion.plan.md` | wiki | clean | 327 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/m3-durable-receive.plan.md` | wiki | clean | 272 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/m4-retry-backoff-dlq.plan.md` | wiki | clean | 105 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/m7-v1-hardening.plan.md` | wiki | clean | 339 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/method-level-timing.plan.md` | wiki | clean | 176 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/opentelemetry-completion.plan.md` | wiki | clean | 600 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/outbox-retention.plan.md` | wiki | clean | 92 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/runtime-builder-and-axum-composition.plan.md` | wiki | clean | 531 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/topic-convergence.plan.md` | wiki | clean | 236 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/two-service-distributed-cache-example.plan.md` | wiki | clean | 334 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/plans/typed-idempotency-identity-error-row-fix.plan.md` | wiki | clean | 96 | 7.7/10 | Plan captures execution detail; mark completed/deferred items; link follow-up decisions. |
| `wiki/proposals/01-kafkaman-objectives.proposal.md` | wiki | clean | 168 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/02-messaging-scope-kafka-command-vs-durable-execution.proposal.md` | wiki | clean | 81 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/03-direct-transport-mode.proposal.md` | wiki | clean | 79 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/04-observability-logging-policy.proposal.md` | wiki | clean | 133 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/05-deep-durability-testing.proposal.md` | wiki | clean | 693 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/06-typed-idempotency-identity-and-error-row-symmetry.proposal.md` | wiki | clean | 68 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md` | wiki | clean | 235 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/08-listen-notify-scheduler-wakeup.proposal.md` | wiki | clean | 158 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/09-entity-first-propagation.proposal.md` | wiki | clean | 447 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md` | wiki | clean | 364 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md` | wiki | clean | 327 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/12-entity-only-message-model.proposal.md` | wiki | clean | 337 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/13-telemetry-pipeline-completion.proposal.md` | wiki | clean | 240 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/14-runtime-builder-and-axum-composition.proposal.md` | wiki | clean | 508 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/15-dispatch-concurrency-and-middleware.proposal.md` | wiki | clean | 311 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/16-message-contract-derive.proposal.md` | wiki | clean | 198 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/17-kafkaman-otel-convenience-crate.proposal.md` | wiki | clean | 205 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/18-example-telemetry-integration-tests.proposal.md` | wiki | clean | 172 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/19-apm-waterfall-traces.proposal.md` | wiki | clean | 186 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/20-method-level-timing.proposal.md` | wiki | clean | 117 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/21-failure-examples-and-panic-containment.proposal.md` | wiki | clean | 141 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/22-failure-taxonomy-and-blame.proposal.md` | wiki | clean | 179 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/proposals/23-topic-convergence-and-environment-provisioning.proposal.md` | wiki | clean | 308 | 7.6/10 | Proposal history is useful; mark superseded sections; link accepted/rejected decisions. |
| `wiki/references/rust-kafka-outbox-ecosystem.reference.md` | wiki | clean | 80 | 7.8/10 | Wiki page has useful institutional memory; keep metadata/index links fresh; archive stale or superseded content. |
| `wiki/reviews/m1-durable-send-implementation-rereview.reference.md` | wiki | clean | 467 | 7.5/10 | Review artifact preserves context; close resolved findings; link any follow-up patches. |
| `wiki/reviews/m1-durable-send-implementation-review.reference.md` | wiki | clean | 474 | 7.5/10 | Review artifact preserves context; close resolved findings; link any follow-up patches. |
| `wiki/reviews/m2-change-engine-config-implementation-review.reference.md` | wiki | clean | 206 | 7.5/10 | Review artifact preserves context; close resolved findings; link any follow-up patches. |
| `wiki/reviews/m3-durable-completion-implementation-rereview.reference.md` | wiki | clean | 225 | 7.5/10 | Review artifact preserves context; close resolved findings; link any follow-up patches. |
| `wiki/reviews/m3-durable-completion-implementation-review.reference.md` | wiki | clean | 344 | 7.5/10 | Review artifact preserves context; close resolved findings; link any follow-up patches. |
| `wiki/reviews/m3-durable-receive-implementation-review.reference.md` | wiki | clean | 367 | 7.5/10 | Review artifact preserves context; close resolved findings; link any follow-up patches. |
| `wiki/reviews/m3-m4-pre-merge-branch-review.reference.md` | wiki | clean | 235 | 7.5/10 | Review artifact preserves context; close resolved findings; link any follow-up patches. |
| `wiki/reviews/m6-opentelemetry-readiness-review.reference.md` | wiki | clean | 180 | 7.5/10 | Review artifact preserves context; close resolved findings; link any follow-up patches. |
| `wiki/roadmaps/path-to-v1.roadmap.md` | wiki | clean | 301 | 7.8/10 | Wiki page has useful institutional memory; keep metadata/index links fresh; archive stale or superseded content. |
| `wiki/specs/entity-first-propagation.spec.md` | wiki | clean | 289 | 8.2/10 | Spec is valuable; keep acceptance criteria traceable to tests; update status as implementation changes. |
| `wiki/specs/m1-durable-send.spec.md` | wiki | clean | 131 | 8.2/10 | Spec is valuable; keep acceptance criteria traceable to tests; update status as implementation changes. |
| `wiki/specs/m2-change-engine-config.spec.md` | wiki | clean | 40 | 8.2/10 | Spec is valuable; keep acceptance criteria traceable to tests; update status as implementation changes. |
| `wiki/specs/m3-durable-receive.spec.md` | wiki | clean | 111 | 8.2/10 | Spec is valuable; keep acceptance criteria traceable to tests; update status as implementation changes. |
| `wiki/specs/m4-retry-backoff-dlq.spec.md` | wiki | clean | 119 | 8.2/10 | Spec is valuable; keep acceptance criteria traceable to tests; update status as implementation changes. |
| `wiki/specs/m6-observability-operability.spec.md` | wiki | clean | 358 | 8.2/10 | Spec is valuable; keep acceptance criteria traceable to tests; update status as implementation changes. |
| `wiki/specs/v1-acceptance.spec.md` | wiki | clean | 101 | 8.2/10 | Spec is valuable; keep acceptance criteria traceable to tests; update status as implementation changes. |
