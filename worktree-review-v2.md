# Worktree Review V2

Date: 2026-09-03
Scope: 414 current files from `git ls-files --cached --others --exclude-standard`; this newly generated `worktree-review-v2.md` is excluded from its own scope. The staged `worktree-review.md` artifact is included because it is currently part of the worktree inventory.

## Verdict

The worktree is materially stronger, but the claim that every file is at least 9/10 is not true yet: 25 of 414 scored files are below 9.0.

The important code paths are in good shape: the `kafkaman-axum` split is clean, public re-exports are small, strict lints pass, MSRV is verified, audit triage is explicit, and the full project gate passes when run through the supported `just check` path outside the socket-restricted sandbox.

## Validation

| Command | Result | Notes |
| --- | --- | --- |
| `cargo fmt --check` | Passed | Current worktree formatting. |
| `cargo check --workspace --all-targets` | Passed | All targets compile. |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed | Strict lint gate. |
| `cargo test --workspace --all-targets --no-run` | Passed | All test targets build. |
| `cargo test --workspace --all-targets` | Invocation failed | Direct invocation lacks `EXAMPLE_ORDER_BIN`/`EXAMPLE_PRODUCT_BIN`; the test itself says to use `just examples telemetry-test` or `just test all`. |
| `just check` | Passed | Needed escalation because two HTTP supervision tests bind local sockets blocked by sandbox. |
| `just msrv` | Passed | Workspace checks on Rust 1.90 with `--locked`. |
| `just audit` | Passed | Escalated for Cargo advisory DB; 1 allowed yanked warning for dev-only `chacha20`. |
| `just publish-order` | Passed | Escalated for crates.io index; leaf package dry run succeeded. |

## Findings

| Severity | File | Lines | Finding / Improvement |
| --- | --- | ---: | --- |
| Medium | `wiki/compatibility/m6-observability-operability-api.compat.md` | 116-121 | Active compatibility note still lists `MAX_CORRELATION_ID_LEN` as public even though it is `pub(crate)`, and still lists removed `kafkaman-axum` supervision exports (`serve`, `RuntimeServer`, `RuntimeTask`, `RuntimeError`, `DEFAULT_DRAIN_TIMEOUT`) as that crate's public surface. `wiki/compatibility/v1-legacy-removal.compat.md:45-52` records those removals, so this page should either become explicitly historical or update the current-surface wording. |
| Medium | `worktree-review.md` | 32-35 | The staged review artifact is stale and contains pre-fix sub-9 scores/findings. If committed as-is, the worktree still literally contains a report proving the file-score target was not met. Delete it, unstage it, or replace it with the current rereview artifact. |
| Low | `crates/kafkaman-rdkafka/src/consumer.rs` | 443 | Comment still says 'Before the commit' after the ingest quarantine transaction commit. It means before committing the Kafka offset; wording remains misleading for future edits. |
| Low | `crates/kafkaman-sqlx/src/outbox_claim.rs` | 27-34 | `claim_batch` still accepts any `i64` limit and relies on caller config validation. Add a helper-boundary guard for non-positive limits so direct callers get deterministic errors before SQL. |
| Low | `crates/kafkaman-sqlx/src/operability.rs` | 360-410 | Status summaries still return only `GROUP BY` rows that exist, while queue metrics expands missing statuses to explicit zero-depth buckets. Aligning them would make admin clients and dashboards simpler. |
| Low | `.github/workflows/ci.yml` | 38-42,54-58,71-79,92-93,101-106 | Runner image is pinned now, but third-party actions are still tag-pinned rather than SHA-pinned. That is acceptable for most projects, not a 9+ supply-chain posture. |
| Low | `justfile` | 167,223-230 | `just msrv` hides `rustup toolchain install` failures with `|| true`, and `publish-order` only packages leaf crates before publish. Both are understandable, but the failure modes should be explicit in output/docs. |
| Low | `README.md / justfile` | 113-123 / 15,453 | The verification docs call out Docker but not `jq`, `cargo-audit`, or rustup/toolchain prerequisites used by the new recipes. |
| Low | `AGENTS.md / project_guidelines.md / .llm_wiki/init.toml` | 3 / 13 / 3 | The copied project description still contains grammar issues and `reliabilty`; it keeps propagating into agent and project metadata. |
| Low | `wiki/index.md / wiki/log.md` | 1 / 1 | Both are wiki files without the metadata block the guidelines require of every wiki page. If index/log are exempt, codify that exception; otherwise add metadata. |
| Low | `.llm_wiki/search.toml` | 6-7 | Search config records an old setup (`configured_by_version = 0.2.1`), and MCP search still reports `project_not_registered`. The wiki search path remains operationally below 9. |
| Low | `.serena/project.yml` | 1-169 | Tracked tool config remains unstaged-modified generated churn. Decide whether it is shared project config or local tool state before release. |

## Below 9

| File | State | LOC | Score | Why below 9 |
| --- | --- | ---: | ---: | --- |
| `.cargo/audit.toml` | staged add | 47 | 8.9/10 | RUSTSEC ignore is well reasoned and audit passes; add a short note that yanked dev-only chacha20 remains an allowed warning. |
| `.github/workflows/ci.yml` | staged modify | 107 | 8.7/10 | Good CI expansion with fixed runner image, MSRV, lockfile, audit, and coverage jobs; pin third-party actions by SHA before release. |
| `.gitignore` | staged modify | 42 | 8.8/10 | Clear local-tool rationale and .claude ignore; add final newline; periodically revisit tracked .serena exception. |
| `.llm_wiki/init.toml` | clean | 25 | 7.4/10 | Framework version current, but copied project description still has grammar/spelling errors; fix source text once and regenerate metadata. |
| `.llm_wiki/search.toml` | clean | 12 | 6.6/10 | Search metadata is stale and MCP reports project_not_registered; register/re-index the project or remove stale search config from release surface. |
| `.serena/.gitignore` | clean | 2 | 8.8/10 | Useful local-memory ignore; keep generated Serena state out of commits and clarify tracked project.yml ownership. |
| `.serena/project.yml` | unstaged modify | 169 | 6.7/10 | Tracked tool config is currently unstaged-modified generated churn; stage intentionally, revert, or untrack before release. |
| `AGENTS.md` | staged modify | 115 | 7.8/10 | Project instructions are useful, but the opening description still has typos/grammar issues; fix it because it is copied into metadata and guidelines. |
| `README.md` | staged modify | 132 | 8.8/10 | Verification section is much stronger; add local prerequisites for jq, cargo-audit, rustup/toolchains, and Docker expectations. |
| `crates/kafkaman-axum/src/redrive.rs` | staged add | 173 | 8.9/10 | Destructive route split and request validation are good; fix stale/awkward doc link around failure_kind inspection workflow. |
| `crates/kafkaman-rdkafka/src/consumer.rs` | clean | 619 | 8.8/10 | Ingest/offset ordering is tested and strong, but line 443 still says 'Before the commit' when it means before committing the Kafka offset. |
| `crates/kafkaman-sqlx/src/operability.rs` | clean | 637 | 8.9/10 | Inspection SQL is solid; consider aligning admin status summaries with queue-metric zero buckets for client stability. |
| `crates/kafkaman-sqlx/src/outbox_claim.rs` | clean | 200 | 8.9/10 | Claim/collapse SQL is efficient; add defensive non-positive limit validation at this helper boundary, not only in caller config. |
| `examples/README.md` | clean | 860 | 8.9/10 | Example documentation is detailed and operational; add anchors/short paths where it grows long. |
| `justfile` | staged modify | 470 | 8.7/10 | Release gates are valuable and passed; make rustup install failures visible, document jq/cargo-audit prerequisites, and say publish-order only packages leaf crates pre-publish. |
| `project_guidelines.md` | clean | 535 | 8.0/10 | Documentation model is strong, but the copied project description is still malformed and the file is dense; polish before treating it as 9+ guidance. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/08-crates-io-outbox-search.json` | clean | 1 | 8.7/10 | Raw JSON provenance is useful but minified/no final newline; keep immutable, but add a wiki summary with retrieval metadata. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/09-crates-io-kafka-search.json` | clean | 1 | 8.7/10 | Raw JSON provenance is useful but minified/no final newline; keep immutable, but add a wiki summary with retrieval metadata. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/10-crates-io-rdkafka.json` | clean | 1 | 8.7/10 | Raw JSON provenance is useful but minified/no final newline; keep immutable, but add a wiki summary with retrieval metadata. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/11-crates-io-rskafka.json` | clean | 1 | 8.7/10 | Raw JSON provenance is useful but minified/no final newline; keep immutable, but add a wiki summary with retrieval metadata. |
| `wiki/compatibility/m6-observability-operability-api.compat.md` | staged modify | 1621 | 7.6/10 | Active note is oversized and stale: it still lists private MAX_CORRELATION_ID_LEN and removed kafkaman-axum supervision exports as public surface. |
| `wiki/decisions/failure-taxonomy-and-blame-separation.decision.md` | clean | 204 | 8.9/10 | Decision content is useful and metadata is present; add the missing final newline for clean text-file hygiene. |
| `wiki/index.md` | staged modify | 1042 | 8.3/10 | All index links resolve and current status is useful, but the file is very large and lacks the metadata block required of wiki pages unless index/log are explicit exceptions. |
| `wiki/log.md` | staged modify | 6404 | 7.2/10 | Valuable audit history but 6400 lines, no metadata block, and mixed historical/current assertions; archive or segment by period before a 9+ score. |
| `worktree-review.md` | staged add | 445 | 5.0/10 | Staged stale/generated review artifact with pre-fix scores below 9; remove it from the release commit or replace it with the current rereview. |

## Score Distribution

| Score | Files |
| ---: | ---: |
| 5.0/10 | 1 |
| 6.6/10 | 1 |
| 6.7/10 | 1 |
| 7.2/10 | 1 |
| 7.4/10 | 1 |
| 7.6/10 | 1 |
| 7.8/10 | 1 |
| 8.0/10 | 1 |
| 8.3/10 | 1 |
| 8.7/10 | 6 |
| 8.8/10 | 4 |
| 8.9/10 | 6 |
| 9.0/10 | 324 |
| 9.1/10 | 65 |

## Full Scorecard

| File | Area | State | LOC | Score | Ideas |
| --- | --- | --- | ---: | ---: | --- |
| `.cargo/audit.toml` | root/config | staged add | 47 | 8.9/10 | RUSTSEC ignore is well reasoned and audit passes; add a short note that yanked dev-only chacha20 remains an allowed warning. |
| `.dockerignore` | root/config | clean | 47 | 9.0/10 | Build context hygiene is clear; revisit toolchain exclusion only if release packaging starts needing it. |
| `.github/workflows/ci.yml` | root/config | staged modify | 107 | 8.7/10 | Good CI expansion with fixed runner image, MSRV, lockfile, audit, and coverage jobs; pin third-party actions by SHA before release. |
| `.gitignore` | root/config | staged modify | 42 | 8.8/10 | Clear local-tool rationale and .claude ignore; add final newline; periodically revisit tracked .serena exception. |
| `.llm_wiki/init.toml` | root/config | clean | 25 | 7.4/10 | Framework version current, but copied project description still has grammar/spelling errors; fix source text once and regenerate metadata. |
| `.llm_wiki/search.toml` | root/config | clean | 12 | 6.6/10 | Search metadata is stale and MCP reports project_not_registered; register/re-index the project or remove stale search config from release surface. |
| `.mcp.json` | root/config | staged modify | 13 | 9.0/10 | Absolute path fixed; keep relying on PATH-based llm-wiki and document setup for contributors. |
| `.serena/.gitignore` | root/config | clean | 2 | 8.8/10 | Useful local-memory ignore; keep generated Serena state out of commits and clarify tracked project.yml ownership. |
| `.serena/project.yml` | root/config | unstaged modify | 169 | 6.7/10 | Tracked tool config is currently unstaged-modified generated churn; stage intentionally, revert, or untrack before release. |
| `AGENTS.md` | root/config | staged modify | 115 | 7.8/10 | Project instructions are useful, but the opening description still has typos/grammar issues; fix it because it is copied into metadata and guidelines. |
| `CLAUDE.md` | root/config | clean | 1 | 9.0/10 | Correctly delegates to AGENTS.md without duplicating instructions; keep it as a shim. |
| `Cargo.lock` | root/config | clean | 4418 | 9.0/10 | Committed lockfile is current under the lockfile gate and audit scan; keep dependency changes intentional. |
| `Cargo.toml` | root/config | staged modify | 139 | 9.0/10 | Workspace metadata, lints, features, and MSRV are now release-oriented and verified by gates. |
| `README.md` | root/config | staged modify | 132 | 8.8/10 | Verification section is much stronger; add local prerequisites for jq, cargo-audit, rustup/toolchains, and Docker expectations. |
| `crates/kafkaman-axum/Cargo.toml` | crate | staged modify | 37 | 9.0/10 | Manifest inherits release metadata and pins features clearly; keep dependency versions publishable. |
| `crates/kafkaman-axum/src/admin.rs` | crate | staged add | 408 | 9.1/10 | Good read-only route separation, capped lists, table-access filtering, and exact tests; keep auth warnings prominent. |
| `crates/kafkaman-axum/src/correlation.rs` | crate | staged add | 168 | 9.1/10 | Correlation layer has bounded cardinality, matched-route span naming, and good tests; keep header policy stable. |
| `crates/kafkaman-axum/src/error.rs` | crate | staged add | 192 | 9.0/10 | Error mapping redacts storage details and distinguishes caller/deployment repairs; keep response bodies intentionally small. |
| `crates/kafkaman-axum/src/lib.rs` | crate | staged modify | 40 | 9.0/10 | Crate root is now a clean module graph and public re-export surface; keep it this small. |
| `crates/kafkaman-axum/src/redrive.rs` | crate | staged add | 173 | 8.9/10 | Destructive route split and request validation are good; fix stale/awkward doc link around failure_kind inspection workflow. |
| `crates/kafkaman-axum/src/state.rs` | crate | staged add | 23 | 9.1/10 | Small focused state holder; keep shared router state centralized here. |
| `crates/kafkaman-axum/src/tests.rs` | crate | staged add | 211 | 9.0/10 | Shared test harness isolates subscriber-global behavior; keep helper comments near the tricky tracing cache behavior. |
| `crates/kafkaman-axum/src/tests/admin.rs` | crate | staged add | 155 | 9.0/10 | Good route-separation and span-tier assertions; keep lazy-pool tests database-free. |
| `crates/kafkaman-axum/src/tests/correlation.rs` | crate | staged add | 108 | 9.0/10 | Covers matched/unmatched routes and header bounds; keep cardinality assertions as regression gates. |
| `crates/kafkaman-axum/src/tests/error.rs` | crate | staged add | 113 | 9.0/10 | Good status and redaction coverage; assert body shape if client contract grows. |
| `crates/kafkaman-axum/src/tests/redrive.rs` | crate | staged add | 69 | 9.0/10 | Good destructive-route parsing coverage; keep unknown-field and dual failure_kind spelling tests. |
| `crates/kafkaman-axum/src/tests/wire_format.rs` | crate | staged add | 151 | 9.0/10 | Strong wire-format timestamp and redaction assertions; keep every new response timestamp covered here. |
| `crates/kafkaman-config/Cargo.toml` | crate | staged modify | 25 | 9.0/10 | Manifest inherits release metadata and pins features clearly; keep dependency versions publishable. |
| `crates/kafkaman-config/src/config.rs` | crate | clean | 340 | 9.0/10 | Config parsing/validation is focused and well tested; keep unknown-field and merge tests complete. |
| `crates/kafkaman-config/src/duration.rs` | crate | clean | 86 | 9.0/10 | Config parsing/validation is focused and well tested; keep unknown-field and merge tests complete. |
| `crates/kafkaman-config/src/error.rs` | crate | clean | 128 | 9.0/10 | Config parsing/validation is focused and well tested; keep unknown-field and merge tests complete. |
| `crates/kafkaman-config/src/lib.rs` | crate | clean | 37 | 9.0/10 | Config parsing/validation is focused and well tested; keep unknown-field and merge tests complete. |
| `crates/kafkaman-config/src/observability.rs` | crate | clean | 321 | 9.0/10 | Config parsing/validation is focused and well tested; keep unknown-field and merge tests complete. |
| `crates/kafkaman-config/src/retry.rs` | crate | clean | 142 | 9.0/10 | Config parsing/validation is focused and well tested; keep unknown-field and merge tests complete. |
| `crates/kafkaman-config/src/schema.rs` | crate | clean | 76 | 9.0/10 | Config parsing/validation is focused and well tested; keep unknown-field and merge tests complete. |
| `crates/kafkaman-config/src/sections.rs` | crate | clean | 247 | 9.0/10 | Config parsing/validation is focused and well tested; keep unknown-field and merge tests complete. |
| `crates/kafkaman-config/src/serde_enum.rs` | crate | clean | 35 | 9.0/10 | Config parsing/validation is focused and well tested; keep unknown-field and merge tests complete. |
| `crates/kafkaman-config/src/tests.rs` | crate | clean | 36 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-config/src/tests/config_file.rs` | crate | clean | 101 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-config/src/tests/dispatcher.rs` | crate | clean | 116 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-config/src/tests/duration.rs` | crate | clean | 60 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-config/src/tests/observability.rs` | crate | clean | 303 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-config/src/tests/retention.rs` | crate | clean | 71 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-config/src/tests/retry.rs` | crate | clean | 166 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-config/src/tests/schema.rs` | crate | clean | 31 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-config/src/tests/topics.rs` | crate | clean | 47 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/Cargo.toml` | crate | staged modify | 32 | 9.0/10 | Manifest inherits release metadata and pins features clearly; keep dependency versions publishable. |
| `crates/kafkaman-core/src/dispatcher_config.rs` | crate | clean | 95 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/enum_macros.rs` | crate | clean | 143 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/envelope.rs` | crate | clean | 64 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/error.rs` | crate | clean | 102 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/failure_kind.rs` | crate | clean | 225 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/idempotency.rs` | crate | clean | 273 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/identifier.rs` | crate | clean | 110 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/lib.rs` | crate | clean | 82 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/lifecycle.rs` | crate | clean | 132 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/message.rs` | crate | clean | 102 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/problem.rs` | crate | clean | 153 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/problem_type.rs` | crate | clean | 22 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/purge_config.rs` | crate | clean | 74 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/relay_config.rs` | crate | clean | 78 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/rfc9557.rs` | crate | clean | 76 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/rows.rs` | crate | clean | 281 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/span.rs` | crate | clean | 214 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/status.rs` | crate | clean | 52 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/tests.rs` | crate | clean | 20 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/dispatcher_config.rs` | crate | clean | 61 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/failure_kind.rs` | crate | clean | 54 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/idempotency.rs` | crate | clean | 118 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/identifier.rs` | crate | clean | 36 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/lifecycle.rs` | crate | clean | 110 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/message.rs` | crate | clean | 20 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/problem.rs` | crate | clean | 325 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/purge_config.rs` | crate | clean | 62 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/relay_config.rs` | crate | clean | 122 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/rfc9557.rs` | crate | clean | 75 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/rows.rs` | crate | clean | 236 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/status.rs` | crate | clean | 67 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/text.rs` | crate | clean | 86 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/topics.rs` | crate | clean | 332 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/tests/trace.rs` | crate | clean | 263 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-core/src/text.rs` | crate | clean | 63 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/topics.rs` | crate | clean | 321 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-core/src/trace.rs` | crate | clean | 533 | 9.1/10 | Core API/invariant code is cohesive and well tested; preserve compatibility notes for public behavior changes. |
| `crates/kafkaman-otel/Cargo.toml` | crate | staged modify | 42 | 9.0/10 | Manifest inherits release metadata and pins features clearly; keep dependency versions publishable. |
| `crates/kafkaman-otel/src/lib.rs` | crate | clean | 653 | 9.0/10 | Rust source is clear and passes strict lints; keep public invariants documented. |
| `crates/kafkaman-otel/src/tests.rs` | crate | clean | 274 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-otel/tests/endpoint_installs_providers.rs` | crate | clean | 105 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-otel/tests/init_installs_a_subscriber.rs` | crate | clean | 55 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-rdkafka/Cargo.toml` | crate | staged modify | 52 | 9.0/10 | Manifest inherits release metadata and pins features clearly; keep dependency versions publishable. |
| `crates/kafkaman-rdkafka/src/consumer.rs` | crate | clean | 619 | 8.8/10 | Ingest/offset ordering is tested and strong, but line 443 still says 'Before the commit' when it means before committing the Kafka offset. |
| `crates/kafkaman-rdkafka/src/error.rs` | crate | clean | 127 | 9.0/10 | Kafka boundary code is well covered by header/trace/ingest tests; keep broker-loop assertions for ordering windows. |
| `crates/kafkaman-rdkafka/src/hooks.rs` | crate | clean | 22 | 9.0/10 | Kafka boundary code is well covered by header/trace/ingest tests; keep broker-loop assertions for ordering windows. |
| `crates/kafkaman-rdkafka/src/ingest_record.rs` | crate | clean | 284 | 9.0/10 | Kafka boundary code is well covered by header/trace/ingest tests; keep broker-loop assertions for ordering windows. |
| `crates/kafkaman-rdkafka/src/lib.rs` | crate | clean | 32 | 9.0/10 | Kafka boundary code is well covered by header/trace/ingest tests; keep broker-loop assertions for ordering windows. |
| `crates/kafkaman-rdkafka/src/metrics.rs` | crate | clean | 217 | 9.0/10 | Kafka boundary code is well covered by header/trace/ingest tests; keep broker-loop assertions for ordering windows. |
| `crates/kafkaman-rdkafka/src/publisher.rs` | crate | clean | 232 | 9.0/10 | Kafka boundary code is well covered by header/trace/ingest tests; keep broker-loop assertions for ordering windows. |
| `crates/kafkaman-rdkafka/src/stats.rs` | crate | clean | 39 | 9.0/10 | Kafka boundary code is well covered by header/trace/ingest tests; keep broker-loop assertions for ordering windows. |
| `crates/kafkaman-rdkafka/src/tests.rs` | crate | clean | 478 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-rdkafka/src/topics.rs` | crate | clean | 266 | 9.0/10 | Kafka boundary code is well covered by header/trace/ingest tests; keep broker-loop assertions for ordering windows. |
| `crates/kafkaman-sqlx/Cargo.toml` | crate | staged modify | 41 | 9.0/10 | Manifest inherits release metadata and pins features clearly; keep dependency versions publishable. |
| `crates/kafkaman-sqlx/src/catch_panic.rs` | crate | clean | 156 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/changelog.rs` | crate | clean | 56 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/changeset.rs` | crate | clean | 243 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/changesets.rs` | crate | clean | 137 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/dispatch.rs` | crate | clean | 715 | 9.1/10 | High-risk transaction/savepoint behavior is explicit and heavily tested; preserve ordering tests before refactors. |
| `crates/kafkaman-sqlx/src/dispatch_cache.rs` | crate | clean | 284 | 9.0/10 | Cache convergence and origin migration behavior are well isolated; keep conflict classification tests exhaustive. |
| `crates/kafkaman-sqlx/src/dispatch_failure.rs` | crate | clean | 249 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/error.rs` | crate | clean | 262 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/generated_changelog.rs` | crate | clean | 252 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/hooks.rs` | crate | clean | 103 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/ingest_failure.rs` | crate | clean | 52 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/lib.rs` | crate | clean | 105 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/lock_keys.rs` | crate | clean | 40 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/migration_runner.rs` | crate | clean | 336 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/operability.rs` | crate | clean | 637 | 8.9/10 | Inspection SQL is solid; consider aligning admin status summaries with queue-metric zero buckets for client stability. |
| `crates/kafkaman-sqlx/src/outbox_claim.rs` | crate | clean | 200 | 8.9/10 | Claim/collapse SQL is efficient; add defensive non-positive limit validation at this helper boundary, not only in caller config. |
| `crates/kafkaman-sqlx/src/outbox_enqueue.rs` | crate | clean | 295 | 9.0/10 | Atomic enqueue path is strong; reserved-header no-audit behavior is now documented as intentional and test-pinned. |
| `crates/kafkaman-sqlx/src/outbox_mark.rs` | crate | clean | 200 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/outbox_purge.rs` | crate | clean | 83 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/queries.rs` | crate | clean | 225 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/received_rows.rs` | crate | clean | 234 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/received_storage.rs` | crate | clean | 267 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/replay.rs` | crate | clean | 331 | 9.0/10 | Runtime redrive is bounded and outbox replay is refused; keep destructive-operation validation centralized. |
| `crates/kafkaman-sqlx/src/resolved_config.rs` | crate | clean | 316 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/retry_backoff.rs` | crate | clean | 117 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/roles.rs` | crate | clean | 253 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/router.rs` | crate | clean | 271 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/schema_sql.rs` | crate | clean | 293 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/tables.rs` | crate | clean | 166 | 9.0/10 | SQL behavior is explicit and strongly tested; keep migration/checksum tests close to schema changes. |
| `crates/kafkaman-sqlx/src/tests.rs` | crate | clean | 101 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/catch_panic.rs` | crate | clean | 157 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/changelog.rs` | crate | clean | 80 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/dispatch_cache.rs` | crate | clean | 72 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/generated_changelog.rs` | crate | clean | 331 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/lock_keys.rs` | crate | clean | 44 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/migration_runner.rs` | crate | clean | 34 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/problem.rs` | crate | clean | 284 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/replay.rs` | crate | clean | 77 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/resolved_config.rs` | crate | clean | 233 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/retry_backoff.rs` | crate | clean | 152 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/roles.rs` | crate | clean | 227 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/schema_sql.rs` | crate | clean | 61 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-sqlx/src/tests/tables.rs` | crate | clean | 136 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `crates/kafkaman-test/Cargo.toml` | crate | staged modify | 33 | 9.0/10 | Manifest inherits release metadata and pins features clearly; keep dependency versions publishable. |
| `crates/kafkaman-test/src/envelope_ext.rs` | crate | clean | 24 | 9.0/10 | Rust source is clear and passes strict lints; keep public invariants documented. |
| `crates/kafkaman-test/src/harness.rs` | crate | clean | 321 | 9.0/10 | Rust source is clear and passes strict lints; keep public invariants documented. |
| `crates/kafkaman-test/src/hooks.rs` | crate | clean | 48 | 9.0/10 | Rust source is clear and passes strict lints; keep public invariants documented. |
| `crates/kafkaman-test/src/lib.rs` | crate | clean | 72 | 9.0/10 | Rust source is clear and passes strict lints; keep public invariants documented. |
| `crates/kafkaman-test/src/publisher.rs` | crate | clean | 79 | 9.0/10 | Rust source is clear and passes strict lints; keep public invariants documented. |
| `crates/kafkaman-worker/Cargo.toml` | crate | staged modify | 43 | 9.0/10 | Manifest inherits release metadata and pins features clearly; keep dependency versions publishable. |
| `crates/kafkaman-worker/src/dispatcher.rs` | crate | clean | 186 | 9.0/10 | Worker loop code follows consistent run-loop patterns; keep shutdown and metric semantics covered. |
| `crates/kafkaman-worker/src/lib.rs` | crate | clean | 159 | 9.0/10 | Worker loop code follows consistent run-loop patterns; keep shutdown and metric semantics covered. |
| `crates/kafkaman-worker/src/metrics.rs` | crate | clean | 362 | 9.0/10 | Worker loop code follows consistent run-loop patterns; keep shutdown and metric semantics covered. |
| `crates/kafkaman-worker/src/purger.rs` | crate | clean | 71 | 9.0/10 | Worker loop code follows consistent run-loop patterns; keep shutdown and metric semantics covered. |
| `crates/kafkaman-worker/src/queue_metrics.rs` | crate | clean | 535 | 9.0/10 | Sampler snapshot design, zero-depth buckets, and staleness reporting are strong; keep provider-ordering tests. |
| `crates/kafkaman-worker/src/relay.rs` | crate | clean | 286 | 9.0/10 | Worker loop code follows consistent run-loop patterns; keep shutdown and metric semantics covered. |
| `crates/kafkaman-worker/src/run_loop.rs` | crate | clean | 21 | 9.0/10 | Worker loop code follows consistent run-loop patterns; keep shutdown and metric semantics covered. |
| `crates/kafkaman/Cargo.toml` | crate | staged modify | 73 | 9.0/10 | Manifest inherits release metadata and pins features clearly; keep dependency versions publishable. |
| `crates/kafkaman/README.md` | crate | staged add | 82 | 9.0/10 | Useful facade-crate docs for adopters; keep examples aligned with public API after re-export changes. |
| `crates/kafkaman/src/axum.rs` | crate | clean | 360 | 9.0/10 | Rust source is clear and passes strict lints; keep public invariants documented. |
| `crates/kafkaman/src/lib.rs` | crate | clean | 71 | 9.0/10 | Rust source is clear and passes strict lints; keep public invariants documented. |
| `crates/kafkaman/src/runtime/builder.rs` | crate | clean | 767 | 9.0/10 | Runtime topology validation and task assembly are solid; future edits should preserve feature-matrix coverage. |
| `crates/kafkaman/src/runtime/context.rs` | crate | clean | 134 | 9.0/10 | Rust source is clear and passes strict lints; keep public invariants documented. |
| `crates/kafkaman/src/runtime/error.rs` | crate | clean | 157 | 9.0/10 | Rust source is clear and passes strict lints; keep public invariants documented. |
| `crates/kafkaman/src/runtime/mod.rs` | crate | clean | 34 | 9.0/10 | Rust source is clear and passes strict lints; keep public invariants documented. |
| `crates/kafkaman/src/runtime/subsystems.rs` | crate | clean | 124 | 9.0/10 | Rust source is clear and passes strict lints; keep public invariants documented. |
| `crates/kafkaman/src/runtime/tasks.rs` | crate | clean | 247 | 9.0/10 | Supervisor semantics are well tested; keep task names stable for diagnostics. |
| `crates/kafkaman/src/runtime/tests.rs` | crate | clean | 587 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `examples/Dockerfile` | example | clean | 132 | 9.0/10 | File is small and purposeful; keep ownership clear. |
| `examples/README.md` | example | clean | 860 | 8.9/10 | Example documentation is detailed and operational; add anchors/short paths where it grows long. |
| `examples/compose.yaml` | example | clean | 419 | 9.0/10 | Configuration is readable and validated by local gates; keep local-only values out of committed files. |
| `examples/contracts/Cargo.toml` | example | staged modify | 22 | 9.0/10 | Manifest is aligned with workspace metadata; keep dev-only dependencies isolated. |
| `examples/contracts/src/lib.rs` | example | clean | 206 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/faults.sh` | example | clean | 580 | 9.0/10 | Operational script is exercised by examples/tests; keep shellcheck-style strictness and documented env vars. |
| `examples/kibana-dashboard.sh` | example | clean | 347 | 9.0/10 | Operational script is exercised by examples/tests; keep shellcheck-style strictness and documented env vars. |
| `examples/kibana-data-view.sh` | example | clean | 80 | 9.0/10 | Operational script is exercised by examples/tests; keep shellcheck-style strictness and documented env vars. |
| `examples/order/Cargo.toml` | example | staged modify | 48 | 9.0/10 | Manifest is aligned with workspace metadata; keep dev-only dependencies isolated. |
| `examples/order/README.md` | example | clean | 44 | 9.0/10 | Example documentation is detailed and operational; add anchors/short paths where it grows long. |
| `examples/order/kafkaman.toml` | example | clean | 53 | 9.0/10 | Configuration is readable and validated by local gates; keep local-only values out of committed files. |
| `examples/order/src/boot.rs` | example | clean | 34 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/order/src/http.rs` | example | clean | 500 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/order/src/lib.rs` | example | clean | 222 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/order/src/main.rs` | example | clean | 99 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/order/src/service.rs` | example | clean | 51 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/order/tests/service.rs` | example | clean | 437 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `examples/otel-collector.yaml` | example | clean | 172 | 9.0/10 | Configuration is readable and validated by local gates; keep local-only values out of committed files. |
| `examples/product/Cargo.toml` | example | staged modify | 49 | 9.0/10 | Manifest is aligned with workspace metadata; keep dev-only dependencies isolated. |
| `examples/product/README.md` | example | clean | 89 | 9.0/10 | Example documentation is detailed and operational; add anchors/short paths where it grows long. |
| `examples/product/kafkaman.toml` | example | clean | 52 | 9.0/10 | Configuration is readable and validated by local gates; keep local-only values out of committed files. |
| `examples/product/src/bin/worker.rs` | example | clean | 84 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/product/src/boot.rs` | example | clean | 148 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/product/src/faults.rs` | example | clean | 361 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/product/src/http.rs` | example | clean | 479 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/product/src/lib.rs` | example | clean | 289 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/product/src/main.rs` | example | clean | 102 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/product/src/service.rs` | example | clean | 55 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/product/src/service_manual.rs` | example | clean | 178 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/product/tests/derive_availability.rs` | example | clean | 515 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `examples/provision/Cargo.toml` | example | staged modify | 32 | 9.0/10 | Manifest is aligned with workspace metadata; keep dev-only dependencies isolated. |
| `examples/provision/README.md` | example | clean | 70 | 9.0/10 | Example documentation is detailed and operational; add anchors/short paths where it grows long. |
| `examples/provision/src/lib.rs` | example | clean | 354 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/provision/src/main.rs` | example | clean | 108 | 9.0/10 | Example code is clear and covered by integration tests; keep demo-only concerns visibly isolated. |
| `examples/smoke.sh` | example | clean | 258 | 9.0/10 | Operational script is exercised by examples/tests; keep shellcheck-style strictness and documented env vars. |
| `examples/trace-handoffs.sh` | example | clean | 332 | 9.0/10 | Operational script is exercised by examples/tests; keep shellcheck-style strictness and documented env vars. |
| `justfile` | root/config | staged modify | 470 | 8.7/10 | Release gates are valuable and passed; make rustup install failures visible, document jq/cargo-audit prerequisites, and say publish-order only packages leaf crates pre-publish. |
| `kafkaman.example.toml` | root/config | clean | 209 | 9.0/10 | Annotated config remains strong and covered by config tests; keep comments synchronized with schema validation. |
| `project_guidelines.md` | root/config | clean | 535 | 8.0/10 | Documentation model is strong, but the copied project description is still malformed and the file is dense; polish before treating it as 9+ guidance. |
| `raw/design/2026-06-20-kafkaman-architecture-discussion.md` | raw | clean | 192 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/design/2026-08-12-entity-first-propagation-discussion.md` | raw | clean | 361 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/design/2026-08-12-restore-policy-and-schema-separation-discussion.md` | raw | clean | 368 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/design/2026-08-13-offset-as-convergence-ordinal-discussion.md` | raw | clean | 244 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/manifest.md` | raw | clean | 81 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/research-summary.md` | raw | clean | 109 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/01-cqrs-fullstack-migration-evidence.md` | raw | clean | 111 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/02-rdkafka-docs.md` | raw | clean | 33 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/03-rskafka-docs.md` | raw | clean | 32 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/04-outbox-pattern-processor.md` | raw | clean | 37 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/05-microservices-patterns.md` | raw | clean | 32 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/06-debezium-outbox-event-router.md` | raw | clean | 30 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/07-eventuate-tram.md` | raw | clean | 32 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/08-crates-io-outbox-search.json` | raw | clean | 1 | 8.7/10 | Raw JSON provenance is useful but minified/no final newline; keep immutable, but add a wiki summary with retrieval metadata. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/09-crates-io-kafka-search.json` | raw | clean | 1 | 8.7/10 | Raw JSON provenance is useful but minified/no final newline; keep immutable, but add a wiki summary with retrieval metadata. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/10-crates-io-rdkafka.json` | raw | clean | 1 | 8.7/10 | Raw JSON provenance is useful but minified/no final newline; keep immutable, but add a wiki summary with retrieval metadata. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/11-crates-io-rskafka.json` | raw | clean | 1 | 8.7/10 | Raw JSON provenance is useful but minified/no final newline; keep immutable, but add a wiki summary with retrieval metadata. |
| `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/12-masstransit-nservicebus-outbox.md` | raw | clean | 32 | 9.0/10 | Raw source is preserved as provenance; do not edit unless replacing source material intentionally. |
| `rust-toolchain.toml` | root/config | clean | 14 | 9.0/10 | Toolchain pin is simple and matches the verified MSRV posture; update deliberately with release notes. |
| `tests/distributed-cache/Cargo.toml` | integration test | staged modify | 36 | 9.0/10 | Manifest is aligned with workspace metadata; keep dev-only dependencies isolated. |
| `tests/distributed-cache/src/lib.rs` | integration test | clean | 362 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/distributed-cache/tests/boot_surface.rs` | integration test | clean | 175 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/distributed-cache/tests/provision.rs` | integration test | clean | 232 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/distributed-cache/tests/runtime_builder.rs` | integration test | clean | 330 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/distributed-cache/tests/two_service_cache.rs` | integration test | clean | 352 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/Cargo.toml` | integration test | staged modify | 43 | 9.0/10 | Manifest is aligned with workspace metadata; keep dev-only dependencies isolated. |
| `tests/durable-send/src/containers.rs` | integration test | clean | 133 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/src/fixtures.rs` | integration test | clean | 99 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/src/lib.rs` | integration test | clean | 141 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/atomic_outbox.rs` | integration test | clean | 329 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/dispatch_failure_fallback.rs` | integration test | clean | 141 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/dispatch_handler_failure.rs` | integration test | clean | 414 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/dispatch_handler_panic.rs` | integration test | clean | 565 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/dispatch_retry_schedule.rs` | integration test | clean | 183 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/dispatch_success.rs` | integration test | clean | 154 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/dispatcher_loop.rs` | integration test | clean | 180 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/dlq_inspection.rs` | integration test | clean | 473 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/idempotent_redelivery.rs` | integration test | clean | 193 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/insert_and_harness.rs` | integration test | clean | 239 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/main.rs` | integration test | clean | 121 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/redrive.rs` | integration test | clean | 384 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/redrive_filters.rs` | integration test | clean | 82 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_receive/retry_budget.rs` | integration test | clean | 308 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_send/claim_lease.rs` | integration test | clean | 106 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_send/config_validation.rs` | integration test | clean | 78 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_send/idempotency_and_headers.rs` | integration test | clean | 173 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_send/inspection.rs` | integration test | clean | 154 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_send/main.rs` | integration test | clean | 97 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_send/migrations.rs` | integration test | clean | 120 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_send/relay_and_publish.rs` | integration test | clean | 227 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/durable_send/replay.rs` | integration test | clean | 68 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/entity_first_outbox_supersede/main.rs` | integration test | clean | 17 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/entity_first_outbox_supersede/publish_ordering.rs` | integration test | clean | 189 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/entity_first_outbox_supersede/supersede_on_enqueue.rs` | integration test | clean | 166 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/entity_first_propagation/cache_apply.rs` | integration test | clean | 283 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/entity_first_propagation/dispatch_ordering.rs` | integration test | clean | 374 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/entity_first_propagation/entity_key_resolution.rs` | integration test | clean | 104 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/entity_first_propagation/main.rs` | integration test | clean | 110 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/entity_first_propagation/retry_and_redrive.rs` | integration test | clean | 274 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/outbox_claim_cost.rs` | integration test | clean | 342 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/outbox_retention.rs` | integration test | clean | 334 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/redpanda_full_loop/ingest_dedup.rs` | integration test | clean | 414 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/redpanda_full_loop/ingest_failures.rs` | integration test | clean | 322 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/redpanda_full_loop/main.rs` | integration test | clean | 155 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/redpanda_full_loop/publish_and_consume.rs` | integration test | clean | 242 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/redpanda_full_loop/retry_dlq.rs` | integration test | clean | 135 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/redpanda_full_loop/run_ingester.rs` | integration test | clean | 63 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/durable-send/tests/topic_convergence.rs` | integration test | clean | 258 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/example-telemetry/Cargo.toml` | integration test | staged modify | 41 | 9.0/10 | Manifest is aligned with workspace metadata; keep dev-only dependencies isolated. |
| `tests/example-telemetry/src/lib.rs` | integration test | clean | 786 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. File is large; avoid adding unrelated responsibilities. |
| `tests/example-telemetry/tests/binary_telemetry.rs` | integration test | clean | 600 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/Cargo.toml` | integration test | staged modify | 85 | 9.0/10 | Manifest is aligned with workspace metadata; keep dev-only dependencies isolated. |
| `tests/observability/src/lib.rs` | integration test | clean | 660 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/admin_http.rs` | integration test | clean | 581 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/dispatch_failure_status.rs` | integration test | clean | 203 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/exception_events.rs` | integration test | clean | 216 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/ingest_disjointness.rs` | integration test | clean | 222 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/ingest_span_covers_decode.rs` | integration test | clean | 209 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/lifecycle_events.rs` | integration test | clean | 257 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/metrics_surface.rs` | integration test | clean | 125 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/otlp_wire.rs` | integration test | clean | 243 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/provider_ordering.rs` | integration test | clean | 76 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/queue_gauge_ordering.rs` | integration test | clean | 108 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/queue_gauge_staleness.rs` | integration test | clean | 110 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/queue_gauges.rs` | integration test | clean | 165 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/single_cycle_silence.rs` | integration test | clean | 43 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/trace_absent.rs` | integration test | clean | 162 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/trace_parented_handoff.rs` | integration test | clean | 82 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/trace_propagation.rs` | integration test | clean | 114 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/observability/tests/trace_root_enqueue.rs` | integration test | clean | 69 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `tests/otlp-capture/Cargo.toml` | integration test | staged modify | 27 | 9.0/10 | Manifest is aligned with workspace metadata; keep dev-only dependencies isolated. |
| `tests/otlp-capture/src/lib.rs` | integration test | clean | 271 | 9.0/10 | Well-scoped regression coverage; keep scenario setup factored and timeouts bounded. |
| `wiki/compatibility/dispatch-handler-ordering.compat.md` | wiki | clean | 128 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/kafkaman-otel-surface.compat.md` | wiki | clean | 188 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/m1-durable-send-schema-and-api-changes.compat.md` | wiki | staged rename | 76 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/m2-change-engine-config-schema-and-api.compat.md` | wiki | clean | 34 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/m3-durable-receive-review-fix-api.compat.md` | wiki | clean | 95 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/m4-retry-backoff-runtime-api.compat.md` | wiki | staged modify | 51 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/m5-code-audit-remediation.compat.md` | wiki | clean | 136 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/m5-entity-first-cache-api.compat.md` | wiki | staged modify | 76 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/m5-entity-first-outbox-supersede.compat.md` | wiki | staged modify | 69 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/m5-outbox-retention.compat.md` | wiki | clean | 90 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/m6-observability-operability-api.compat.md` | wiki | staged modify | 1621 | 7.6/10 | Active note is oversized and stale: it still lists private MAX_CORRELATION_ID_LEN and removed kafkaman-axum supervision exports as public surface. |
| `wiki/compatibility/m7-hardening-api.compat.md` | wiki | clean | 258 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/module-test-separation-internal-hooks.compat.md` | wiki | clean | 148 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/runtime-builder-and-axum.compat.md` | wiki | clean | 222 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/topic-convergence-api.compat.md` | wiki | clean | 141 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/typed-idempotency-identity-api.compat.md` | wiki | clean | 166 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/compatibility/v1-legacy-removal.compat.md` | wiki | clean | 169 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/decisions/apm-waterfall-trace-shape.decision.md` | wiki | clean | 181 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/configuration-and-environment-model.decision.md` | wiki | clean | 104 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/consumer-test-tooling.decision.md` | wiki | clean | 182 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/dispatch-handler-ordering.decision.md` | wiki | clean | 214 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/dispatch-infrastructure-error-classification.decision.md` | wiki | staged modify | 46 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/dispatch-stats-semantics.decision.md` | wiki | staged modify | 40 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/entity-first-propagation-model.decision.md` | wiki | clean | 268 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/example-telemetry-integration-test-boundary.decision.md` | wiki | clean | 154 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/failure-taxonomy-and-blame-separation.decision.md` | wiki | clean | 204 | 8.9/10 | Decision content is useful and metadata is present; add the missing final newline for clean text-file hygiene. |
| `wiki/decisions/failures-as-typed-exceptions.decision.md` | wiki | clean | 232 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/handler-panic-containment-and-fault-injection.decision.md` | wiki | clean | 216 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/ingest-poison-quarantine-policy.decision.md` | wiki | clean | 47 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/kafka-ingest-identity-and-ordering.decision.md` | wiki | staged modify | 47 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/kafkaman-otel-extraction.decision.md` | wiki | staged modify | 171 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/library-test-strategy.decision.md` | wiki | clean | 162 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/message-consumption-and-handler-model.decision.md` | wiki | clean | 254 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/message-identity-and-header-namespace.decision.md` | wiki | clean | 92 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/messaging-scope-and-receive-model.decision.md` | wiki | clean | 116 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/method-level-timing-and-span-depth.decision.md` | wiki | clean | 278 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/metric-instrument-and-attribute-schema.decision.md` | wiki | clean | 251 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/missing-handler-dispatch-policy.decision.md` | wiki | staged modify | 62 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/observability-operability-policy.decision.md` | wiki | staged modify | 149 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/outbox-retention-policy.decision.md` | wiki | clean | 99 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/receive-handler-surface-scope.decision.md` | wiki | staged modify | 41 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/retry-backoff-dlq-policy.decision.md` | wiki | clean | 98 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/runtime-builder-and-axum-composition.decision.md` | wiki | clean | 317 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/runtime-composition-and-topology.decision.md` | wiki | clean | 241 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/schema-and-change-management.decision.md` | wiki | clean | 203 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/telemetry-backend-and-example-topology.decision.md` | wiki | clean | 304 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/telemetry-pipeline-ownership.decision.md` | wiki | staged modify | 224 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/topic-convergence-and-rebuild.decision.md` | wiki | clean | 174 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md` | wiki | clean | 423 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md` | wiki | clean | 123 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/decisions/v1-roadmap-execution-policy.decision.md` | wiki | clean | 80 | 9.1/10 | Decision record is well structured; keep consequences and revisit triggers alive. |
| `wiki/index.md` | wiki | staged modify | 1042 | 8.3/10 | All index links resolve and current status is useful, but the file is very large and lacks the metadata block required of wiki pages unless index/log are explicit exceptions. |
| `wiki/log.md` | wiki | staged modify | 6404 | 7.2/10 | Valuable audit history but 6400 lines, no metadata block, and mixed historical/current assertions; archive or segment by period before a 9+ score. |
| `wiki/plans/apm-waterfall-traces.plan.md` | wiki | clean | 486 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/entity-first-propagation.plan.md` | wiki | staged modify | 272 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/example-telemetry-integration-tests.plan.md` | wiki | clean | 354 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/failure-examples.plan.md` | wiki | clean | 223 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/failure-taxonomy-separation.plan.md` | wiki | clean | 111 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/first-poc-outbox-publisher.plan.md` | wiki | clean | 134 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/kafkaman-otel-extraction.plan.md` | wiki | clean | 213 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/m1-durable-send-implementation.plan.md` | wiki | staged modify | 487 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/m2-change-engine-config.plan.md` | wiki | staged modify | 611 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/m3-durable-completion.plan.md` | wiki | clean | 327 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/m3-durable-receive.plan.md` | wiki | clean | 272 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/m4-retry-backoff-dlq.plan.md` | wiki | staged modify | 105 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/m7-v1-hardening.plan.md` | wiki | clean | 339 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/method-level-timing.plan.md` | wiki | clean | 176 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/opentelemetry-completion.plan.md` | wiki | clean | 600 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/outbox-retention.plan.md` | wiki | clean | 92 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/runtime-builder-and-axum-composition.plan.md` | wiki | clean | 531 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/topic-convergence.plan.md` | wiki | clean | 236 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/two-service-distributed-cache-example.plan.md` | wiki | clean | 334 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/plans/typed-idempotency-identity-error-row-fix.plan.md` | wiki | clean | 96 | 9.0/10 | Plan records execution clearly; keep completed/deferred state current. |
| `wiki/proposals/01-kafkaman-objectives.proposal.md` | wiki | clean | 168 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/02-messaging-scope-kafka-command-vs-durable-execution.proposal.md` | wiki | clean | 81 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/03-direct-transport-mode.proposal.md` | wiki | clean | 79 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/04-observability-logging-policy.proposal.md` | wiki | clean | 133 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/05-deep-durability-testing.proposal.md` | wiki | staged modify | 693 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/06-typed-idempotency-identity-and-error-row-symmetry.proposal.md` | wiki | clean | 68 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md` | wiki | clean | 235 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/08-listen-notify-scheduler-wakeup.proposal.md` | wiki | clean | 158 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/09-entity-first-propagation.proposal.md` | wiki | clean | 447 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md` | wiki | clean | 364 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md` | wiki | clean | 327 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/12-entity-only-message-model.proposal.md` | wiki | clean | 337 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/13-telemetry-pipeline-completion.proposal.md` | wiki | clean | 240 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/14-runtime-builder-and-axum-composition.proposal.md` | wiki | clean | 508 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/15-dispatch-concurrency-and-middleware.proposal.md` | wiki | clean | 311 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/16-message-contract-derive.proposal.md` | wiki | clean | 198 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/17-kafkaman-otel-convenience-crate.proposal.md` | wiki | clean | 205 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/18-example-telemetry-integration-tests.proposal.md` | wiki | clean | 172 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/19-apm-waterfall-traces.proposal.md` | wiki | clean | 186 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/20-method-level-timing.proposal.md` | wiki | clean | 117 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/21-failure-examples-and-panic-containment.proposal.md` | wiki | clean | 141 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/22-failure-taxonomy-and-blame.proposal.md` | wiki | clean | 179 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/proposals/23-topic-convergence-and-environment-provisioning.proposal.md` | wiki | clean | 308 | 9.0/10 | Proposal history is useful; link accepted decisions and mark superseded sections. |
| `wiki/references/rust-kafka-outbox-ecosystem.reference.md` | wiki | clean | 80 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/reviews/m1-durable-send-implementation-rereview.reference.md` | wiki | clean | 467 | 9.0/10 | Historical review evidence is valuable; keep it immutable except for explicit errata. |
| `wiki/reviews/m1-durable-send-implementation-review.reference.md` | wiki | clean | 474 | 9.0/10 | Historical review evidence is valuable; keep it immutable except for explicit errata. |
| `wiki/reviews/m2-change-engine-config-implementation-review.reference.md` | wiki | clean | 206 | 9.0/10 | Historical review evidence is valuable; keep it immutable except for explicit errata. |
| `wiki/reviews/m3-durable-completion-implementation-rereview.reference.md` | wiki | clean | 225 | 9.0/10 | Historical review evidence is valuable; keep it immutable except for explicit errata. |
| `wiki/reviews/m3-durable-completion-implementation-review.reference.md` | wiki | clean | 344 | 9.0/10 | Historical review evidence is valuable; keep it immutable except for explicit errata. |
| `wiki/reviews/m3-durable-receive-implementation-review.reference.md` | wiki | clean | 367 | 9.0/10 | Historical review evidence is valuable; keep it immutable except for explicit errata. |
| `wiki/reviews/m3-m4-pre-merge-branch-review.reference.md` | wiki | clean | 235 | 9.0/10 | Historical review evidence is valuable; keep it immutable except for explicit errata. |
| `wiki/reviews/m6-opentelemetry-readiness-review.reference.md` | wiki | clean | 180 | 9.0/10 | Historical review evidence is valuable; keep it immutable except for explicit errata. |
| `wiki/roadmaps/path-to-v1.roadmap.md` | wiki | clean | 301 | 9.0/10 | Wiki page has metadata and index coverage; keep status, sources, and related pages synchronized. |
| `wiki/specs/entity-first-propagation.spec.md` | wiki | staged modify | 289 | 9.1/10 | Spec is traceable to tests and decisions; keep sources pointing at current proof paths. |
| `wiki/specs/m1-durable-send.spec.md` | wiki | staged modify | 131 | 9.1/10 | Spec is traceable to tests and decisions; keep sources pointing at current proof paths. |
| `wiki/specs/m2-change-engine-config.spec.md` | wiki | clean | 40 | 9.1/10 | Spec is traceable to tests and decisions; keep sources pointing at current proof paths. |
| `wiki/specs/m3-durable-receive.spec.md` | wiki | staged modify | 111 | 9.1/10 | Spec is traceable to tests and decisions; keep sources pointing at current proof paths. |
| `wiki/specs/m4-retry-backoff-dlq.spec.md` | wiki | staged modify | 119 | 9.1/10 | Spec is traceable to tests and decisions; keep sources pointing at current proof paths. |
| `wiki/specs/m6-observability-operability.spec.md` | wiki | staged modify | 358 | 9.1/10 | Spec is traceable to tests and decisions; keep sources pointing at current proof paths. |
| `wiki/specs/v1-acceptance.spec.md` | wiki | clean | 101 | 9.1/10 | Spec is traceable to tests and decisions; keep sources pointing at current proof paths. |
| `worktree-review.md` | root/config | staged add | 445 | 5.0/10 | Staged stale/generated review artifact with pre-fix scores below 9; remove it from the release commit or replace it with the current rereview. |
