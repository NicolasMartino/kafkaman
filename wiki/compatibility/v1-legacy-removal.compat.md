# V1 Legacy Removal

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-31
- Category: Public API and schema
- Scope: Records the removal, before the V1 tag, of every deprecated shim and every upgrade path that existed only to repair kafkaman's own pre-release development history.
- Sources:
  - crates/kafkaman-axum/src/lib.rs
  - crates/kafkaman-sqlx/src/changesets.rs
  - crates/kafkaman-sqlx/src/schema_sql.rs
  - crates/kafkaman-sqlx/src/migration_runner.rs
  - crates/kafkaman-core/src/idempotency.rs
  - crates/kafkaman-core/src/rows.rs
- Related:
  - wiki/compatibility/runtime-builder-and-axum.compat.md
  - wiki/compatibility/typed-idempotency-identity-api.compat.md
  - wiki/compatibility/m2-change-engine-config-schema-and-api.compat.md
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/roadmaps/path-to-v1.roadmap.md

## Why This Exists

Every item below was carried for the benefit of a database or a caller that has
never existed. kafkaman has not shipped, so "pre-existing table", "row written
before this format", and "existing adopters" all denote the empty set. Keeping
them past the tag would have frozen kafkaman's development history into its
permanent public surface — an adopter's first migration would have replayed
repairs for edits made while nothing was deployed, and its first look at the API
would have found two supervision surfaces, one deprecated on the day it shipped.

This is a one-time reset, and it is the last one. **From the V1 tag the rules
these removals suspend apply in full**: a shipped table template is never edited
in place, a public name is deprecated for a release before it is removed, and a
persisted format stays readable.

## Removed: The Duplicate Supervision Surface

`kafkaman-axum` carried a complete second runtime supervisor, deprecated in
favour of the facade's. Both were reachable through `kafkaman::axum`, where the
glob re-export and an explicit one resolved the collision by shadowing. Removed
outright:

- `RuntimeTask`, `RuntimeTask::spawn`
- `RuntimeServer`, `serve`, `RuntimeServer::with_runtime`,
  `RuntimeServer::with_runtime_drain_timeout`
- `DEFAULT_DRAIN_TIMEOUT`
- `RuntimeError` and its `Display`/`Error` impls

The canonical surface is unchanged and is the facade's:
`kafkaman::axum::serve(listener, app).with_runtime(runtime).spawn()`, with
`kafkaman::{RuntimeError, RuntimeTasks, DEFAULT_DRAIN_TIMEOUT}`. `kafkaman-axum`
is now HTTP-only — correlation middleware and operator routes — and the
`#![allow(deprecated)]` both crates needed is gone with it.

One behavior moved rather than disappeared. The deleted `with_runtime` was the
only `info`-level `kafkaman::internal` span on the supervision path, and the
span-tier test pinned it as the *message-path half* of that tier, without which
the tier could silently revert to `debug` wholesale. `RunningService::run` is now
instrumented in its place, and the assertion moved to `kafkaman::axum` beside it.

## Removed: Nine Upgrade Changesets

`schema_sql.rs` said it plainly: those alters were "the repair for three in-place
template edits", made while nothing was deployed. All nine are gone, with the SQL
builders behind them:

| Changeset | Repaired |
| --- | --- |
| `AddOutboxEntityKey` | outbox tables created before entity-first supersede |
| `AddIdempotencyKey` | outbox tables created before typed idempotency |
| `AddReceivedEntityKey` | received tables created before the entity key column |
| `AddOutboxRetentionIndex` | outbox tables created before retention |
| `AddOutboxTraceContext` | outbox tables created before distributed tracing |
| `AddReceivedTraceContext` | received tables created before distributed tracing |
| `AddReceivedFailureMetadata` | received tables created before failure columns |
| `AddReceivedFailedIndex` | received tables created before the DLQ index |
| `RemoveReceivedProcessingStatus` | received tables admitting the unwritten `Processing` status |

Also removed: `add_idempotency_key_sql`, `add_idempotency_source_sql`,
`add_outbox_entity_key_sql`, `add_received_entity_key_sql`,
`add_outbox_trace_context_sql`, `add_received_trace_context_sql`,
`add_received_failure_metadata_sql`, `backfill_received_failure_metadata_sql`,
and `remove_received_processing_status_sql`.

`RECEIVED_TEMPLATE_VERSION` drops from `1` to `0`, so **every table kind now sits
at slot 0 of its band and V1's generated changelog is one create per table**.
`RemoveReceivedProcessingStatus` had been running on every fresh database as a
no-op `UPDATE` plus a constraint drop-and-recreate; that work is gone from boot.

The `generated_changelog::upgrades` mechanism is deliberately kept, and returns
empty for all three kinds. It is the extension point the first post-V1 template
bump registers against, and the slot-0 assertion in
`shipped_template_slots_match_their_registered_upgrades` is what forces the bump
and the registration to land together.

Generated changeset *versions* are unchanged: the band digest, `BAND_WIDTH`, and
`RESERVED_CEILING` are untouched, and the removed changesets occupied slot 1,
never slot 0.

## Removed: The Nullable `changelog_history` Columns

`checksum` and `applied_by` were nullable, with two `ADD COLUMN IF NOT EXISTS`
statements and an `UPDATE … SET applied_by = 'unknown' WHERE applied_by IS NULL`
in the bootstrap, all for "history tables that predate them". Both columns are
now `NOT NULL` in the create, and the three repair statements are gone.

`HistoryRow::checksum` is `String` rather than `Option<String>`, so the
checksum-mismatch comparison no longer has a "no checksum recorded, skip
verification" branch. Every applied changeset is verified.

This supersedes the M2 spec line stating that NULL checksums are skipped by
verification and missing `applied_by` values are backfilled as `unknown`.

## Removed: Persisted-Format Back-Compat In `ReceivedError`

The `#[serde(alias = "kind")]` on `type` and `#[serde(alias = "message")]` on
`detail` read the pre-RFC-9457 shape of a stored error row. Both are gone;
`ReceivedError` reads exactly the RFC 9457 problem-detail shape it writes.

## Renamed: The Legacy String Idempotency Namespace

Not a removal — a de-legacying, and the one judgement call here worth reviewing.

`IdempotencyIdentity::derive_legacy_string` and
`LEGACY_STRING_IDEMPOTENCY_NAMESPACE` (`"kafkaman:legacy-string:v1"`) were the
retained pre-typed-identity conversion, quarantined under a "compatibility
namespace" the typed-identity compat note told callers to prefer `derive` over.

The conversion is kept, because it is load-bearing for the
`IntoIdempotencyIdentity` impls on `&str`/`String` that the test suite uses in
about thirty places, and a single already-unique string is a legitimate identity.
What is removed is its framing as a compatibility path:

- `derive_legacy_string` → `derive_from_string`
- `LEGACY_STRING_IDEMPOTENCY_NAMESPACE` → `STRING_SOURCE_IDEMPOTENCY_NAMESPACE`
- `"kafkaman:legacy-string:v1"` → `"kafkaman:string-source:v1"`

**The namespace is hashed into the digest, so every key derived from a bare
string changes.** Harmless here — no stored key exists — and it is exactly the
kind of change that stops being harmless after the tag. The doc now says when to
prefer `derive`: whenever the identity has parts, so the business namespace is
visible at the call site.

The alternative was deleting the string conversion outright and rewriting those
call sites to build typed identities. That is a defensible narrowing of the API
and it was not done, because it trades real test readability for a rule the
renamed, documented convenience already satisfies.

## Kept Deliberately

Two things read as legacy and are not:

- **`Replay::outbox`**, which always fails with `Error::UnsafeOutboxReplay`. It
  is a refusal, not a vestige: row-sourced outbox replay would republish old
  state at a *newer* Kafka offset and silently overwrite every consumer's cache,
  and the M5 spec records the rejection as delivered behavior. Its doc no longer
  says it is "retained rather than deleted" — it says why an explicit refusal
  beats a missing method for an operator mid-incident.
- **`Subsystems::all()` as the builder default.** The old justification —
  "preserves the pre-M7 behavior" — was void, but the default is right on its
  own terms: `PURGE` starts nothing without a `[retention]` section, so `all()`
  and `PIPELINE` are identical for any host that has not opted in. Both doc
  comments now give that reason instead.

## Evidence

`just lint` green, and the Docker-backed integration suite green, both after the
removals.
