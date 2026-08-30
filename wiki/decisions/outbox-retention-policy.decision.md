# Outbox Retention Policy

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-24
- Category: Operational model
- Scope: Which kafkaman tables are purged, on what window, by what mechanism, and which are never purged.
- Sources:
  - wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md
  - review.md
  - crates/kafkaman-sqlx/src/lib.rs
- Related:
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md
  - wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md

## Decision

**Only the outbox is purged.** Retention deletes `Published` and `Superseded`
outbox rows older than a configured window, in bounded batches. `Failed` outbox
rows are retained by default and purged only on explicit opt-in. Received tables
and cache tables are never purged by kafkaman.

The mechanism mirrors the existing scheduler pairing rather than inventing a
shape:

- `purge_outbox_once(pool, table, &PurgeConfig) -> PurgeStats` performs **one
  bounded batch** and returns what it deleted. The caller loops. This is the same
  contract as `relay_once`, `dispatch_once`, and `ingest_once`.
- `run_purger(pool, table, cfg, shutdown)` loops until a batch deletes nothing,
  then sleeps — the same shape as `run` and `run_dispatcher`.

Retention duration is the operator's, expressed in `kafkaman.toml`. kafkaman owns
the mechanism and the chunking; it does not choose the window.

**Zero durations are rejected for `outbox_after`.** Each field in the new config
section states its own zero-semantics rather than inheriting a blanket rule.

## Rationale

The scope is inherited from the restore-and-retention proposal's three-way split by
reconstructibility, not argued fresh:

- The outbox carries the **drop** verb — already excluded from the backup set and
  rebuilt empty on recovery. Nothing is permitted to depend on a historical outbox
  row, so deleting one during normal operation is that same directive applied
  continuously. `Replay::outbox` is now rejected as unsafe, so no read path over old
  rows exists at all.
- The received table carries **protect** — "the only durable record of *what was
  done*, reconstructible from nothing." Purging it is unsafe for a second, sharper
  reason: the unique `idempotency_key` index is what makes a Kafka redelivery a
  no-op, so retention shorter than the redelivery window silently re-runs handlers.
  The dedupe window and the retention window are the same window.
- The cache table carries **rebuild** and *is* the state. Reclaiming soft-deleted
  rows is a different problem that gates on cache continuity, not on age.

`Failed` outbox rows are the invalid-send audit trail the error-row-symmetry
decision deliberately writes. Purging them by default would delete the record of
work that was rejected, which is the one thing in the outbox with no successor.

The batching is not a performance nicety. A single unbounded `DELETE` over a table
that has grown for months holds a long lock and generates WAL proportional to the
whole backlog; bounded batches keep each transaction short and the write-ahead log
flat.

`outbox_after = 0` is rejected because it deletes a row the instant it publishes,
destroying the operational record while an incident is still being diagnosed. This
is stated explicitly because a blanket "reject zero" rule was previously relaxed
across all durations at once and silently un-guarded a field that needed it —
recorded as R3.

## Consequences

- Adding the retention index to an existing deployment blocks writes for the
  duration of the build, because changesets apply inside a transaction and
  `CONCURRENTLY` is therefore unavailable. Retention and index-adding changes are
  ordered, not independent.
- An operator who wants no retention keeps the default: absent config means no
  purger runs. Growth remains the status quo rather than becoming a silent deletion.
- Received-table and quarantine growth remain unaddressed and are now explicitly
  out of scope rather than merely unconsidered.

## Evidence

- `retention_reclaims_terminal_rows_and_spares_everything_else` — `Pending`,
  `Publishing`, and `Failed` all survive a sweep that reclaims `Published` and
  `Superseded`.
- `retention_reclaims_failed_rows_only_on_opt_in`
- `retention_spares_rows_inside_the_window`
- `retention_batches_are_bounded_and_converge` — batches cap at `batch_size` and
  the sweep terminates.
- `retention_rejects_a_config_that_would_delete_live_rows` — validation is enforced
  at the call, so a direct caller cannot bypass the loop's check.
- `purge_config_rejects_settings_that_delete_live_rows_or_spin`
- `retention_section_is_optional_and_validated`
- `add_outbox_retention_index_upgrades_a_legacy_table`
- `retention_scan_is_index_served` (diagnostic) — the age scan uses the retention
  index: Index Scan, no sort, 0.47 ms over 200k rows. Checked rather than assumed,
  because R14 records assuming exactly this and being wrong.
