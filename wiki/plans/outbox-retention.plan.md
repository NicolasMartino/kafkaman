# Outbox Retention and the Claim-Order Index

- Document Class: Plan
- Status: Completed
- Date: 2026-08-24
- Category: Operational model
- Scope: Two changes proposed together after measuring R1 — an index to make
  `claim_batch`'s ordering index-served, and outbox retention. One shipped.
- Sources:
  - review.md
  - wiki/decisions/outbox-retention-policy.decision.md
  - wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md

## What prompted it

Measuring R1's fix surfaced two things neither review pass had found: the claim's
candidate ordering falls back to a sequential scan and an external merge sort under
a backlog (R14), and **nothing purges any kafkaman table**, so the outbox grows for
the life of the application.

Scoping the first turned up a third: changesets apply inside a transaction, so
`CREATE INDEX CONCURRENTLY` is unavailable and every index build blocks writes. On
an unbounded outbox that makes retention close to a prerequisite for adding any
index at all. Both invariants are now recorded in proposal 11.

## Stage 1 — claim-order index. Refuted, not shipped.

Hypothesis: a partial index `(created_at) WHERE status = 'Pending'` would let the
claim stream rows in order and stop at the limit.

The stage carried an explicit gate — **the pass condition was the disappearance of
the `Sort` node**, not a faster wall clock, because timing alone had already misled
one round of this work. Measured on both table shapes:

| Shape | Execution | Plan |
| --- | ---: | --- |
| backlog — 200k `Pending` | 512–642 ms | Seq Scan + external merge sort, 7,840 kB to disk |
| steady state — 200k `Published` + 200 `Pending` | 0.3 ms | Bitmap Index Scan + 35 kB quicksort |

The index changed nothing: the plan is byte-identical with and without it. The
candidate predicate is an `OR` across two statuses, so the planner must examine
every row to decide membership and no index on one branch can order the union.

Two corrections fell out, both now in `review.md`. R14 does **not** cost every
relay cycle — the steady-state shape is already sub-millisecond, so it bites only
when the relay is behind. And most of the backlog-shape time is JIT compilation
triggered by the inflated cost estimate, not the scan.

The gate held: nothing shipped, and the refuted index is kept in the benchmark as a
recorded negative result so it is not retried. A real fix means removing the `OR` —
a `UNION ALL` of two separately-ordered branches — which is a semantic change to
the correctness-critical claim and hits Postgres rejecting `FOR UPDATE` with
`UNION`. That earns its own change.

## Stage 2 — outbox retention. Shipped.

Scope was inherited from proposal 11's three-way split rather than argued fresh:
the outbox carries the **drop** verb, so nothing may depend on a historical row;
the received table carries **protect**; cache tables carry **rebuild**.

Delivered:

- `purge_outbox_once` (one bounded batch) and `run_purger` (loop), mirroring the
  `relay_once`/`run` and `dispatch_once`/`run_dispatcher` pairing.
- `PurgeConfig` with per-field zero rejection, `PurgeStats`, `RetentionSection`,
  and an optional `[retention]` config section.
- `create_outbox_retention_index_sql`, emitted by `CreateOutboxTable` for fresh
  tables and by `AddOutboxRetentionIndex` for existing ones.

Applying Stage 1's lesson, the retention index was **verified used, not assumed**:
`retention_scan_is_index_served` shows an Index Scan, no sort, 0.47 ms over 200k
rows.

## Evidence

- `retention_reclaims_terminal_rows_and_spares_everything_else`
- `retention_spares_rows_inside_the_window`
- `retention_batches_are_bounded_and_converge`
- `retention_reclaims_failed_rows_only_on_opt_in`
- `retention_rejects_a_config_that_would_delete_live_rows`
- `add_outbox_retention_index_upgrades_a_legacy_table`
- `purge_config_rejects_settings_that_delete_live_rows_or_spin`
- `retention_section_is_optional_and_validated`
- `claim_candidate_ordering_cannot_be_index_served` (diagnostic, `#[ignore]`d)
- `retention_scan_is_index_served` (diagnostic, `#[ignore]`d)

## Left open

- The `UNION ALL` restructure of `claim_batch`, the only thing that would actually
  fix R14.
- Received-table and quarantine growth, now explicitly out of scope rather than
  merely unconsidered.
