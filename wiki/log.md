# Wiki Log

## [2026-08-14] implementation | M5 outbox supersede first slice

Implemented Phase 3's outbound entity ordering foundation. Outbox rows now carry
`entity_key`; `OutboxStatus` includes `Superseded`; fresh outbox tables include
the nullable `entity_key` column plus an entity-state index; and
`AddOutboxEntityKey` upgrades existing outbox tables idempotently.

For compact message types with a valid idempotency identity, enqueue now takes a
transaction-scoped advisory lock keyed by schema, message type, and entity key
before updating same-entity pending rows to `Superseded` and inserting the new
row. Relay claiming also blocks a newer pending row while another row for the
same entity is still `Publishing`, preserving the one-in-flight ordering
contract that makes Kafka offsets usable as the convergence ordinal.

Verified gates:
- `first_concurrent_enqueues_for_entity_serialize`
- `supersede_collapses_queued_updates`
- `publishing_entity_blocks_newer_pending_claim_until_published`
- `add_outbox_entity_key_upgrades_legacy_outbox_table`

Verification:
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test entity_first_outbox_supersede -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test entity_first_propagation -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_send -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests -- --test-threads=1`
- `rtk cargo test --workspace --all-features`

Pages affected:
- wiki/plans/entity-first-propagation.plan.md
- wiki/compatibility/m5-entity-first-cache-api.compat.md
- wiki/compatibility/m5-entity-first-outbox-supersede.compat.md
- wiki/index.md
- wiki/log.md

Code affected:
- crates/kafkaman-core/src/lib.rs
- crates/kafkaman-sqlx/src/lib.rs
- tests/durable-send/tests/entity_first_outbox_supersede.rs

## [2026-08-13] implementation | M5 concurrent cache convergence gate

Added `concurrent_dispatch_of_two_states_converges_to_newer` to the entity-first
propagation integration tests. The gate starts one dispatcher on an older entity
row and holds its transaction open, then lets a second dispatcher claim the
newer row through `FOR UPDATE SKIP LOCKED`. The newer row updates the compact
cache first; when the older row finishes last, the offset guard prevents cache
regression.

Verification:
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test entity_first_propagation -- --test-threads=1`
- `rtk cargo test --workspace --all-features`

Pages affected:
- wiki/plans/entity-first-propagation.plan.md
- wiki/index.md
- wiki/log.md

Code affected:
- tests/durable-send/tests/entity_first_propagation.rs

## [2026-08-13] implementation | M5 cache-upsert first slice

Started M5 on branch `implementation/m5-entity-first-propagation`. Added the
defaulted entity/retention public surface: `RetentionClass`,
`KafkaMessage::entity_key`, `KafkaMessage::retention_class`, and
`MessageDescriptor.retention_class`. Existing message implementations remain
source-compatible through default methods and constructor defaults, but direct
`MessageDescriptor` struct literals must provide the new field.

Added `CacheTable`, `CreateCacheTable`, and compact-type cache table DDL. Receive
dispatch now applies a guarded cache upsert for `Compact` message types before
marking the row `Processed`; older retry/redrive rows still process but do not
regress cache state. The harness creates cache tables for compact received types.

Implemented and verified the first two M5 convergence gates:
`retry_after_newer_applied_does_not_regress_cache` and
`redrive_after_newer_applied_does_not_regress_cache`.

Verification:
- `rtk cargo check --workspace --all-features`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test entity_first_propagation -- --test-threads=1`
- `rtk cargo test --workspace --all-features`

Pages affected:
- wiki/plans/entity-first-propagation.plan.md
- wiki/compatibility/m5-entity-first-cache-api.compat.md
- wiki/index.md
- wiki/log.md

Code affected:
- crates/kafkaman-core/src/lib.rs
- crates/kafkaman-sqlx/src/lib.rs
- crates/kafkaman-test/src/lib.rs
- tests/durable-send/tests/entity_first_propagation.rs

## [2026-08-13] update | M3/M4 merged and proposal 12 accepted for M5

Merged `implementation/m3-durable-receive` into `main` as the prerequisite for
M5, then resolved the pre-M5 proposal 12 decision. Proposal 12 is now Accepted:
every type is an entity, `entity_key` is universal but defaults to `message_id`,
and each type declares a retention class (`compact` / `delete`). `compact`
drives compacted topic configuration, cache-table generation, bootstrap
eligibility, and a meaningful convergence guard; `delete` keeps existing M1-M4
work-item behavior with no cache table and no bootstrap offer.

The entity-first decision is amended accordingly: the implicit entity/non-entity
split is replaced by a declared retention class, without removing the durable
execution surface or reversing the messaging-scope decision. The roadmap now
marks M5 Active and no longer treats its shape as contested. The M5 plan now
includes retention-class declaration, `delete` defaults for compatibility,
`compact`-only cache generation, and boot-time topic validation when broker
metadata is available.

Pages affected:
- wiki/proposals/12-entity-only-message-model.proposal.md
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/proposals/09-entity-first-propagation.proposal.md
- wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md
- wiki/plans/entity-first-propagation.plan.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-08-13] update | outbound entity enqueue serialization tightened

Replaced the previous "lock latest outbox row" multi-writer mechanism with
key-level serialization on `(message_type, entity_key)`.

The latest-row lock is insufficient because first concurrent writes may have no
row to lock, and a writer that waited behind another writer can decide from
stale latest state unless it re-reads under the same key-level lock. The
accepted design now requires a transaction-scoped PostgreSQL advisory lock or a
dedicated entity enqueue lock/control row, followed by a re-read of latest
outbox state and the supersede-or-insert decision in the same transaction.

This serializes horizontally scaled instances sharing one database. It does not
coordinate two services or two databases writing the same entity type, which
remains forbidden by entity ownership. The M5 plan now carries explicit gates for
the no-existing-row race and the waiting-writer re-read race, and the roadmap's
M5 exit now states that outbound serialization is limited to the affected entity
key rather than absent. Proposal 12 also notes that direct-mode `compact` types
cannot rely on the outbox lock and must be rejected unless they provide
equivalent key-level outbound serialization.

Pages affected:
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/proposals/09-entity-first-propagation.proposal.md
- wiki/proposals/12-entity-only-message-model.proposal.md
- wiki/plans/entity-first-propagation.plan.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-08-13] update | entity-only reselected as declared retention class

External review of proposal 12, plus a counter-review, converged on changing the
selected option. Four claims were checked against the files; two held.

**The real defect was an incomplete option set.** Proposal 12 listed three
options, all of which kept work items served by kafkaman, so "entity-only" was
selected against a field containing no strict reading of it. Added option 4
(entity-only by exclusion) and option 5 (uniform entity model with a declared
retention class), and reselected 5.

**The decisive argument against the original selection.** Entity types and
work-item types need different topic retention under *every* option — that split
already exists under entity-first, where proposal 11 states "entity topics use
compaction alone." The earlier claim that entity-only *relocated* the duality
into broker configuration overstated it. What entity-only-by-redefinition
actually does is delete the type-level declaration that proposal 11's proposed
boot-time topic validation needs as its input, leaving the same two
configurations with misconfiguration made undetectable and both failure modes
silent. That is strictly worse than entity-first on this axis and better on none.

**Hard exclusion is recorded and costed, not selected.** It argues against the
evidence the messaging-scope decision rests on — RepForge dropped the command
transport and kept `mutation_jobs` — exports the branching into the adopter's
architecture, and strands most of M4, which is work-item machinery.

**The selected option is non-breaking**, by defaulting `entity_key` to
`message_id` and the retention class to `delete`. Existing M1-M4 types compile
and behave unchanged. This closed the migration-story and required-`entity_key`
open questions, and means the proposal promotes as an amendment rather than a
revision: nothing is removed, so the entity-first decision's point 1 still holds.

**Two conflicts surfaced in proposal 11.** Its `kafkaman_cache` "rebuild —
replay is authoritative" directive is compaction-conditional, not universal; and
the resync sweep it describes under business-data restore is what the
self-healing collapse depends on entirely, so its scope must cover non-terminal
work items.

Two review findings were declined as framework noise: a Proposed page has not
superseded an Accepted one, and Accepted pages must not be rewritten to match an
unaccepted proposal. The legitimate residue was backlinking, now added — proposal
09's open question 3, proposal 11's per-type restore bullet, the entity-first
decision's Revisit When, and the roadmap's M5 section all point forward to
proposal 12 as contested, with no Accepted content rewritten.

**Second review pass, same day — two corrections, both accepted.** The revision
above reintroduced the very failure it removed: a summary bullet claiming "one
table shape, one guard, one upsert" contradicted the proposal's own
retention-class table, which generates a cache table for `compact` types and none
for `delete` types. The "What collapses" section is now an explicit accounting —
three of six forks removed, one conditionally — and records that two survivors
(cache-table generation, bootstrap eligibility) are structural rather than
policy, so "the remainder is only configuration" is not available as a defense.

The second correction qualifies the restore claim. The resync sweep is generic
only for entity types, where kafkaman owns the cache table and can enumerate it;
for work-item types "non-terminal" is a predicate over the application's own
status machine, so the sweep becomes a per-type adopter hook. The unification is
therefore **policy-level, not mechanism-level** — the same exported-complexity
cost charged against option 4, at much smaller scale. Named in both proposals for
consistency; it does not change the selection.

Pages affected:
- wiki/proposals/12-entity-only-message-model.proposal.md
- wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md
- wiki/proposals/09-entity-first-propagation.proposal.md
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-08-13] ingest | multi-writer and erasure resolved; entity-only proposed

Three outcomes from the design review of the summary-of-decisions.

**Multi-writer is resolved and moves into the decision.** Enqueue takes a row
lock on the entity's most recent outbox row before checking send status, so a
concurrent writer blocks through the supersede-or-insert commit. This serializes
every instance of the owning service — the case that actually occurs, since
horizontal scaling shares one database. The previous entry claiming this had "no
proposed answer" was wrong: it conflated intra-database scaling with
cross-database writers. The latter remains unsafe and is now forbidden by entity
ownership rather than left open, and proposal 09's open question 6 is closed.

**GDPR erasure is resolved** without a topic rebuild and without real tombstones:
publish the entity with every field but the ID stripped and a deleted status, so
compaction reclaims the earlier PII-bearing records and every cache converges to
the redacted version. Two conditions recorded — the ID must be an opaque
surrogate, since it survives forever by design, and compaction timing becomes a
compliance parameter in direct tension with proposal 11's own suggestion to raise
`min.compaction.lag.ms` to retain recent history. Storage growth from unreclaimed
keys remains unresolved.

**Filed proposal 12, entity-only.** Every message type becomes an entity; work
items are entities with a key unique per item, where the convergence guard is a
harmless no-op, compaction collapses nothing meaningful, and execute-once stays
`idempotency_key`'s job. This removes per-type branching from restore policy,
audit, self-healing, and opt-out config.

Recorded against it rather than glossed: it reverses the messaging-scope
decision's refutation of propagation-only scope, it breaks the general
send/receive surface shipped in M1-M4, and — the finding that emerged while
drafting — it **relocates the duality rather than eliminating it**. Compaction
retains one record per key forever, entity key cardinality is bounded but work
item cardinality is not, so work-item topics need `cleanup.policy=delete`, which
is exactly what proposal 11's retention invariant forbids for entity topics. Two
broker configurations, chosen per type, each with a silent failure mode if
misapplied.

Also fixed index ordering: proposal 11's entry preceded proposal 10's.

Pages affected:
- wiki/proposals/12-entity-only-message-model.proposal.md (new)
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/proposals/09-entity-first-propagation.proposal.md
- wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md
- wiki/index.md
- wiki/log.md

## [2026-08-13] update | entity-first slotted as M5; milestones renumbered

Gave entity-first propagation a milestone slot. It had an Accepted decision, a
revised proposal and an execution plan but no place in the delivery sequence,
because the propagation model was decided after the roadmap was written.

Inserted as **M5, ahead of observability**, on the argument that it adds per-type
cache tables and a `Superseded` outbox status — building dashboards, metrics and
DLQ views on the current table layout would mean rebuilding them one milestone
later. Observability moved to M6 and V1 hardening to M7.

Renumbering rather than an out-of-order insert, because four pages outside the
roadmap referenced "M5" meaning observability and would otherwise have drifted:
the M2 plan's deferred admin routes, the M4 spec's "later milestones", the
roadmap-execution-policy decision's ratification note, and proposal 04's
promotion target. All four updated.

Also refreshed the roadmap's stale "Where We Are", which still claimed only M1
code existed, and recorded that the M1-M4 work sits unmerged on
`implementation/m3-durable-receive` with all five pre-merge findings closed —
making the merge the stated prerequisite for M5.

Cache bootstrap/readiness (proposal 10) and restore/retention/schema boundaries
(proposal 11) are explicitly deferred out of M5 and noted as separately tracked,
since proposal 11's schema split breaks M2's single-schema surface.

Pages affected:
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/plans/m2-change-engine-config.plan.md
- wiki/specs/m4-retry-backoff-dlq.spec.md
- wiki/decisions/v1-roadmap-execution-policy.decision.md
- wiki/proposals/04-observability-logging-policy.proposal.md
- wiki/plans/entity-first-propagation.plan.md
- wiki/index.md
- wiki/log.md

## [2026-08-13] update | replica renamed to cache

Renamed the entity-store concept from "replica" to "cache" across the wiki, on
the user's call. Two reasons: the project already calls itself "a distributed
cache library rather than a message library to build a cache on", and "replica"
collides badly with Postgres replication in a codebase whose restore policy,
PITR behavior, and physical replication are all under active discussion —
"restore the replica" was becoming ambiguous.

Schema named `kafkaman_cache` rather than `kafkaman_distributed_cache`: from any
single service the schema holds that service's local shard, so distribution is a
system property rather than a schema property.

Renamed `wiki/proposals/10-replica-bootstrap-and-readiness.proposal.md` to
`10-cache-bootstrap-and-readiness.proposal.md` via `git mv`, and updated the five
pages linking to it. `ReplicaTable` became `CacheTable`, `replica_state` became
`cache_state`, and proposal 10's typestate is now `Cache<Bootstrapping>` →
`Cache<Ready>`.

Deliberately **not** renamed: "replica" in the deployment sense — worker
replicas, concurrent replicas in a rolling deploy, application/replica boots —
which appears in the M1 spec and review, the library-test-strategy and
schema-and-change-management decisions, proposal 01, and proposal 08. Verb forms
(replicating, replicated, replication) were preserved everywhere. `raw/` was left
untouched per the immutable-provenance rule, and prior log entries were left
as-written per append-only.

Corrected an assertion made during the discussion: it is not true that the cache
has no origin to fall back to. The compacted topic *is* the origin — that is what
makes bootstrap-from-zero work at all. What it lacks is a per-key fallback, since
Kafka has no key-based random read, so refill is necessarily bulk replay. That
asymmetry is now recorded in proposal 10 as the reason readiness must be a
typestate rather than lazy-loading.

Also repaired stale content in proposal 10 exposed by the rename: its drift-repair
option still described the outbox-sequence versioning caveat and claimed republish
"is only safe for types using a domain version". Both are obsolete; it now carries
the state-sourced republish rule.

Pages affected:
- wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md (renamed from 10-replica-*)
- wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md
- wiki/proposals/09-entity-first-propagation.proposal.md
- wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/plans/entity-first-propagation.plan.md
- wiki/index.md
- wiki/log.md

## [2026-08-13] create | restore, retention, and schema boundaries proposal

Promoted the material the same-day ordinal ingest deliberately left in `raw/`
into proposal 11, on the user's call to file it separately rather than fold it
into the entity-first pages.

Records which of the 2026-08-12 discussion's invariants survived the offset
ordinal and which died with it: the immutable-version-source and
producer-state-restore invariants are gone, and the never-restore-the-outbox rule
survives with its rationale *replaced* — republished rows now win on offset
rather than colliding with a rewound sequence.

Departures from the source discussion, each argued in the proposal rather than
asserted: the schema cut is three-way by reconstructibility (drop / protect /
rebuild) rather than inbound/outbound, because the inbound/outbound cut groups
the irreplaceable received table with the fully rebuildable replica table; the
split is justified as backup-set composition and grants rather than independent
restore policy, because PITR and physical replication are cluster-wide; and
`enqueue_on_connection` commits received-row updates and outbox inserts in one
transaction, so restoring the two schemas to different points would tear
committed transactions apart.

Also carried forward the boundary that soft-delete-first makes GDPR erasure a
topic-rebuild operation, and left proposal 08's polling-cost question explicitly
unanswered and assigned.

Pages affected:
- wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md (new)
- wiki/index.md
- wiki/log.md

## [2026-08-13] ingest | offset as convergence ordinal

Ingested two design sources: the 2026-08-12 restore-policy/schema-separation
discussion and the 2026-08-13 conversation that reviewed it. The review of the
first source dismantled and rebuilt the entity-first version model, so the second
source supersedes parts of the first.

The change: **there is no producer-side entity version.** The convergence ordinal
is the Kafka offset, already persisted as `source_topic` / `source_partition` /
`source_offset` on every received row in shipped M3 code. It is trustworthy only
because of a new send-side rule — per-entity supersede of pending outbox rows —
which makes log order equal truth order and thereby answers proposal 09's
original reason for rejecting offsets.

Removed from the accepted design: the per-entity high-water table, the outbox
monotonicity constraint, the `kafkaman-entity-version` reserved header, the
per-type version-source declaration and its boot-time fail-fast, and the
domain-version override. Added: state-sourced republish (re-emitting a stored
outbox row is unsafe because it receives a new higher offset, which constrains
M2's `Replay::outbox` for entity types), and topic-lifecycle invalidation with
detection that must not key off observing offset 0.

Contradictions resolved with the user rather than silently overwritten: seven
claims in the Accepted entity-first decision, plus the newly surfaced send-side
replay hazard, which inverted an earlier claim made during the conversation that
replay is harmless under a version guard — true for a producer-stamped version,
false for an offset.

**Deliberately not promoted this pass** (user scoped it to the ordinal change):
the restore-policy and schema-separation material from the 2026-08-12 source —
inbound-ledger protection, layered idempotency, and the inbound/outbound schema
split. It remains staged in `raw/design/` as provenance and is referenced from
the new plan's Out of Scope, but has no proposal or decision page. Note that
offset-as-ordinal dissolved that source's invariant 4 and the version-rewind
rationale behind invariants 5 and 6; invariant 5 survives for a different reason
(republished rows win on offset), which is now recorded as state-sourced
republish.

Also corrected stale index bookkeeping: the M4 plan was listed Active while the
page itself reads Completed, and the stage line still said "M3/M4 pre-merge fixes
active". The M3 durable-receive plan is still marked Active while the roadmap
records M3 as Completed — left alone pending a lint pass rather than changed
here.

Two cross-page contradictions were also resolved. Proposal 07 required a foreign
tombstone's version to come from the `kafkaman-entity-version` header; its open
question 2 is now answered, because a Debezium tombstone has a Kafka offset by
virtue of arriving on the topic, so it orders against kafkaman-produced records
with no header and no per-type rule. Proposal 10 referenced proposal 09's
version-source check as a contrast case for compile-time enforcement; that check
no longer exists, and proposal 10 instead gains a second caller — forced
re-bootstrap on topic-lifecycle invalidation is the `Ready` → `Bootstrapping`
transition it already defines.

Pages affected:
- raw/design/2026-08-12-restore-policy-and-schema-separation-discussion.md (new)
- raw/design/2026-08-13-offset-as-convergence-ordinal-discussion.md (new)
- wiki/proposals/09-entity-first-propagation.proposal.md
- wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md
- wiki/proposals/10-replica-bootstrap-and-readiness.proposal.md
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/plans/entity-first-propagation.plan.md (new)
- wiki/index.md
- wiki/log.md

## [2026-08-12] ingest | error-row rule scope and ownership recorded

Promoted the reserved-header question from an open asymmetry to a decided
boundary, extending the typed-idempotency decision rather than filing a new page
since that decision already owns the error-row rule.

The rule now has an explicit scope: it applies to a send whose envelope is
structurally safe to persist. A send rejected *because its own content must not
enter the ledger* fails before any database work. That draws the line at
"would recording it write something the ledger must not contain" rather than at
severity, and it explains the reserved-header path as consistent with an
existing house pattern — invalid config and invalid retry settings already fail
before touching the database, with tests asserting exactly that — instead of as
an oversight in the error-row rule. Concretely, an audit row for a reserved
header would persist the offending `kafkaman-*` header into the row's `headers`
JSONB, so downstream tooling reading `headers` would observe a spoofed
`kafkaman-message-id`.

Also recorded that the send/receive difference follows from transaction
ownership: on receive kafkaman owns the transaction and can roll back to a
savepoint while persisting its failure record, whereas on send the caller owns
the transaction and the business write inside it. An earlier reading of this as
a design flaw was wrong.

Consequences added: reserved-header rejections leave no ledger trace, so that
failure mode is only visible in application logs; and a caller that commits
after an invalid send holds business state with no corresponding event, which is
a deliberate election of forensics over atomicity that the caller must
reconcile. Revisit triggers added for wanting a durable record of reserved-header
rejections (which would mean persisting the envelope with the header stripped
and the removal noted) and for making identity required at construction or via
typestate, which would remove the missing-identity error row entirely.

Pages affected: wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md,
wiki/index.md, wiki/log.md.

## [2026-08-12] implementation | tests for caller-controlled invalid-send audit

Reworked the invalid-send tests to demonstrate the property they claim. Both
existing tests exercised the commit and rollback branches but wrote no business
data, so neither proved the thing that makes the choice meaningful: that the
caller's own rows share the fate of the audit row. Both now write business data
inside the same transaction and assert it survives a commit and is discarded by
a rollback.

Added the recovery path that makes rollback safe rather than lossy. A
consume-then-produce handler whose enqueue is rejected has its business write
and the audit row discarded together by the handler savepoint, leaves the
receive row `Retryable` with its failure recorded and `processed_at` unset, and
the message is delivered again — completing on the next attempt once the handler
supplies an identity. The recorded failure is asserted to be the rejected
enqueue rather than an incidental error.

Clarified during discussion that the send/receive asymmetry follows from
transaction ownership, not inconsistency: on receive kafkaman owns the
transaction and can roll back to a savepoint while persisting its failure
record, whereas on send the caller owns the transaction and the business write
inside it, so kafkaman must not decide whether that write survives. An earlier
reading of this as a design flaw was wrong.

The reserved-header path is pinned by a test rather than left implicit: a
rejection returns before the insert, so it leaves the caller nothing to commit
even if they want the record, while a missing idempotency identity does. That
behaviour was kept and is now decided rather than open — see the follow-up entry
above.

Verified: durable_send 20 passed, durable_receive 27 passed, redpanda_full_loop
10 passed, workspace 85 passed across 23 suites, clippy clean.

Pages affected: wiki/compatibility/typed-idempotency-identity-api.compat.md,
wiki/log.md.

## [2026-08-12] implementation | typed idempotency fix verified against Docker

Ran the Docker-backed gates that the typed idempotency fix plan had never
executed. The earlier `CreateContainer(RequestTimeoutError)` was a cold Docker
daemon, not a defect. First execution produced seven `durable_receive` failures
in three classes: a latent serialization defect exposed by the F3/F4 fix, two
tests querying `idempotency_key` with pre-digest plaintext, and a
diagnosability regression where a missing row was reported by its digest rather
than the caller's key.

The load-bearing finding is the first. `ReceivedError::occurred_at` was a bare
`OffsetDateTime`, so `time`'s default serde wrote a component array that
`(errors -> -1 ->> 'occurred_at')::timestamptz` cannot parse. Nothing had ever
cast that field, so the wrong format was invisible until the fix reached for it.
The deeper problem was the cast itself: deriving triage state from audit JSON
couples DLQ queries to a serialization format. Failure time and kind were
promoted to `last_failed_at` and `last_failure_kind` columns, which decouples
them, makes triage indexable, and frees the audit records to carry annotated
RFC 9557 timestamps that PostgreSQL cannot cast at all.

Confirmed empirically against PostgreSQL 16 that `timestamptz` rejects both
`...Z[UTC]` and `...+01:00[Europe/London]`, and that RFC 9557 is a strict
superset of RFC 3339, so an unannotated timestamp is valid under both. Stored
failure records were reshaped as RFC 9457 problem details (`type` as a stable
`urn:kafkaman:problem:*` URI, `title`, `detail`, `occurred_at` as an extension
member; `status` omitted as HTTP-specific), with read-side aliases so
pre-problem-detail rows stay readable and an unknown `type` degrades to the
default kind rather than failing the read.

Two further defects surfaced while fixing these. DLQ inspection and bounded
redrive ordered by failure time with a random `message_id` tiebreak, so a
`max_rows` redrive could select an unpredictable subset of rows failed by the
same dispatch pass; ordering is now `last_failed_at, created_at, message_id`.
And `apps/axum-outbox` enqueued without an idempotency identity, so the
reference example had silently stopped working under the F1 contract — its
error type renders as a 200 with a text body, which is why its test asserted a
successful status and then failed parsing JSON.

Verified: `durable_send` 19 passed, `durable_receive` 26 passed,
`redpanda_full_loop` 10 passed, workspace 83 passed across 23 suites, clippy
clean. All five pre-merge review findings are closed and the branch now meets
its own merge gate.

Pages affected: wiki/plans/typed-idempotency-identity-error-row-fix.plan.md
(Active to Completed), wiki/compatibility/typed-idempotency-identity-api.compat.md
(Draft to Active, expanded), wiki/reviews/m3-m4-pre-merge-branch-review.reference.md
(resolution section), wiki/index.md, wiki/log.md.

## [2026-08-12] ingest | entity-first propagation design discussion

Ingested the 2026-08-12 design discussion that settled kafkaman's positioning as
a reference propagation system and the identity model that positioning requires.
Central finding: dedup is not convergence. The existing `idempotency_key`
answers "have I executed this work item", while a replica needs "is this newer
than what I hold", and diff-and-upsert cannot close the gap because kafkaman's
own retry backoff, `Replay::received` redrive, redelivery, and bootstrap replay
all reorder application by design. Fixed a required `entity_version` sourced
from the outbox sequence by default and overridable by a domain version, since
direct transport mode has no outbox and republish-based drift repair corrupts
the replica under sequence versioning. Adopted entity-only messages across the
existing inbox plus a new guarded per-entity replica table, an outbox
monotonicity constraint as source-side defense-in-depth, and advisory
`#[non_exhaustive]` origin intent with a mandatory catch-all wire variant —
without which a single new variant can trip the ingest circuit breaker
topic-wide. Reversed proposal 07's emission choice to soft-delete-first, because
real tombstones are reclaimed after `delete.retention.ms` and a long-offline
replica misses the delete; proposal 07 keeps real-tombstone ingestion for
foreign producers. Filed the bootstrap/backfill gap left open by the previous
ingest as proposal 10. Implementation planning deliberately deferred until
M3/M4 merges and the typed-idempotency fix plan lands.
Contradiction resolved: proposal 07's selected option (per-type real tombstones
on send and receive) conflicted with soft-delete-first; the user resolved it in
favor of soft delete during the discussion, and proposal 07 was revised in place
with the reversal recorded rather than silently rewritten.
Pages affected: `raw/design/2026-08-12-entity-first-propagation-discussion.md`,
`wiki/proposals/09-entity-first-propagation.proposal.md`,
`wiki/proposals/10-replica-bootstrap-and-readiness.proposal.md`,
`wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md`,
`wiki/decisions/entity-first-propagation-model.decision.md`,
`wiki/index.md`, `wiki/log.md`.

## [2026-08-12] ingest | prior-art review of cqrs-fullstack messaging

Reviewed the `cqrs-fullstack` messaging implementation in the workout2 project
snapshot as external prior art for kafkaman's durable send/receive model. The
snapshot implements the same pattern by hand: a transactional `outbox_events`
table with claim-by-`SKIP LOCKED`, stuck-row reclaim, attempt counters, and a
retention purge; and per-topic inbox tables where the consumer only enqueues and
commits offsets before a separate dispatcher executes domain work with retry and
dead-letter state. Two capability gaps were identified and filed as proposals:
Kafka tombstones are unrepresentable in kafkaman and currently quarantine as
`MissingPayload`, and both schedulers poll on a fixed interval with no
notification-driven wakeup. A third gap, bootstrap/backfill of a new reference
replica, was identified but not yet filed pending a positioning decision on
kafkaman as a reference builder.
Pages affected: `wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md`,
`wiki/proposals/08-listen-notify-scheduler-wakeup.proposal.md`,
`wiki/index.md`, `wiki/log.md`.

## [2026-08-12] implement | typed idempotency identity fixes

Implemented typed SHA-256 idempotency identity with retained source JSON,
transactional invalid-send outbox audit rows, digest Kafka header parsing,
latest-failure-time DLQ/redrive filtering, and received-only replay checksum
cleanup. Added regression tests and a draft compatibility note. Verification
passed for non-Docker compile/tests and clippy; Docker-backed integration
execution is pending because testcontainer creation timed out with
`CreateContainer(RequestTimeoutError)`.

Pages affected:
- `wiki/compatibility/typed-idempotency-identity-api.compat.md`
- `wiki/plans/typed-idempotency-identity-error-row-fix.plan.md`
- `wiki/index.md`
- `wiki/log.md`

## [2026-08-12] create | typed idempotency identity and error-row symmetry

Accepted the typed idempotency identity proposal and recorded the durable
decision that kafkaman idempotency is a SHA-256 digest plus caller-provided JSON
source material. Added an active implementation plan to close the M3/M4
pre-merge review findings and to apply the send/receive rule: record invalid or
problem work transactionally, return an error, allow caller rollback, and have
workers claim only non-error rows.

Pages affected:
- `wiki/proposals/06-typed-idempotency-identity-and-error-row-symmetry.proposal.md`
- `wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md`
- `wiki/plans/typed-idempotency-identity-error-row-fix.plan.md`
- `wiki/index.md`
- `wiki/log.md`

## [2026-06-23] review | M3/M4 pre-merge branch

Recorded an adversarial pre-merge review of branch
`implementation/m3-durable-receive` against local `main`. Findings: send-side
idempotency remains optional while receive-side ingest requires
`kafkaman-idempotency-key`; empty idempotency keys are accepted and collapse
unrelated receive rows; DLQ inspect/redrive "failure time" semantics use business
or row time instead of latest failure time; received-only replay knobs can alter
outbox replay checksums without changing outbox SQL. Verification recorded:
workspace all-features compile/tests and clippy passed; one durable-send package
`PoolTimedOut` flake passed when rerun in isolation.

Pages affected:
- `wiki/reviews/m3-m4-pre-merge-branch-review.reference.md`
- `wiki/index.md`
- `wiki/log.md`

## [2026-06-22] promote | M4 retry/backoff/DLQ spec

Completed M4 Phase 3/4 and promoted validated behavior into an Active spec
`specs/m4-retry-backoff-dlq.spec.md`. Added `ReceivedFailureFilter` (since +
kind) narrowing on the DLQ inspect surface, `Replay::failure_kind` to scope
redrive by failure kind, and `Replay::clear_history` for clean-slate redrive
(default redrive preserves attempts and error history). Phase 4 added no Redpanda
coverage by design — retry/backoff/DLQ is Postgres-side. Updated the M4 plan to
Completed, the roadmap M4 status, the index, and the runtime-API compat note.

## [2026-06-22] add | M4 Phase 3 DLQ inspect surface

Added `received_failed_rows` and `received_failed_count` to `kafkaman-sqlx` so
operators can inspect the terminal `Failed` (DLQ) backlog before redriving with
`Replay::received`. The list is oldest-first, `limit`-bounded, and preserves
attempts and error history. Recorded under the M4 plan Phase 3 progress. Test:
`received_failed_rows_inspect_surface_lists_terminal_dlq_rows`.

## [2026-06-22] fix | Replay::received redrive targets terminal Failed rows

The M4 retry-backoff slice scheduled a `next_attempt_at` on every `Retryable`
failure, which made the prior `Retryable` + `next_attempt_at IS NULL` parked
shape unreachable and stranded the `Replay::received` operational surface.
Repointed redrive at exhausted terminal `Failed` rows, preserving attempts and
error history. Reconciled the M3 spec replay paragraph and limitation note, and
recorded the change under M4 plan Phase 3. Test renamed to
`replay_received_redrives_failed_rows_without_replaying_processed_rows`.

## [2026-06-22] promote | M3 durable receive spec

Promoted validated M3 durable receive behavior into an Active spec. The spec
captures Kafka ingest ordering, durable quarantine, consecutive-skip breaker,
idempotency-key dedup, message-id conflict quarantine, receive dispatch,
failure classification, replay semantics, production loops, and atomic
consume-then-produce evidence. Updated the project stage and marked the M3
completion plan completed while leaving remaining chaos/model cases in the
deep-durability hardening backlog.

Pages affected:
- wiki/specs/m3-durable-receive.spec.md
- wiki/index.md
- wiki/log.md
- wiki/plans/m3-durable-completion.plan.md

## [2026-06-22] update | M3 Phase 4 consume-then-produce J3

Extended the consume-then-produce atomicity test through J3. The existing
handler surface (`&mut PgConnection`, `ReceivedMeta`, and `enqueue_on_connection`)
now proves success commits business row plus follow-up outbox, failure rolls
both back, and duplicate input redelivery deduplicates without a second business
effect or outbox row. Reconciled and accepted the central
`message-consumption-and-handler-model` decision for the shipped M3 surface.

Proof:
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive handler_enqueues_outbox_atomically_with_receive_transaction`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop full_loop_consume_then_produce_deduplicates_duplicate_input -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests`
- `rtk cargo test --workspace --all-features`
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

Pages affected:
- wiki/decisions/message-consumption-and-handler-model.decision.md
- wiki/index.md
- wiki/log.md
- wiki/plans/m3-durable-completion.plan.md
- wiki/proposals/05-deep-durability-testing.proposal.md

## [2026-06-22] update | M3 A1 real two-worker dispatch interleaving

Added `kafkaman-sqlx` `test-hooks` support for pausing after a failed handler
transaction rolls back and before failure accounting is recorded. Added the
real two-worker A1 regression: worker A fails and pauses before stale failure
accounting, worker B processes the same durable row through normal
`dispatch_once`, and worker A resumes without clobbering the processed row or
over-reporting failure stats.

Proof:
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests`
- `rtk cargo test --workspace --all-features`
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

Pages affected:
- wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
- wiki/index.md
- wiki/log.md
- wiki/plans/m3-durable-completion.plan.md
- wiki/proposals/05-deep-durability-testing.proposal.md

## [2026-06-22] update | M3 G1/G2 ingest uncertainty gates

Added `kafkaman-rdkafka` `test-hooks` support for injecting a failure after
the durable receive write commits and before Kafka offset commit. Added
Redpanda tests for G1 crash-window redelivery and G2 runner-level offset commit
uncertainty; both prove same-group redelivery deduplicates to the existing
received row and then commits the broker offset.

Proof:
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests`
- `rtk cargo test --workspace --all-features`
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

Pages affected:
- wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
- wiki/index.md
- wiki/log.md
- wiki/plans/m3-durable-completion.plan.md
- wiki/proposals/05-deep-durability-testing.proposal.md

## [2026-06-22] update | M3 production ingest runner

Added the production rdkafka ingest runner slice. The compatibility note now
records `IngestLoopStats` and `RdkafkaConsumer::run_ingester`, and the M3
completion plan notes the cancellable loop, transient retry backoff, and loud
poison-breaker stop semantics.

Proof:
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop -- --test-threads=1`
- `rtk cargo test --workspace --all-features`
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

Pages affected:
- wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
- wiki/index.md
- wiki/log.md
- wiki/plans/m3-durable-completion.plan.md

## [2026-06-22] update | M3 durable completion implementation slice

Implemented and documented the M3 durable completion slice covering Phase 1A
dispatch hardening, Phase 2 receive replay, Phase 3 receive ingest/dispatcher
loop, and the Phase 4 minimal consume-then-produce handler surface. Added
accepted decisions for MissingHandler policy, post-handler infrastructure error
classification, dispatch stats semantics, Kafka ingest identity/ordering, and
receive handler surface scope. Proof commands recorded in the plan progress:
`cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets
--all-features -- -D warnings`; `cargo test --workspace --all-features`;
`cargo test --manifest-path tests/durable-send/Cargo.toml --tests`;
`cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda
--test redpanda_full_loop`.

Pages affected:
- wiki/plans/m3-durable-completion.plan.md
- wiki/index.md
- wiki/proposals/05-deep-durability-testing.proposal.md
- wiki/decisions/missing-handler-dispatch-policy.decision.md
- wiki/decisions/dispatch-infrastructure-error-classification.decision.md
- wiki/decisions/dispatch-stats-semantics.decision.md
- wiki/decisions/kafka-ingest-identity-and-ordering.decision.md
- wiki/decisions/receive-handler-surface-scope.decision.md

## [2026-06-22] update | M3 durable completion plan review fixes

Applied a plan review (all five findings verified valid). Phase 1 split into
Phase 1A blockers (C1 head-of-line, C3 poisoned-tx, M3-stats, A1 real-worker) that
gate Phase 3 and Phase 1B invariant pins (B2, D1-D3, E2, I1, K4, K5) that may
defer. Scoped Phase 3's full-loop gate to receive-only effective-once and added a
Phase 4 consume-then-produce full-loop gate so a green Phase 3 cannot read as
end-to-end. Added Phase 2 and Phase 4 specific verification gates (injected-clock
due-boundary; Replay dry-run/apply/audit/guardrail/idempotence). Added a closure
step to accept/reconcile the still-Draft `message-consumption-and-handler-model`
decision before spec promotion. Bumped the index Updated date to 2026-06-22.

Pages affected:
- wiki/plans/m3-durable-completion.plan.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] create | M3 durable completion plan

Created a dedicated completion plan sequencing the remaining M3 work after the
first slice and its review fixes. Adopts a tests-lead strategy grounded in the
deep-durability-testing proposal: Phase 0 scaffolding (interleaving primitive +
P1 oracle fix), Phase 1 hardening of the `dispatch_once` seam against the
gap-revealing catalog (A1 real workers, C3 poisoned-tx, C1 head-of-line block,
B2 crash window, M3 stats, D/E/I/K pins) with each forced choice recorded as its
own decision doc, Phase 2 operational replay + injected clock, Phase 3 Kafka
ingest + dispatcher loop with full-loop Redpanda coverage, Phase 4 decision-gated
handler surface for consume-then-produce, and Phase 5 chaos/model + spec
promotion. Rationale: the proposal predicts real defects in current `dispatch_once`
(C1/C3) and forces ingest-shaping decisions (I2/G3/K5), so the seam is hardened
and its contracts decided before an engine wraps it.

Pages affected:
- wiki/plans/m3-durable-completion.plan.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] update | M3 review M1/L3 follow-up resolved

Landed handler metadata access (M1) and the L3 nullability note, closing the last
two M3 implementation-review findings. Added `ReceivedMeta` to `kafkaman-core`,
rethreaded `MessageRouter::handler`/`dispatch_once`/the erased handler trait to
pass `Fn(&mut PgConnection, ReceivedMeta, P)`, documented the nullable
`correlation_id`/`causation_id` columns inline, and added the
`dispatch_exposes_message_metadata_to_handler` gate (durable_receive now 11
tests). Marked the review Resolved and recorded the slice in the M3 plan Progress;
only out-of-scope M3 surface area (`FromMessage`/`Rx`/Tower, `Replay::received`,
injected clock, macros, Kafka ingest, full-loop) remains.

Pages affected:
- wiki/reviews/m3-durable-receive-implementation-review.reference.md
- wiki/plans/m3-durable-receive.plan.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] update | Deep durability testing second review verification

Verified the second follow-up review against the proposal, M3 review, dispatch
SQL/control flow, handler API, received insert path, and receive tests. Updated
the proposal to remove the unsupported "incorrect review claim" framing, narrow
B1's landed status to panic-only rollback, align A2/A3 matrix status with their
landed sequential tests, and add verified gaps for stale-failure `DispatchStats`
over-reporting, consume-then-produce API support, hard-coded received
`message_version = 1`, and the load-bearing success/failure status-guard
asymmetry.

Pages updated:
- wiki/proposals/05-deep-durability-testing.proposal.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] update | Deep durability testing review verification

Double-checked the follow-up review against `dispatch_once`,
`record_received_failure`, received-table DDL, core receive statuses, Harness
registration paths, and receive integration tests. Updated the proposal to
correct B2's attempt-accounting mechanism, strengthen C1's head-of-line
description, add the poisoned-transaction success-branch gap, track parked
retryables as already test-pinned, add reserved status variant drift coverage,
and add timestamp precision test-oracle hardening. Confirmed that parked
retryables are already covered by
`dispatch_failure_rolls_back_effect_and_parks_retryable`.

Pages updated:
- wiki/proposals/05-deep-durability-testing.proposal.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] update | Deep durability testing proposal review

Expanded the deep durability testing proposal from the original M3 receive-focused
catalog into a reviewed durability-test roadmap. Marked landed receive
regressions, added rare-failure classes for ambiguous commits, cancellation,
Kafka offset uncertainty, idempotency/source-offset collisions,
consume-then-produce atomicity, schema/config drift, observability safety,
send-side mirrors, and model/chaos testing, and refreshed priority order.

Pages updated:
- wiki/proposals/05-deep-durability-testing.proposal.md
- wiki/index.md

## [2026-06-21] create | Direct mode and observability policy proposals

Captured two proposed design directions from user discussion: an explicit non-durable direct Kafka producer/consumer mode for high-throughput or low-durability workloads, and a configurable `tracing`-based observability/logging policy with per-message-type overrides and payload-safety controls.

Pages created:
- wiki/proposals/03-direct-transport-mode.proposal.md
- wiki/proposals/04-observability-logging-policy.proposal.md

Pages updated:
- wiki/index.md

## [2026-06-21] fix | M2 change-engine + config review findings resolved

Implemented the fixes from the M2 implementation review. Code:
- H1: Axum example resolves config before opening a pool or touching the schema.
- H2: `ResolvedConfig::from_config` validates a present `[retry]` section on the
  boot path (new `Config::contains`); boot-path gate test added.
- H3: checksums are now SHA-256 hex (`sha256:<hex>`, new `sha2` dep), replacing
  the interim `fnv1a64:` format.
- H4: `migrate_dry_run` runs inside a rolled-back transaction, so it persists no
  bootstrap DDL, `applied_by` backfill, or changelog rows; gate test added.
- M1: `Replay` resets `attempts`/`last_error` on requeued rows.
- M2: `kafkaman.example.toml` labels `database.url`/`kafka.brokers` as host-owned.
- M3: dry-run uses `pg_try_advisory_lock`, returns new `Error::MigrationLockBusy`.
- M4: added boot-path retry and dry-run-legacy-history gate tests.
- L1/L2/L4/L5: explicit nullable checksum decode, `try_changelog!`, zero-duration
  rejection, `errors_limit` is `u32`. L3 (bind-carrying statements) deferred.

Verification: fmt/clippy/check clean; 13 unit + 17 durable-send + 1 axum-http
tests pass. Pages affected: wiki/specs/m2-change-engine-config.spec.md,
wiki/compatibility/m2-change-engine-config-schema-and-api.compat.md,
wiki/reviews/m2-change-engine-config-implementation-review.reference.md,
wiki/log.md.

## [2026-06-21] promote | M2 change-engine/config implementation validated

Implemented M2 change-engine and configuration slice: dedicated `kafkaman-config`
crate, config-loaded `ResolvedConfig`, migration reports, nullable checksum and
`applied_by` changelog audit columns, checksum drift enforcement,
`changelog!`, dry-run preview, and guarded send-side `Replay`.
Pages affected: wiki/specs/m2-change-engine-config.spec.md,
wiki/compatibility/m2-change-engine-config-schema-and-api.compat.md,
wiki/roadmaps/path-to-v1.roadmap.md, wiki/plans/m2-change-engine-config.plan.md,
wiki/index.md.

## [2026-06-21] create | V1 remaining decision pass

Recorded user-selected options for the remaining V1 planning decisions:
required idempotency keys for all persisted V1 messages, rejected
user-supplied `kafkaman-*` headers, per-message retry/backoff/DLQ runtime
configuration in `kafkaman.toml`, table-backed V1 DLQ, split sub-decision vs
milestone validation status, and dependency-aware parallel worktrees.

Pages created:
- wiki/decisions/message-identity-and-header-namespace.decision.md
- wiki/decisions/retry-backoff-dlq-policy.decision.md
- wiki/decisions/v1-roadmap-execution-policy.decision.md

Pages affected:
- wiki/decisions/message-consumption-and-handler-model.decision.md
- wiki/decisions/configuration-and-environment-model.decision.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-06-20] promote | M1 durable-send implementation validated

Implemented the M1 durable-send slice on branch `implementation/m1-durable-send`.
The workspace now includes core types, SQLx migration/outbox primitives, claim
lease relay worker, rdkafka publisher, Docker-free Harness seed, testcontainers
Postgres integration gates, and a runnable Axum outbox example. Promoted the
validated behavior into an active spec and marked the two M1 plans completed.

Evidence:
- `cargo test --workspace` passed with 10 tests across 16 suites in 7.34s.
- Integration tests start Postgres through `testcontainers`.

Pages affected:
- specs/m1-durable-send.spec.md (created, Active)
- plans/first-poc-outbox-publisher.plan.md (Status: Completed)
- plans/m1-durable-send-implementation.plan.md (Status: Completed)
- roadmaps/path-to-v1.roadmap.md (M1 Status: Completed)
- index.md (catalog refreshed)

## [2026-06-20] update | M1 plan review fixes

Corrected the M1 implementation plan after review. The relay model now uses a
real claim lease/token flow instead of an impossible single transaction around
Kafka publish; `Publishing` rows are reclaimable after lease expiry. The plan now
uses object-safe changesets, typed message descriptors, validated SQL identifiers,
stale-claim-safe `mark_published`, `mark_publish_failed`, and a Docker-free
capturing-publisher Harness default with opt-in Redpanda/full-loop support.
Rewrote the first-PoC plan to match those mechanics and strengthened verification
gates so delivery claims require both a published/captured record and the expected
outbox row state.

Pages affected:
- plans/m1-durable-send-implementation.plan.md
- plans/first-poc-outbox-publisher.plan.md
- decisions/schema-and-change-management.decision.md

## [2026-06-20] lint | design review fixes (8 findings) + M1 implementation plan

Actioned an external design review of the four Draft decisions, the roadmap, and
the PoC plan. Fixes:
- **Receive state machine (#2):** pinned the dispatch claim predicate
  (`status IN (Pending, Retryable) AND next_attempt_at <= now()`) and the status
  set (`Pending→Processing→Processed`, `→Retryable→`, `→Failed`) in the
  message-consumption decision; only the retry *policy* stays deferred.
- **Effective-once scope (#1):** message-consumption point 6 now states the
  guarantee covers only writes through `Rx`; external side effects are
  at-least-once and need their own idempotency/outbox. Handler example comment
  corrected.
- **`send_now` footgun (#4):** runtime decision renames it
  `send_non_transactional`, quarantines it off the default `Sender` surface,
  requires a counter/span.
- **Subsystem default (#5):** runtime decision default is now the smallest
  non-destructive set (relay + consumers, never `Purge`); explicit selection.
- **Daemon vocabulary (#6):** reconciled "no standalone daemon" across the runtime
  decision and the proposal's `kafkaman-worker` note ("worker-role host binary").
- **"Full" vs bounded history (#7):** message-consumption `errors` reworded to
  "recent failure history" (bounded ring, explicitly not an audit log).
- **raw/ immutability (#8):** marked the design-discussion source as append-only
  provenance, distinct from compiled `wiki/` knowledge.
- **Roadmap (#3 + TDD):** dropped the blanket "flip four decisions to Accepted at
  M1"; now "Accepted design direction + revisit gate at each milestone's exit",
  only the send-side of runtime ratifiable at M1. Added an Outside-In TDD working
  method (the failing `Harness` test drives the API).
- Recorded the **dedup-identity open question** (idempotency key vs message_id vs
  offset) in the message-consumption decision, with a recommended default to
  ratify before M3.

Page created:
- plans/m1-durable-send-implementation.plan.md — code-level M1 plan (crate layout,
  deps, types/signatures, migrate engine + outbox DDL, enqueue/claim/mark, relay,
  Harness seed, ordered outside-in-TDD tasks → PoC gates).

## [2026-06-20] create | project bootstrap

Initialized `kafkaman` with the LLM Wiki framework.

## [2026-06-20] ingest | kafkaman objectives research bundle

Ingested `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/`
(research-summary.md, manifest.md, sources 02-12). The 01-cqrs-fullstack copy is
on disk but gitignored; its distilled form (research-summary.md §2) was used.

Pages created:
- proposals/01-kafkaman-objectives.proposal.md
- proposals/02-messaging-scope-kafka-command-vs-durable-execution.proposal.md
- references/rust-kafka-outbox-ecosystem.reference.md
- plans/first-poc-outbox-publisher.plan.md
- index.md updated.

Contradictions: none in the wiki (it was empty). One real architecture tension
recorded as proposal 02 (host architecture uses Kafka for propagation + internal
HTTP for commands, vs kafkaman's original Kafka-instead-of-REST idea) — filed as
a candidate awaiting ratification, not an accepted decision.

Gaps: outbox-pattern-processor docs.rs/crates.io version recency disagreed
(0.3.6 vs 0.4.0). Five v1-scope open questions remain in proposal 01.

## [2026-06-20] promote | messaging scope ratified to a decision

Ratified proposal 02 as a decision after reviewing the cqrs-fullstack migration
history (commands-over-Kafka was built and dropped in migration 010; replaced by
the durable HTTP `mutation_jobs` engine). Choice: durable-execution-first core,
Kafka-only transport in v1, HTTP/commands/synchronous outcomes deferred, receive
is fire-and-forget + durable status with `correlation_id`/`causation_id` in the
envelope and wait-for-outcome reserved as future primitives.

Pages affected:
- decisions/messaging-scope-and-receive-model.decision.md (created, Accepted)
- proposals/02-messaging-scope-... (Status → Accepted, promoted)
- proposals/01-kafkaman-objectives (open questions 2 and 4 resolved)
- index.md (stage → "Scope decided"; Decisions section populated)

## [2026-06-20] lint | external review fixes (provenance + delivery semantics)

Acted on an external review of the bootstrap state. Fixes:
- Provenance: the decision cited the gitignored cqrs-fullstack copy. Added a
  committed verbatim excerpt
  (sources/01-cqrs-fullstack-migration-evidence.md) and repointed the decision's
  sources to it; full project remains local-only.
- Delivery semantics: PoC plan claimed "exactly-once-effectively". Corrected to
  durable at-least-once (publish-then-mark duplicate window), added a
  crash-after-ack-before-mark verification test, noted effective-once belongs to
  the (out-of-scope) consumer idempotency layer.
- Provenance hygiene: expanded abbreviated `raw/research/...` Source paths to
  full resolvable paths in the ecosystem reference.
- Config hygiene: gitignored `.llm_wiki/runtime.toml` (per-machine install state,
  absolute paths); set Serena `languages: [rust]`; trimmed AGENTS.md EOF blank
  line; staged the previously-untracked decision so the index is commit-consistent.

Pages affected:
- raw/.../sources/01-cqrs-fullstack-migration-evidence.md (created)
- decisions/messaging-scope-and-receive-model.decision.md
- plans/first-poc-outbox-publisher.plan.md
- references/rust-kafka-outbox-ecosystem.reference.md

## [2026-06-20] update | execution model + reference stance recorded

From design discussion: recorded the agreed **durable message runtime** model
(durable table → scheduler → annotated handler; receive-tx owned by kafkaman and
handed to the handler; send offers transactional + fire-and-forget enqueue;
runtime `migrate()`), and the guiding stance that the cqrs-fullstack reference is
a discussion starting point, not a blueprint. Physical table layout (per-type vs
partitioning) and purge/retention remain under active discussion, flagged as
open in the proposal.

Pages affected:
- proposals/01-kafkaman-objectives.proposal.md (Execution Model section + reference stance)

## [2026-06-20] create | schema & change-management decision

Captured the design discussion outcome on persistence and operations so the
reasoning is not lost. Decision: dedicated `kafkaman` schema; distinct per-type
tables from one template; per-table UNIQUE for idempotency; DELETE-based purge
(partitioning deferred); a Rust Flyway-style change engine with versioned
changesets (structural + operational unified) tracked in
`kafkaman.changelog_history`; `migrate()` = CI/CD convergence, `Runtime::start()`
= subsystems; retention declared by changeset, enforced at runtime; no SQL
functions; plain tables for break-glass. Alternatives (single generic table,
partitioning, SQL functions, extending host sqlx migrations) recorded with
rejection rationale.

Pages affected:
- decisions/schema-and-change-management.decision.md (created, Accepted)
- proposals/01-kafkaman-objectives.proposal.md (schema/change-management bullet;
  table-layout + purge open items resolved)
- index.md (Decisions section)

## [2026-06-20] update | changeset versioning + placement settled

Settled two changeset details: sequential integer versions (not timestamps —
merge collisions are a deliberate reconciliation forcing function), and the
changelog lives in its own module out of `main` (one changeset per file;
directory-derive macro deferred).

Pages affected:
- decisions/schema-and-change-management.decision.md (point 10 added)

## [2026-06-20] update | realigned the first-PoC plan to the decisions

Rewrote the PoC plan to match the accepted decisions: per-type outbox table in
the `kafkaman` schema provisioned by a **minimal** `kafkaman::migrate()`
(structural changesets only — operational changesets, checksums, audit, and the
`changelog!` macro explicitly deferred); envelope carries
`correlation_id`/`causation_id`; example shows the changelog in its own module
and the two-phase `main`. Delivery semantics stay durable at-least-once. Now
validates the two most expensive commitments (per-type layout + `migrate()`
entry point), not just a generic outbox.

Pages affected:
- plans/first-poc-outbox-publisher.plan.md (rewritten)
- index.md (plan summary)

## [2026-06-20] lint | external review fixes (round 2)

Acted on a second external review. Fixes:
- Provenance: created raw/design/2026-06-20-kafkaman-architecture-discussion.md
  (curated design-discussion note) and repointed both decisions' dangling
  "Design discussion" source to it.
- messaging-scope decision: "without a schema migration" → "without changing the
  existing envelope fields" (the waiter store still adds tables).
- PoC plan: softened the per-type claim (validates the template + plumbing, not
  the operational advantages) and added a second `CreateMessageTable` + a
  template-generalizes gate at near-zero cost.
- schema decision: added an "Open Refinement: Guardrails for Operational
  Changesets" section (env targeting, dry-run, auto-vs-gated apply mode,
  blast-radius limits); auto-vs-gated default flagged OPEN, leaning gated.
- reference: softened "no dominant Rust crate" → "this research did not find…".
- Trimmed EOF blank lines from 9 raw files (whitespace only, no provenance
  impact) for a clean first commit; `git diff --check` now clean.

Pages affected:
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (created)
- decisions/schema-and-change-management.decision.md
- decisions/messaging-scope-and-receive-model.decision.md
- plans/first-poc-outbox-publisher.plan.md
- references/rust-kafka-outbox-ecosystem.reference.md

## [2026-06-20] update | operational-changeset apply mode resolved → auto-apply

Closed the OPEN guardrail question: operational changesets **auto-apply** like
structural ones (rationale: reaching prod means convergence through lower envs).

## [2026-06-20] create | configuration & environment decision

After discussion (and reverting an earlier premature retention→config edit),
settled the config/env model and recorded it as its own decision. Model: a
changeset is `apply(env, builder)`; `env` may select values, not branch
structure. One flat `kafkaman.toml` (no profiles) is rendered per environment by
CI/CD, injecting values/secrets from the vault; the app reads one resolved file,
validated at startup. Tunable settings (retention, batch sizes) are **runtime
config re-read each boot**, not changesets — so they are tunable via config +
redeploy without authoring a changeset (a run-once changeset could not be
re-tuned). Schema decision updated to match (points 6, 7, 9, +11, guardrails);
`SetRetention`-as-changeset removed.

Pages affected:
- decisions/configuration-and-environment-model.decision.md (created, Accepted)
- decisions/schema-and-change-management.decision.md (aligned)
- proposals/01-kafkaman-objectives.proposal.md (change-engine bullet)
- plans/first-poc-outbox-publisher.plan.md (out-of-scope)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (config section)
- index.md (Decisions section)

## [2026-06-20] update | operational-changeset guardrails finalized

Made the safety nets explicit and mandatory — bulk-op batching/rate-limiting
(since lower-env testing validates correctness but not prod volume), dry-run
visibility in deploy logs, and env-targeting as the per-operation opt-out.
Residual risk (large op at prod scale + replay downstream amplification) recorded
as accepted.

Pages affected:
- decisions/schema-and-change-management.decision.md (guardrails section finalized)

## [2026-06-20] update | migrate() runs at app startup

Clarified where `migrate()` runs: **at app startup** (in `main`, before
`Runtime::start()`), mirroring the reference's `sqlx::migrate!` in `main` —
advisory-locked and idempotent. Standalone pre-deploy invocation kept as an
option. To keep startup non-blocking under auto-apply, operational changesets do
only fast bounded state changes (`Replay` flips status / bumps epoch); the heavy
rate-limited draining is the runtime subsystems' job, after boot. (Retention is
runtime config, not a `SetRetention` changeset — see the later config &
environment decision entry, which supersedes any earlier `SetRetention` mention.)

Pages affected:
- decisions/schema-and-change-management.decision.md (phase 7 sharpened)
- proposals/01-kafkaman-objectives.proposal.md (migrate bullet)

## [2026-06-20] lint | external review fixes (config decision, round 3)

Acted on a third external review (config & environment decision). Fixes:
- Secrets hygiene: `.gitignore` now ignores the resolved `kafkaman.toml` and
  keeps `!kafkaman.example.toml` tracked (previously only `.env*` was covered, so
  a rendered config with injected secrets was one `git add` from leaking).
- Config-API contradiction resolved: the decision said kafkaman *loads*
  `kafkaman.toml` while the alternatives said it *consumes a property bag the host
  produces*. Settled on a **thin loader by convention** (sqlx-style); reworded the
  rejected alternative to "a full config *framework* (profiles/layering/
  precedence)", which is what is actually rejected.
- "Values not structure" made contract-enforced, not policy: changesets now
  receive a resolved **config bag** (`apply(&self, cfg, b)`) exposing typed values
  with **no env identity** to branch on — so the rule is guaranteed by the API,
  not left to discipline. Aligned the schema decision (points 9, 11) and the raw
  design note.
- Created the committed `kafkaman.example.toml` the decision asserted exists
  (illustrative/pre-implementation; documents the intended keys).
- Log hygiene: restored a heading on an orphaned guardrails entry; corrected a
  superseded `SetRetention`-as-changeset mention in the migrate-at-startup entry.

Pages affected:
- .gitignore
- kafkaman.example.toml (created)
- decisions/configuration-and-environment-model.decision.md
- decisions/schema-and-change-management.decision.md
- raw/design/2026-06-20-kafkaman-architecture-discussion.md
- wiki/log.md (orphaned heading + supersede note)

## [2026-06-20] create | runtime composition & topology decision (draft)

Drafted the runtime/topology decision resolving objectives OQ1. Schedulers are
spawnable units (`runtime.run(shutdown)` over all subsystems, or `into_tasks()`
per subsystem) honoring a `CancellationToken`; kafkaman owns neither a process
nor the Tokio runtime. Topology (embedded vs worker-role) is a host choice via
`.subsystems(...)`; no standalone daemon since handlers are compiled-in Rust.
Request-path concerns are Axum-native (`CorrelationLayer`, admin/DLQ routes,
`serve().with_runtime()` shutdown helper); background loops are never middleware.
Send UX is opinionated around `axum-sqlx-tx`: a `Sender` extractor enqueues into
the host's ambient auto-committing tx (no manual `commit()`; business write +
outbox row commit atomically), with `send_now` as the fire-and-forget opt-out;
core `enqueue(&mut tx)` stays generic in kafkaman-sqlx. Receive/consumption side
deferred to the next discussion. Status: Draft.

Pages affected:
- decisions/runtime-composition-and-topology.decision.md (created, Draft)
- proposals/01-kafkaman-objectives.proposal.md (OQ1 resolved; kafkaman-axum bullet)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (runtime composition section)
- index.md (Decisions section)

## [2026-06-20] create | message consumption & handler model decision (draft)

Drafted the receive-side decision. Two schedulers so user code never locks Kafka:
an ingest scheduler consumes → writes the received row → commits the offset
immediately; a dispatch scheduler polls (`SKIP LOCKED`) → runs the handler. Dedup
is a log (`ON CONFLICT DO NOTHING`); failures accumulate in a bounded `errors`
JSONB array (most-recent-N ring; `attempts` is the authoritative counter). Handler
model is axum-shaped but its own thing, not HTTP: a `MessageRouter` that *is* a
`tower::Service<Message>`, keyed by `message_type`, explicit `.handler::<T>(fn)`
registration, kafkaman `FromMessage` extractors; generic Tower middleware is
inherited, http-bound axum/tower-http pieces are not (no http masquerade). The
receive tx relocates to kafkaman (Ok → business write + mark Processed commit
together). Hybrid wiring = config → migrate → consumer tower → axum tower → one
server. Retry/backoff/DLQ taxonomy deferred (fields reserved).

Consistency fixes: Core Promise #2 corrected ("offset after the handler" →
"offset after the durable receive write"); Execution Model bullet rewritten for
the two-scheduler model; objectives OQ3 (polling vs CDC) resolved → polling, CDC
not pursued for v1.

Pages affected:
- decisions/message-consumption-and-handler-model.decision.md (created, Draft)
- proposals/01-kafkaman-objectives.proposal.md (Core Promise #2; Execution Model; OQ3 resolved)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (consumption section)
- index.md (Decisions section)

## [2026-06-20] create | testing decisions — library strategy + consumer tooling (drafts)

Split the test story into two decisions, per request. (1) Library test strategy:
how kafkaman tests itself — a three-tier pyramid (unit Docker-free / Postgres
integration / Postgres+Redpanda full-loop via testcontainers), crash-injection
gates and property tests for the core invariants (effective-once, no loss,
bounded errors ring, idempotent migrate), determinism via `dispatch_once()` + an
injected `Clock` (kafkaman dogfoods its own consumer tooling), containers started
once per test binary with schema/topic isolation. (2) Consumer test tooling: a
dedicated `kafkaman-test` dev-dependency crate — `tower` `oneshot` handler tests,
a deterministic `Harness` (ephemeral schema + migrate + capturing sender +
`dispatch_once()` + Clock + row assertions) against a caller-provided connection
string, and a `#[kafkaman::test]` macro (sqlx::test-style injection, Docker-free
by default, containers/broker opt-in, per-binary containers, never auto-starts
schedulers, optional sugar over explicit `Harness::connect`). Transport stance
respects OQ2: Postgres-only fast tests + real Redpanda full-loop behind an
optional `testcontainers` feature; in-memory fake/seam deferred. Recorded a
standing principle: macros are opt-in sugar over explicit APIs, never
load-bearing.

Pages affected:
- decisions/library-test-strategy.decision.md (created, Draft)
- decisions/consumer-test-tooling.decision.md (created, Draft)
- proposals/01-kafkaman-objectives.proposal.md (kafkaman-test crate added to shape)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (testing section)
- index.md (Decisions section)

## [2026-06-20] update | dogfooding-first elevated to the primary test principle

Per discussion, sharpened the library test strategy: kafkaman's own suite uses
the consumer toolkit (`kafkaman-test`) *wherever a test sits at or above the
toolkit's abstraction*, making our suite the toolkit's primary consumer. Recorded
the boundary (layers beneath the toolkit — Harness/macro internals, SQL/DDL
builders, dedup query, ring logic, rdkafka edges — stay white-box to avoid
circularity) and the consequence (`kafkaman-test` is an early deliverable built
with core/sqlx; toolkit-using library tests live in a separate workspace test
member to avoid the `core ⇽ test` dev-dependency cycle).

Pages affected:
- decisions/library-test-strategy.decision.md (dogfooding-first as primary principle + boundary + build order)
- decisions/consumer-test-tooling.decision.md (early-deliverable consequence)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (dogfooding sharpened)

## [2026-06-20] update | PoC seeds the test Harness; V1 roadmap drafted

Two related steps. (1) Adjusted the first-PoC plan so dogfooding-first holds from
line one: added a minimal `kafkaman-test` `Harness` seed (ephemeral-schema
`migrate()`, enqueue helper, `relay_once()` one-step driver, row assertions) as
the PoC's first test-facing deliverable, routed the crash/integration gates
through it, added `-test` to the workspace stubs, and marked the full toolkit
out of scope. (2) Drafted the V1 roadmap: six milestones (M1 durable send/PoC →
M2 change-engine + config → M3 durable receive + toolkit maturity → M4
retry/backoff/DLQ → M5 observability → M6 hardening), `kafkaman-test` as a
cross-cutting track (seeded M1, matured M3, completed M6), and the explicit notes
that M4 opens with its own retry/DLQ decision and the four Draft decisions flip to
Accepted at the M1-entry review.

Pages affected:
- roadmaps/path-to-v1.roadmap.md (created, Draft)
- plans/first-poc-outbox-publisher.plan.md (Harness seed; steps + gates; out-of-scope)
- index.md (Stage line; Roadmaps section)

## [2026-06-20] create | M1 durable-send implementation review

Created a sourced review page that verifies the post-implementation M1
durable-send review against the current Rust code, plan, spec, tests, and
examples. Confirmed the main gaps around status-string centralization, database
clock ownership, Redpanda/full-loop scope, idempotency durability, worker
resilience, Harness migration races, facade usage, and index-name collisions.
Added additional findings for migration concurrency, example worker failure
visibility, duplicate descriptors, and unused worker publish-error surface.

Pages affected:
- wiki/reviews/m1-durable-send-implementation-review.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] create | M1 durable-send implementation re-review

Created a fresh sourced re-review after attempted fixes to the M1 durable-send
implementation. Verified the current working tree against the plan, active spec,
tests, examples, and prior review. Recorded fixed items, remaining scope gaps,
and new risks, including the missing `idempotency_key` upgrade migration for
existing outbox tables, still-missing Redpanda/full-loop Harness path, remaining
Harness registration race, strict clippy failure, and reserved Kafka metadata
header collision risk. Updated the wiki index date and Reviews catalog.

Pages affected:
- wiki/reviews/m1-durable-send-implementation-rereview.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] implement | M1 durable-send review-fix pass

Resolved both M1 durable-send reviews in code. Implemented the Redpanda
full-loop path (`Harness::connect_redpanda` + `HarnessPublisher::Redpanda`,
behind the `redpanda` feature) with a testcontainer gate that publishes through
`RdkafkaPublisher` and consumes the record back, asserting payload, key, and
`kafkaman-*` headers. Centralized status SQL via `OutboxStatus`, moved lease and
retry scheduling to the database clock, made the worker `run()` loop resilient
with `tracing`, advisory-locked `migrate()`, made `idempotency_key` durable and
forwarded (with an additive `AddIdempotencyKey` upgrade changeset and test),
rejected reserved `kafkaman-` headers at enqueue, and closed the Harness
registration race. Strict clippy and fmt pass. Added `cargo llvm-cov` coverage
with an 80% workspace line gate (`just test coverage`); current line coverage is
~93%. The example now consumes the `kafkaman` facade, is split into a testable
lib + thin bin, and was moved from `examples/` to `apps/axum-outbox` so
cargo-llvm-cov (which excludes `examples/`) counts it in the workspace total.
Added a `justfile` with `just test [all|unit|integration|coverage]` (default
`all` runs the full suite including integration).

Pages affected:
- wiki/plans/m1-durable-send-implementation.plan.md
- wiki/compatibility/m1-durable-send-schema-and-api-changes.compatibility.md
- wiki/index.md
- wiki/log.md
- justfile
- apps/axum-outbox/ (moved from examples/)

## [2026-06-21] review | M2 change-engine + config implementation

Wrote `wiki/reviews/m2-change-engine-config-implementation-review.reference.md`: a
line-by-line challenge of the M2 implementation against its plan and spec.

Key findings:
- H1: retry-config validation never invoked on any boot path (unit-test-only),
  so the spec's "retry validated" / roadmap "fails fast at boot" is unmet for retry.
- H2: checksum is FNV-1a 64-bit, not the SHA-256 the plan specified; ad-hoc
  delimiter-based canonical form.
- H3: migrate_dry_run creates schema/history table and backfills (not side-effect-free).
- M1: Replay leaves attempts/last_error stale, will fight M4 retry cap.
- Plus medium/low: noisy from_config error aggregation, NULL/decode conflation in
  history_row, example.toml advertising ignored keys, exclusive dry-run lock,
  missing concurrent-migrate and tunable-change gate tests.

Pages affected:
- wiki/reviews/m2-change-engine-config-implementation-review.reference.md (new)
- wiki/index.md
- wiki/log.md

## [2026-06-21] create | M3 durable receive implementation plan

Created the active M3 execution plan for durable receive and toolkit maturity.
The plan sequences the first Harness-level receive test, received-table schema,
deterministic dispatch, explicit handler API, receive-side test tooling, Kafka
ingest, and validation gates.

Pages affected:
- wiki/plans/m3-durable-receive.plan.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] update | M3 durable receive plan review fixes

Clarified the M3 dispatch transaction model before implementation. The plan now
chooses the held-transaction row-lock model, removes persistent receive claim
columns from M3, defines parked retryable failure accounting, requires
Clock-bound due predicates, adds property and crash gates, includes consume-side
replay, includes `#[kafkaman::test]`, states crate topology, and adds
greenfield `idempotency_key NOT NULL` plus bounded-errors ring verification.

Pages affected:
- wiki/plans/m3-durable-receive.plan.md
- wiki/log.md

## [2026-06-21] update | M3 durable receive first implementation slice

Recorded the first M3 durable-receive implementation slice: received-table core
types and DDL, deduplicating received insert, Harness receive helpers, minimal
message router, and held-transaction `dispatch_once()` success/failure behavior.
Proof commands recorded in the active M3 plan.

Pages affected:
- wiki/plans/m3-durable-receive.plan.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] review | M3 durable receive implementation review

Wrote a sourced post-implementation review of the first M3 durable-receive slice
against the active plan. Static review plus re-run of the cheap gates
(`cargo fmt --check`, `cargo check`, `cargo clippy`, `kafkaman-sqlx` lib tests —
all clean); Postgres integration suite not re-executed (Docker/testcontainers).
Headline finding (H1, high): the failure-path accounting update runs outside the
held transaction with an unguarded `WHERE message_id = $1`, so under concurrent
dispatch it can overwrite a row another worker committed as `Processed`,
producing a double effect — an effective-once hole the single-call tests cannot
catch. Also flagged the minimal handler surface vs plan scope, a Harness
send/receive registration conflict, the missing property/crash/ring gates, and
low-severity polish.

Pages affected:
- wiki/reviews/m3-durable-receive-implementation-review.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] update | M3 durable receive review follow-up fixes

Resolved the H1 stale failure-accounting race by guarding receive failure
accounting to `Pending`/`Retryable` rows, fixed Harness send/receive migration
registration for same-type send-after-receive use, removed the transient
in-transaction `Processing` write, and changed missing receive lookups to report
the missing idempotency key. Added receive integration gates for stale failure
interleaving, crash redrive, randomized duplicate redelivery convergence,
bounded 20-entry error retention, and missing received-row error reporting.
Serialized the Docker-backed durable receive integration file with an in-process
async mutex to avoid local Postgres container pool flakiness under the default
parallel test runner.

Verification passed:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-features`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`
- `cargo test -p kafkaman-sqlx --lib`
- `cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`

Pages affected:

- wiki/reviews/m3-durable-receive-implementation-review.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] update | M3 durable receive review double-check

Double-checked the sourced M3 durable-receive implementation review against the
landed code and tests. Re-ran the cheap gates (`cargo fmt --all -- --check`,
`cargo check --workspace --all-features`, `cargo clippy --workspace
--all-targets --all-features -- -D warnings`, `cargo test -p kafkaman-sqlx
--lib`) and the Docker/testcontainers-backed receive integration suite
(`cargo test --manifest-path tests/durable-send/Cargo.toml --test
durable_receive`); all passed after escalating the integration run for Docker
access. Confirmed H1 by exact failure-path SQL/control-flow inspection and
confirmed M2 with a temporary regression test that failed with PostgreSQL
`42P01` missing outbox relation after receive-side registration. Corrected the
review and index to state that the bounded error-ring SQL is inspected as
correct, but the 20-entry cap is not yet test-pinned.

Pages affected:
- wiki/reviews/m3-durable-receive-implementation-review.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] create | Deep durability testing proposal

Added a living deep-testing proposal cataloguing adversarial concurrency, crash,
and durability test designs for kafkaman's durable-execution paths, plus the
invariant-escape technique that surfaced the M3 review's H1. Seeded with 13
test designs across concurrency, crash, robustness, atomicity, bounded-resource,
clock, and ingest classes, a priority order, a status-tracking table, and open
design questions (crash-durable receive `attempts`; parking on `MissingHandler`;
the controlled-interleaving primitive for `kafkaman-test`). Predicts A1, B2, and
C1 reveal real defects today.

Pages affected:
- wiki/proposals/05-deep-durability-testing.proposal.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] review | M3 durable-completion implementation

Adversarial in-depth review of the M3 durable-completion slice against landed
code, focused on failure modes the deep-durability catalog does not anticipate.
Findings: F1 ingest poison stalls the partition (offset never advances on any
pre-commit error); F2 PK-vs-`ON CONFLICT`-target mismatch is a second stall
vector; F3 the dispatcher loop does not drain (one row per poll interval, against
the plan's stated "drain"); F4 parked failure/missing-handler rows are
unrecoverable through the shipped API (`Replay::received` is Processed-only and
dispatch never reclaims Retryable/NULL); F5 `Replay::received` re-executes handler
side effects on already-processed rows. Also recorded plan-accuracy gaps: the
Phase 0 interleaving primitive, the real two-worker A1 test, and the Phase 4 J3
consume-then-produce full-loop are claimed in Progress but not present in code.

Pages affected:
- wiki/reviews/m3-durable-completion-implementation-review.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] update | M3 durable-completion review fixes

Recorded implementation follow-up for the M3 durable-completion review fixes.
F1-F9 are now closed by deterministic ingest poison skips with offset commit,
receive insert conflict hardening, dispatcher backlog drain and mid-dispatch
shutdown tests, retryable-only `Replay::received`, structured receive failure
causes, and Redpanda poison/redelivery/topic-provenance coverage. The active
plan still keeps broader M3 closure gates open: controlled interleaving, real
two-worker A1, G1/G2 crash/offset uncertainty, consume-then-produce Redpanda J3,
handler-model reconciliation, and M3 spec promotion.
Pages affected:
- wiki/reviews/m3-durable-completion-implementation-review.reference.md
- wiki/plans/m3-durable-completion.plan.md
- wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] update | M3 ingest poison quarantine policy

Implemented and documented the ingest poison quarantine policy prompted by the
G1-G5 review. Added accepted decision `ingest-poison-quarantine-policy`, durable
`received_ingest_failures` quarantine table, consecutive-skip circuit breaker,
explicit receive insert outcomes for idempotency duplicate vs message-id
conflict, handler-domain SQL failure classification, and replay history
preservation. Evidence: `rtk cargo test --manifest-path
tests/durable-send/Cargo.toml --tests` (37 passed), `rtk cargo test
--manifest-path tests/durable-send/Cargo.toml --features redpanda --test
redpanda_full_loop -- --test-threads=1` (5 passed), and `rtk cargo test
--workspace --all-features` (61 passed).
Pages affected:
- wiki/decisions/ingest-poison-quarantine-policy.decision.md
- wiki/reviews/m3-durable-completion-implementation-rereview.reference.md
- wiki/reviews/m3-durable-completion-implementation-review.reference.md
- wiki/plans/m3-durable-completion.plan.md
- wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] update | M3 durable-completion review double-check

Added follow-up verification notes to the M3 durable-completion implementation
review after checking each claim against the current code and tests. Clarified
the scope of F1/F2, confirmed F1-F6 and the plan-accuracy gaps, and added three
missed gaps: case-variant reserved headers can poison ingest before offset
commit (F7), consumed source topic is not checked or persisted from the broker
record (F8), and graceful shutdown mid-dispatch is not proven by the shipped
dispatcher cancellation test (F9/H3). Updated the index summary accordingly.

Pages affected:
- wiki/reviews/m3-durable-completion-implementation-review.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] rereview | M3 durable-completion fixes

Second-pass review after the review-fix slice. Confirmed F1-F5 closed and
test-pinned (ingest poison skip, untargeted ON CONFLICT, dispatcher drain,
retryable-only Replay::received). Found the fixes traded stalls for silent drops:
G1 ingest skip is silent/untraceable data loss; G2 schema skew is treated as
poison and dropped topic-wide on a bad deploy ordering; G3 untargeted ON CONFLICT
silently drops a different logical message on message_id collision; G4 failure
`kind` keys off the Rust error variant (handler SQL errors mis-bucket as
Infrastructure); G5 replay erases the error history of the parked rows it targets.
Recommended a single ingest-poison/dead-letter policy decision. Noted residual
scope: no run_ingest loop, Phase 0 primitive / real two-worker A1 / J3 still absent.

Pages affected:
- wiki/reviews/m3-durable-completion-implementation-rereview.reference.md
- wiki/index.md
- wiki/log.md
## [2026-06-22] update | M4 retry/backoff/DLQ first slice

Started M4 reliability work. Added active M4 plan and compatibility note,
marked M3 completed and M4 active in the V1 roadmap, and implemented
policy-driven receive retry scheduling: `ResolvedConfig` retains `RetryConfig`,
`ReceivedTable` carries the per-message retry policy, receive failure accounting
computes `next_attempt_at`, honors configured `errors_limit`, and moves
exhausted rows to terminal `Failed` table-backed DLQ state. Redrive/admin
surfaces remain in the active M4 plan.
Pages affected: `crates/kafkaman-config/src/lib.rs`,
`crates/kafkaman-sqlx/src/lib.rs`, `tests/durable-send/Cargo.toml`,
`tests/durable-send/tests/durable_receive.rs`,
`wiki/plans/m4-retry-backoff-dlq.plan.md`,
`wiki/compatibility/m4-retry-backoff-runtime-api.compat.md`,
`wiki/roadmaps/path-to-v1.roadmap.md`, `wiki/index.md`, `wiki/log.md`.

## [2026-06-22] update | dispatch failure accounting hardening

Changed receive dispatch failure accounting to hold the claimed row lock through
normal failure recording by using a handler savepoint, with best-effort rollback
and separate-connection fallback only when the transaction connection is already
unusable. Updated M3 spec, dispatch decisions, compatibility note, and
deep-durability proposal evidence to match the new single-flight behavior.
Pages affected: `crates/kafkaman-sqlx/src/lib.rs`,
`tests/durable-send/tests/durable_receive.rs`,
`wiki/specs/m3-durable-receive.spec.md`,
`wiki/decisions/dispatch-infrastructure-error-classification.decision.md`,
`wiki/decisions/dispatch-stats-semantics.decision.md`,
`wiki/decisions/missing-handler-dispatch-policy.decision.md`,
`wiki/compatibility/m3-durable-receive-review-fix-api.compat.md`,
`wiki/proposals/05-deep-durability-testing.proposal.md`,
`wiki/reviews/m3-durable-completion-implementation-review.reference.md`,
`wiki/index.md`, `wiki/log.md`.
