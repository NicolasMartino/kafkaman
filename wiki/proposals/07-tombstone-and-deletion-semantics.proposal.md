# Tombstone and Deletion Semantics

- Document Class: Proposal
- Status: Proposed
- Date: 2026-08-12
- Category: Message semantics
- Scope: Proposes real Kafka tombstone *ingestion* so kafkaman can consume compacted topics produced by systems it does not own; emission-side deletion is now governed by soft-delete-first in proposal 09.
- Sources:
  - User design discussion, 2026-08-12
  - raw/design/2026-08-12-entity-first-propagation-discussion.md
  - Prior-art review of `cqrs-fullstack/code/shared/messaging` (workout2 project snapshot)
  - wiki/specs/m3-durable-receive.spec.md
  - wiki/decisions/ingest-poison-quarantine-policy.decision.md
- Related:
  - wiki/decisions/dispatch-handler-ordering.decision.md
  - wiki/proposals/09-entity-first-propagation.proposal.md
  - wiki/proposals/06-typed-idempotency-identity-and-error-row-symmetry.proposal.md
  - wiki/proposals/08-listen-notify-scheduler-wakeup.proposal.md
  - wiki/decisions/kafka-ingest-identity-and-ordering.decision.md

## Revision 2026-08-12: emission superseded, ingestion retained

This proposal originally selected **per-message-type opt-in real tombstones on
both the send and receive paths**. The emission half of that selection is
superseded by
[09-entity-first-propagation](09-entity-first-propagation.proposal.md), which
adopts **soft-delete-first**: the entity flows in full with a deleted status
and/or delete intent, and rows are reclaimed by a later batch.

The reversal turns on a correctness hazard this proposal did not weigh. Real
tombstones are reclaimed after `delete.retention.ms`, so a cache offline longer
than that window misses the delete and holds a ghost entity forever. A
soft-delete record is a normal record with a value, is therefore always the last
record for its key, and is retained by compaction indefinitely — so bootstrap
always observes the delete. The original Option 1 critique (unbounded topic
growth) is real but is a storage cost, and a storage cost was judged preferable
to a silent-correctness cost. Reclamation is deferred to a later phase that emits
a real tombstone only after a window comfortably longer than any consumer's
maximum offline time.

What survives unchanged is this proposal's Option 1 critique that typed-payload
deletion **cannot interoperate with external producers that emit real
tombstones**. Debezium, connector-based sources, and any foreign compacted topic
will send a null value with a non-null key regardless of kafkaman's own emission
doctrine. That is now the residual scope of this proposal: kafkaman still needs
to *ingest* real tombstones correctly even though it no longer *emits* them.

The send-path and handler-surface sections below are retained as superseded
context and should not be implemented as written.

## Context

In Kafka, deletion on a log-compacted topic is expressed as a record with a
non-null key and a null value. Compaction retains the tombstone long enough for
consumers to observe it, then reclaims the key. Any consumer maintaining a local
cache of a compacted reference topic must interpret that record as "remove this
key", not as a corrupt message.

kafkaman today has no representation for this on either side.

On the receive path, `ReceivedIngestFailureKind::MissingPayload`
(`crates/kafkaman-core/src/lib.rs:587`) classifies a null payload as an ingest
failure, so ingest writes a quarantine row into `received_ingest_failures` and
commits past the record. A deletion therefore never reaches a handler and the
cache silently retains a row that the producer intended to delete.

Prior art confirms this is load-bearing rather than theoretical. The
`cqrs-fullstack` snapshot emits deletions on its compacted `user.sync` topic as
real tombstones: `user_delete_event` constructs an event with
`serde_json::Value::Null` and a required message key, and the relay branches on
`event.payload.is_null()` to call `produce_tombstone`. Under kafkaman as it
stands, every one of those deletions would land in quarantine.

This matters most under the reference-replication use case, where a downstream
domain maintains a cached projection of another domain's entities. A cache that
cannot represent deletion is not a correct cache.

## Options

1. **No tombstones; model deletion as a typed payload.**
   Require a domain-level `Deleted { id }` variant carried as a normal non-null
   payload. Requires no kafkaman changes and works on non-compacted topics, but
   it cannot drive Kafka compaction — the key is never reclaimed, so the topic
   grows without bound — and it cannot interoperate with external producers such
   as Debezium or connector-based sources that emit real tombstones.
2. **Always reinterpret a null payload as a deletion.**
   Removes `MissingPayload` and treats every null value as a delete. Simple, but
   it silently reinterprets genuinely truncated or corrupt records as destructive
   operations against the cache, which is the opposite of the quarantine policy
   kafkaman applies everywhere else.
3. **Per-message-type opt-in tombstone support.**
   A message type declares whether it is tombstone-bearing. For such a type, a
   null value with a non-null key is a first-class deletion; for every other type
   a null value remains a `MissingPayload` quarantine.

Selected option, as revised: **3, receive path only.**

Option 3's reasoning still holds for ingestion — it preserves kafkaman's bias
toward explicit weakening over silent reinterpretation, and keeps the
destructive reading of a null payload confined to types whose authors asked for
it. Option 1 is what kafkaman now does on its **own** emission path under
proposal 09, having accepted its unbounded-growth cost deliberately; its
interoperability weakness is precisely why ingestion support is still required
here.

## Proposal

Introduce real-tombstone **ingestion** as a declared per-message-type capability.

### Message type declaration

A message type opts in to tombstone-bearing ingestion as part of its descriptor.
Types that do not opt in keep today's behavior exactly.

### Receive path

For a tombstone-bearing type, ingest treats a null value with a non-null key as a
valid record and writes a normal received row flagged as a deletion, carrying the
usual identity, metadata, status, and retry fields. Deduplication, claiming,
retry, and DLQ behavior are unchanged — a deletion is ordinary durable work.

A null value with a null key remains a `MissingPayload` quarantine for every
type, including tombstone-bearing ones.

Under proposal 09 the ingested tombstone must resolve to the same cache
operation as a soft delete, so that a cache fed by a foreign producer and a
cache fed by kafkaman converge to the same state. A tombstone carries no
payload, so its ordinal cannot come from the body.

**Resolved by the 2026-08-13 revision of proposal 09.** The convergence ordinal
is the Kafka offset, which a foreign tombstone has by virtue of arriving on the
topic at all. No header is required and no per-type derivation rule is needed —
a Debezium tombstone orders against kafkaman-produced records identically,
because both are ordered by the same partition offsets.

### Handler surface

The deletion flag should travel in `ReceivedMeta` rather than by changing the
shape of `P`. Handlers for tombstone-bearing types branch on metadata, which
avoids forcing every payload type into an enum wrapper and keeps the M3 handler
signature stable.

**2026-08-26: settled — the flag travels in `ReceivedMeta`, and it is now
implemented as a reserved field.** `wiki/decisions/dispatch-handler-ordering.decision.md`
accepts that the handler signature must be able to carry a tombstone *before* it
is published, because absence cannot be retrofitted into `T` without a breaking
change — and the runtime builder is what publishes it. Phase 0 of
`wiki/plans/runtime-builder-and-axum-composition.plan.md` weighed this section's
direction against an `Option<T>`-style payload and kept this one.

The deciding argument against `Option<T>` is that it prices a feature nobody has
into every handler. kafkaman emits soft deletes as full entity states, so on any
topic kafkaman produces the payload is *never* absent; `Option<T>` would make
every handler in every service match on a `None` that its own producer cannot
emit, to accommodate foreign producers such as Debezium that most services never
consume. Metadata is where a per-delivery fact that most handlers ignore belongs.

What landed in `crates/kafkaman-core/src/rows.rs`:

- `ReceivedMeta` is now `#[non_exhaustive]`. It was a plain struct with sixteen
  public fields, so *adding* the flag later would itself have been the breaking
  change the decision forbids deferring. This is the part that had to happen
  before first publication; the rest is additive.
- It carries `deleted: bool`, `#[serde(default)]`, read through
  `ReceivedMeta::is_deleted()`. `impl From<&ReceivedRow>` sets it to `false`
  unconditionally, and the doc comments say so.

Nothing else here changes. Ingestion, ordering, and identity remain Proposed and
unimplemented; nothing writes `true`; and the cache table's `deleted` column
stays reserved and unwritten. A handler that branches on `is_deleted()` today is
writing dead but forward-compatible code, which is the point — when ingestion
lands it keeps compiling and starts seeing `true`.

### Identity

Tombstones remain subject to the identity rules in proposal 06 and still require
an idempotency key. This is the sharpest open problem: external producers that
emit bare tombstones will not set `kafkaman-idempotency-key`, so a
tombstone-bearing type consumed from a foreign producer needs a defined
fallback — most plausibly a digest derived from the source topic and record key,
declared explicitly by the message type.

### Send path (superseded)

The original proposal added an explicit enqueue variant writing a tombstone
outbox row, requiring a partition key and mapping to a null-value record at
publish. **Superseded by proposal 09**: kafkaman emits soft deletes as full
entity states instead. This section is retained only as the record of what was
considered.

A future reclamation phase may reintroduce a narrow version of this — emitting a
real tombstone after a retention delay to let compaction reclaim keys — but that
is deliberately out of scope until topic growth is a demonstrated problem.

## Consequences

kafkaman becomes able to consume compacted topics produced by systems it does not
own, including Debezium and connector-based sources, without quarantining their
deletions.

The costs:

- A changeset is required for the deletion flag column on the received table, and
  status/flag SQL must be generated from the Rust type as the existing status
  literals are, so database and Rust cannot drift.
- `MissingPayload` narrows in meaning; its decision page and the quarantine
  policy need updating to state that it applies to non-tombstone-bearing types
  and to keyless records.
- Tests must cover the destructive direction specifically: a tombstone for an
  unknown key, a tombstone redelivered after the delete already applied, and a
  tombstone racing a later non-null record for the same key.
- Two deletion representations now coexist — kafkaman's own soft delete and a
  foreign real tombstone — and both must converge to the same cache state.
  That equivalence needs an explicit test.

## Open Questions

1. How is the idempotency key derived for tombstones from external producers, and
   should that derivation be declared per message type?
2. ~~Where does `entity_version` come from for a foreign tombstone that carries
   no kafkaman headers?~~ **Resolved 2026-08-13**: the ordinal is the record's
   Kafka offset, which every ingested record has regardless of producer.
3. Does a deletion need a distinct terminal status, or is the existing
   `Processed` status plus a deletion flag sufficient for operator triage?
4. Should kafkaman assert or warn when a tombstone-bearing type is bound to a
   topic that is not configured for compaction?
5. At what point does the deferred reclamation phase become worth building, and
   what signal indicates it?

## Promotion Target

If accepted, promote into a decision that fixes the tombstone-bearing type
declaration, the `ReceivedMeta` deletion flag, the identity and version fallback
rules for foreign tombstones, the convergence equivalence between soft delete and
ingested tombstone, and the required changesets.
