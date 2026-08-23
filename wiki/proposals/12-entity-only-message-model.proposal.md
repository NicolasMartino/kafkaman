# Entity-Only Message Model

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-13
- Revised: 2026-08-13 (post-review; selected option changed; accepted); 2026-08-14 (purview narrowed to compact entity cache only)
- Category: Messaging model
- Scope: Accepts collapsing kafkaman's message model to compact entity-cache
  propagation only. The 2026-08-14 amendment supersedes the earlier
  `compact`/`delete` retention-class product model: non-entity work items,
  commands, generic jobs, emails, payments, and direct transport are outside
  kafkaman's purview.
- Sources:
  - raw/design/2026-08-13-offset-as-convergence-ordinal-discussion.md
- Related:
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/proposals/09-entity-first-propagation.proposal.md
  - wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md
  - wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md
  - wiki/proposals/03-direct-transport-mode.proposal.md
- Promotion Target: promoted as an amendment to the entity-first propagation
  decision and a scope revision to the messaging-scope decision.

## Context

Entity-first made entities the headline use case but left the general message
model intact: a type may be a propagating entity or an ordinary work item, and
the library serves both.

The observation motivating this proposal is that **every unresolved caveat in the
current design is a snapshot-versus-non-snapshot caveat**, not a mechanism
problem:

- Dropping the outbox on recovery is self-healing for entities and permanent loss
  for work items, which is why proposal 11 has to make restore policy a per-type
  property and require a dropped-row audit for one class only.
- Convergence is meaningful for entities and meaningless for work items.
- Proposal 09 open question 3 exists solely to let non-propagating types opt out
  of costs they should not pay.

Each of those is branching introduced by the duality itself. The question is
whether removing the second class removes the branching or merely hides it.

## Options

1. **Status quo — entity-first with general message types.** Two classes, with
   per-type policy wherever they differ.
2. **One wire shape, per-type convergence opt-out.** Every type carries an
   `entity_key`, but the convergence guard is switchable per type. Removes the
   structural duality while keeping a behavioral flag.
3. **Entity-only by redefinition.** Every message type is an entity; work items
   are modeled as entities whose key is unique per item, with no type-level
   classification of any kind. *Originally selected 2026-08-13; **rejected the
   same day on review** — see below.*
4. **Entity-only by exclusion.** kafkaman refuses non-entity types outright:
   emails, payments, commands, analytics, and generic durable jobs are out of
   scope, and direct transport mode goes with them. The strict reading of
   "entity-only". **Selected by the 2026-08-14 purview amendment.**
5. **Uniform entity model with a declared retention class.** Every type is an
   entity and shares one code path; a type additionally declares whether its key
   space is bounded (`compact`) or unbounded (`delete`), which drives topic
   configuration, cache-table generation, and bootstrap eligibility. **Selected
   2026-08-13; superseded 2026-08-14 as a product-scope model.**

Selected option: **4, narrowed to compact entity-cache propagation**. Accepted
2026-08-14 as the product purview. The same-day follow-up removes the public
`delete` compatibility surface rather than carrying it forward: kafkaman no
longer presents delete-retention work items as a first-class message type.

Options 4 and 5 did not exist in the original version of this proposal, which is
the defect review found first: option 3 was selected against a field that
contained no strict interpretation of the idea it claimed to implement.

## 2026-08-14 Purview Amendment

kafkaman's purview is now the distributed cache for domain entities:
full-snapshot entity propagation, compacted topics, guarded cache upsert,
per-entity outbound supersede, state-sourced republish, bootstrap/readiness, and
soft-delete-first deletion.

Messages such as "send welcome email to user 123", payment execution, commands,
analytics events, generic jobs, and `mutation_jobs`-style durable queues are
outside kafkaman's product surface. They may still need durable execution in an
application, but that does not make them part of this library's scope.

This deliberately reverses the 2026-08-13 option 5 selection. The earlier
`compact` / `delete` distinction remains useful as implementation history and a
superseded design record because M5 briefly introduced `RetentionClass::Delete`
as a default for existing M1-M4 APIs. The same-day follow-up removes that public
surface instead of treating it as long-term compatibility.

M4's retry/backoff/DLQ machinery is not promoted as a general durable job queue
promise under this amendment. It remains relevant only where the entity-cache
pipeline needs failure accounting, redrive, poison handling, and operator
visibility for entity propagation.

## Historical 2026-08-13 Argument: Why Work Items Fit As Entities

The following section records the superseded 2026-08-13 argument for option 5.
It explains why work items could fit the same mechanics, but no longer controls
kafkaman's product purview.

The objection that reaches for option 1 is that a command — "send the welcome
email" — is not an entity. Under durable execution it already is: the row *is*
the work item, with a status that transitions. Modeled as an entity with a key
unique per item, the properties fall out benignly rather than awkwardly:

- **The convergence guard becomes a harmless no-op.** With one key per work item
  there is never a second state to lose to, so the guard never rejects anything
  it should have applied.
- **Compaction collapses nothing meaningful.** One record per key is that work
  item's final state.
- **Execute-once is unaffected**, because that was always `idempotency_key`'s job,
  never the convergence guard's.
- **Intermediate status transitions were already advisory.** The accepted
  advisory-intent rule states intent is reliable for live tailing and never for
  bootstrap, so a consumer that needed every transition was already outside the
  supported model.

This reasoning survived review as a claim about *mechanism* fit. The 2026-08-14
amendment accepts a different product boundary: mechanism fit is not enough to
keep work items inside kafkaman's purview.

## Why option 3 fails: it removes the signal, not the duality

Entity types and work-item types need different topic retention. That is true
under entity-first and it stays true under any option here:

- Compaction retains one record per key forever. Entity key cardinality is
  bounded — the number of users, products, exercises. Work-item cardinality is
  unbounded — every payment, every email, accumulating for the life of the
  system. A compacted work-item topic grows without bound.
- Delete-by-age bounds it, but proposal 11's retention invariant forbids
  delete-by-age on entity topics precisely because it can remove the **last
  surviving record for a quiet key**, so a fresh bootstrap never learns the
  entity existed.

**This two-configuration split is not introduced by entity-only.** It already
exists under entity-first, where proposal 11 states the invariant as "entity
topics use compaction alone" and leaves everything else on delete-by-age. The
earlier version of this proposal claimed entity-only *relocated* the duality into
broker configuration; that overstated it. The configurations were already there.

What option 3 actually changes is worse and more specific. Under entity-first,
the type declares whether it propagates, so the correct topic configuration
follows mechanically from something kafkaman knows, and proposal 11's proposed
boot-time check — "kafkaman should **validate this at boot** where the broker
exposes topic configuration, matching the M2 fail-fast idiom" — has an input.
Option 3 deletes that declaration. Every type looks like an entity, so the
validator has nothing to compare the broker's configuration against.

The result is the same two configurations with the misconfiguration made
**undetectable**, and both failure modes are silent:

- Compaction on a work-item topic: unbounded growth, discovered as a disk alert.
- Delete-by-age on an entity topic: the last record for a quiet key disappears,
  discovered when a rebuilt cache is missing entities nobody noticed were rare.

Option 3 is therefore strictly worse than entity-first on this axis while being
no better on any other. It is the weakest position in the set.

## Why Option 4 Is Now Selected

Hard exclusion is the clean reading of "entity-only" and is now accepted. The
library identity is not "a durable execution engine that happens to support
entity propagation"; it is "a distributed cache for compacted domain entities."

The cost is real and accepted. The reference project's `mutation_jobs` evidence
still shows that applications may need durable work queues. The revised judgment
is that those queues are adjacent application infrastructure, not kafkaman's
public product surface. kafkaman should avoid becoming the general answer for
"run this work later with retries" because that expands the library away from
cache coherence.

This also means the earlier M4 value argument changes. Retry, backoff, DLQ,
poison classification, and error history remain useful to operate the
entity-cache pipeline, but they no longer justify exposing non-entity message
types, commands, jobs, or direct transport as first-class kafkaman features.

## Historical 2026-08-13 Model: The Declared Retention Class

This section records the superseded option 5 model. It is not the current
product-scope decision, but it explains why `RetentionClass` briefly existed
before the 2026-08-14 removal pass.

A type declares one field: whether its entity key space is **bounded** or
**unbounded**. Everything policy-level follows from it.

| | `compact` (bounded key space) | `delete` (unbounded key space) |
|---|---|---|
| Topic `cleanup.policy` | `compact` alone, never with delete | `delete` |
| Cache table | generated and maintained | none |
| Bootstrap by replay | supported (proposal 10) | not offered |
| Convergence guard | meaningful | present, no-op by construction |

In the superseded 2026-08-13 model, this closed proposal 09's open question 3.
A non-propagating type did not opt out of a class; it declared `delete` and was
charged for nothing it did not use.

### It restores the boot check proposal 11 already wants

The declaration is exactly the input proposal 11's boot-time validation was
missing. kafkaman reads the topic configuration at boot and fails fast when it
disagrees with the declared class, in the M2 idiom. No new machinery is required
on either side — the validator was already proposed, and this gives it something
to check. The two proposals fit together and currently do not know it.

The residual failure mode is honest and should be stated: the declaration is an
**unverifiable claim about key cardinality**. A type declared `compact` whose key
space is in fact unbounded still grows forever. What changes is that the error
moves from an invisible broker setting to a field in the type definition —
visible in code review, and checkable against the broker at boot. Reduced and
localized, not eliminated.

### The default makes this non-breaking

The superseded option defaulted `entity_key` to `message_id` and the retention
class to `delete`.

A work item's "unique key per item" is then not something adopters invent — it is
the physical record identity every message already carries. Every M1-M4 type
compiles and behaves exactly as it does today: no cache table, no compaction, a
convergence guard that is structurally incapable of rejecting anything. Declaring
a real entity key and `compact` is what opts a type *into* cache semantics.

This was the sharpest practical difference from options 3 and 4, both of which
would have required `entity_key` on every type and therefore broken the general
send/receive surface shipped in M1-M4. It closed open questions 3 and 4 of the
original version of this proposal.

## What Collapses After The Purview Amendment

The current model removes the work-item branch instead of reclassifying it.
Within kafkaman's purview there is one kind of message: a compact entity
snapshot that feeds a cache.

Collapsed:

- **No public `delete` class.** Delete-by-age work-item topics are outside the
  library purview, so topic validation has one in-scope target:
  `cleanup.policy=compact` alone for entity topics.
- **No cache eligibility fork.** In-scope types have cache tables and bootstrap
  semantics; out-of-scope work queues do not.
- **No work-item restore story.** kafkaman's state-sourced republish and resync
  sweep are about current entity state only. Durable job recovery remains an
  application concern.
- **No direct transport exception.** Direct transport has no outbox and therefore
  cannot provide the key-level outbound serialization required for compact
  entity cache correctness. It stays outside purview.

The earlier 2026-08-13 claim was a halved, declared, and boot-validatable
duality. The 2026-08-14 amendment is stricter: kafkaman does not carry that
duality as a product model.

## Conflicts This Resolves Or Moves Out

Proposal 11's `kafkaman_cache` rebuild directive becomes clean again:
`kafkaman_cache` exists only for compact entity topics, and replay is
authoritative for that class.

The resync sweep dependency also narrows. kafkaman still needs a state-sourced
resync surface for compact entities, but it no longer needs a generic
"non-terminal work item" sweep. That work-item recovery problem is real, but it
belongs to whichever application-specific durable job queue owns the status
machine.

This amendment creates one documentation debt instead of the old policy fork:
older M1-M4 pages and current compatibility notes describe durable execution
surfaces that existed before the purview was narrowed. Those pages need either
compatibility framing or later spec consolidation so they do not read as a v1
product promise for non-entity work.

## What This Reverses

This amendment **does** reverse the messaging-scope decision's rejection of
propagation-only scope. The earlier statement that "nothing is removed" is no
longer true.

What remains inside kafkaman:

- compact entity snapshot propagation;
- durable outbox/inbox mechanics needed to publish and consume those entity
  snapshots reliably;
- retry/backoff/DLQ as operational support for that entity-cache pipeline;
- state-sourced republish, bootstrap/readiness, topic-lifecycle invalidation,
  and soft-delete-first deletion.

What moves outside kafkaman:

- commands-over-Kafka and direct transport;
- emails, payments, analytics events, generic jobs, and durable work queues;
- `mutation_jobs`-style application mutation dispatch.

This is a breaking product-scope and pre-v1 API revision; the same-day
implementation removes the public retention-class compatibility surface.

## Consequences

Positive:

- The library's identity is clear: distributed cache coherence for compact
  domain entities.
- Topic validation becomes simpler for the public model: in-scope entity topics
  must be compacted and must not use delete-by-age.
- Bootstrap/readiness, state-sourced republish, and cache-table generation all
  target the same bounded-key entity model.
- The implementation can shed or hide general durable-job promises rather than
  carrying them as a second product.

Costs and risks:

- Applications that need durable jobs still need another mechanism. That is an
  explicit scope tradeoff, not a hidden non-goal.
- Older M1-M4 documentation still exposes broader durable-execution surface than
  the new purview. Spec consolidation remains follow-up documentation work.
- M4 is narrower than originally framed: failure handling remains important, but
  only as cache-pipeline support.
- Removing `RetentionClass` and the default `entity_key = message_id` is a
  breaking pre-v1 API change, but it matches the entity-only model.

## Implementation Open Questions

1. ~~Should `RetentionClass::Delete` be removed from public API, kept only as a
   deprecated compatibility path, or hidden behind an internal/legacy feature?~~
   **Resolved 2026-08-14.** Remove `RetentionClass`,
   `KafkaMessage::retention_class`, and `MessageDescriptor.retention_class`
   entirely.
2. Which M1-M4 APIs and specs remain valid as entity-cache plumbing, and which
   ones need deprecation because they advertise generic durable jobs?
3. Should boot validation now reject every in-purview topic that is not
   `cleanup.policy=compact` alone, and how should it behave when broker topic
   configuration is unavailable?
4. Should `Replay::outbox` be rejected for all in-purview kafkaman message types,
   with only state-sourced republish exposed?
5. What migration guidance do adopters get if they were using kafkaman as a
   generic durable job queue before v1 scope narrowed?
