# Entity-Only Message Model

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-13
- Revised: 2026-08-13 (post-review; selected option changed; accepted)
- Category: Messaging model
- Scope: Accepts collapsing kafkaman's message model so every type is an entity, using a uniform entity model with a **declared retention class** rather than entity-only-by-redefinition, because the latter removes the type-level signal that makes topic misconfiguration detectable.
- Sources:
  - raw/design/2026-08-13-offset-as-convergence-ordinal-discussion.md
- Related:
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/proposals/09-entity-first-propagation.proposal.md
  - wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md
  - wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md
  - wiki/proposals/03-direct-transport-mode.proposal.md
- Promotion Target: promoted as an amendment to the entity-first propagation decision. Under the selected option this is an **extension, not a revision** — see *What this does and does not reverse*.

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
   "entity-only". Rejected — see below.
5. **Uniform entity model with a declared retention class.** Every type is an
   entity and shares one code path; a type additionally declares whether its key
   space is bounded (`compact`) or unbounded (`delete`), which drives topic
   configuration, cache-table generation, and bootstrap eligibility. **Selected.**

Selected option: **5**. Accepted 2026-08-13 as the M5 model.

Options 4 and 5 did not exist in the original version of this proposal, which is
the defect review found first: option 3 was selected against a field that
contained no strict interpretation of the idea it claimed to implement.

## Why work items fit as entities

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

This reasoning survives review and is why options 3 and 5 both remain live where
option 4 does not. It is a claim about *mechanism* fit, and it holds. What it
does not establish is that no *policy* distinction remains, which is where
option 3 fails.

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

## Why option 4 is not selected

Hard exclusion is product-clean and internally consistent, and it is the honest
strict reading of "entity-only". It is rejected because it argues against the
evidence base the project's scope decision rests on, not merely against the
decision.

`messaging-scope-and-receive-model.decision.md` rejected propagation-only as
Option B: *"too narrow — leaves the handler/retry/durable engine (the most
valuable, currently hand-rolled part) outside the library."* That was not a
preference. It rests on the reference project's production history: what
RepForge **dropped** was the Kafka command transport (`commands_inbox`), and what
it **kept and built out** was `mutation_jobs` — a durable job queue with a status
machine, `attempt_count`, `next_attempt_at`, and `FOR UPDATE SKIP LOCKED`
claiming. Hard exclusion puts the one artifact the evidence proved was needed
back outside kafkaman.

Two further costs, which should be priced rather than left implicit:

- **It exports the branching rather than eliminating it.** The adopter runs
  kafkaman for entities plus a second, hand-rolled durable job mechanism — which
  is the thing kafkaman exists to stop. The complexity moves into the adopter's
  architecture, where it is larger and less visible than a declared field.
- **It guts M4.** Retry, backoff, DLQ, poison classification, and bounded error
  history are work-item machinery. A pure cache barely needs them: proposal 09
  already notes a stale entity message simply loses the guard and parks in the
  DLQ without freezing its entity. M4 is a shipped milestone; excluding work
  items strands most of its value.

Option 4 should only be selected as a deliberate rejection of the `mutation_jobs`
evidence, with M4's scope reconsidered in the same breath. It is recorded here so
that choice is available and costed, not so it is quietly unavailable.

## The declared retention class

A type declares one field: whether its entity key space is **bounded** or
**unbounded**. Everything policy-level follows from it.

| | `compact` (bounded key space) | `delete` (unbounded key space) |
|---|---|---|
| Topic `cleanup.policy` | `compact` alone, never with delete | `delete` |
| Cache table | generated and maintained | none |
| Bootstrap by replay | supported (proposal 10) | not offered |
| Convergence guard | meaningful | present, no-op by construction |

This is what closes proposal 09's open question 3. A non-propagating type does
not opt out of a class; it declares `delete` and is charged for nothing it does
not use.

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

Default `entity_key` to `message_id` and the retention class to `delete`.

A work item's "unique key per item" is then not something adopters invent — it is
the physical record identity every message already carries. Every M1-M4 type
compiles and behaves exactly as it does today: no cache table, no compaction, a
convergence guard that is structurally incapable of rejecting anything. Declaring
a real entity key and `compact` is what opts a type *into* cache semantics.

This is the sharpest practical difference from options 3 and 4, both of which
require `entity_key` on every type and therefore break the general send/receive
surface shipped in M1-M4. It closes open questions 3 and 4 of the original
version of this proposal.

## What collapses

Stated as an accounting rather than a claim, because earlier versions of this
proposal overstated it twice.

**Unified** — forked under entity-first, not forked here:

- **`entity_key` presence.** Universal and defaulted, so one envelope and one
  wire shape rather than two.
- **The convergence guard.** Universal code, a no-op by construction for `delete`
  types, so the dispatch path carries no class branch.
- **Restore policy.** Every type has a successor state to republish, so
  proposal 11's "non-snapshot message types make this a per-type property" and
  its dropped-row audit requirement both disappear — **contingent on the resync
  sweep**, which is load-bearing and qualified below.

**Still forked**, now driven by one declared field rather than an implicit class:

- **Topic `cleanup.policy`.** Genuinely a policy consequence.
- **Cache-table generation.** *Structural* — a changeset and per-type DDL, or
  none.
- **Bootstrap eligibility.** *Structural* — a runtime mode offered or withheld.

Three of six forks are removed, one of those conditionally. An earlier bullet
here claimed "one table shape, one guard, one upsert"; that **contradicted this
proposal's own retention-class table two sections above** and is withdrawn.
`compact` and `delete` types do not share a storage shape.

The honest claim is a **halved, declared, and boot-validatable** duality. Two of
the three survivors change generated storage and runtime behavior, not merely
configuration, so "the remainder is only policy" is not available as a defense.

Proposal 09 open question 3 is answered rather than dissolved: a type declares
`delete` instead of opting out of a class.

The test matrix loses the snapshot-versus-event axis of the regression gates,
though it gains retention-class validation cases.

## Conflicts this surfaces in proposal 11

Two, neither currently recorded there, both live under any option that keeps
work items:

1. **`kafkaman_cache`'s directive stops being universally true.** Proposal 11's
   schema table marks the cache schema *"rebuild — restore for speed only;
   replay is authoritative."* Replay is authoritative only for a compacted topic.
   For a `delete`-retention type the log has holes by design, so a cache rebuilt
   by replay is not equivalent to a restored one. The selected option makes this
   expressible rather than silently false: a `delete` type has no cache table and
   no bootstrap offer, so the directive holds for every type that has a cache at
   all. Proposal 11 should say so explicitly.
2. **Self-healing depends on a resync sweep that is not yet load-bearing.**
   "The outbox is dropped and the next write supersedes it" recovers a lost row
   only if something re-reads current state and re-enqueues it. Proposal 11
   describes that sweep under business-data restore, but the collapse claimed
   above depends on it existing and on its being able to identify **non-terminal
   work items**, not just current entity state. Without it, a dropped work-item
   outbox row is still permanently lost — exactly the case the collapse claims to
   remove. The sweep is a prerequisite of this proposal, not an adjacent concern.

   **And the sweep cannot be generic for work items.** For a `compact` entity
   type kafkaman owns the cache table and can enumerate it, so the sweep is
   library code. For a `delete` work-item type, "non-terminal" is a predicate over
   the application's own status machine in the application's own table, which
   kafkaman cannot know. The best available shape is a per-type hook the adopter
   implements.

   That makes the unification **policy-level, not mechanism-level**: kafkaman no
   longer branches, but an adopter who wants work-item recovery writes per-type
   sweep code. This is the same kind of cost charged against option 4 above —
   exported complexity — and consistency requires naming it here rather than only
   there. It is much smaller (a sweep hook, not a hand-rolled durable job queue),
   and it does not change the selection, but it is not zero and it should not be
   discovered during a restore.

   The retention class still predicts which shape applies, so the declaration
   remains the single control point even where the work is the adopter's.

## What this does and does not reverse

Under the selected option, **nothing is removed**, so
`entity-first-propagation-model.decision.md` point 1 — *"this does not reopen the
messaging-scope decision's refutation of propagation-only as a library scope;
nothing is removed"* — still stands. The general durable-execution surface is
intact, work items remain first-class, and M4 keeps its purpose. This promotes as
an **amendment** to the entity-first decision.

That is a substantive difference from options 3 and 4, both of which do reverse
it and would need an explicit revision of the messaging-scope decision. It is
also why the selected option can land without a breaking-change compatibility
note, where the others cannot.

Direct transport mode (proposal 03) still needs re-examination: it has no outbox,
so the `(message_type, entity_key)` enqueue lock cannot serialize it. Under the
selected option this is narrower than it was - a direct-mode type defaults to
`delete` with a no-op guard, so only a direct-mode type declaring `compact` is
affected. A direct-mode `compact` type should be rejected unless it supplies an
equivalent key-level outbound serialization mechanism.

## Consequences

Positive:

- Per-type branching disappears from the envelope, the wire shape, the dispatch
  path, and restore policy, and every fork that survives is driven by one
  declared field rather than an implicit class.
- Topic misconfiguration becomes **boot-detectable**, feeding validation
  proposal 11 already proposed but could not supply an input for.
- Non-breaking against M1-M4 by defaulting `entity_key` to `message_id` and the
  retention class to `delete`.
- The library's identity sharpens toward a distributed cache library without
  discarding the durable-execution engine that the scope evidence validates.

Costs and risks:

- The retention class is an unverifiable claim about key cardinality; a wrong
  declaration still produces unbounded growth, now visibly rather than silently.
- A declared field is still a duality. This proposal reduces and localizes it; it
  does not deliver the single-class model its title suggests. **Two of the three
  surviving forks — cache-table generation and bootstrap eligibility — are
  structural, changing generated DDL and runtime behavior rather than only
  configuration.**
- The self-healing collapse is contingent on a resync sweep that does not exist
  yet, must identify non-terminal work items, and **cannot be generic for
  work-item types**, so it becomes a per-type adopter obligation. The unification
  is policy-level, not mechanism-level.
- Universal `entity_key`, even defaulted, means every type carries convergence
  columns it may never use.

## Implementation Open Questions

1. Is the retention class declared on the message type in Rust, in
   `kafkaman.toml`, or both? The type is where it is visible in review; the
   config is where the M2 fail-fast idiom already lives. Both has precedent in
   the per-message retry policy.
2. Does a `delete`-retention type ever need a cache table? The selected option
   says no. If a use case appears that wants local storage of unbounded-key work
   items, that is Postgres growth with no compaction backstop and should be
   refused rather than accommodated.
3. Should the boot check hard-fail or warn when the broker does not expose topic
   configuration? Proposal 11 notes the check degrades to documentation under
   restrictive ACLs; failing closed there would make kafkaman unbootable on
   locked-down clusters.
4. ~~What is the migration story for a deployed non-entity type?~~ **Resolved
   2026-08-13.** Defaulting `entity_key` to `message_id` and the class to
   `delete` makes existing types compile and behave unchanged.
5. ~~Does `entity_key` become a required part of `KafkaMessage` itself?~~
   **Resolved 2026-08-13.** It becomes universal but defaulted, so no type is
   forced to supply one.
