# Topic Convergence and Rebuild

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-24
- Category: Propagation model
- Scope: Fixes how an entity topic's required configuration is declared,
  converged, and repaired: the descriptor carries the spec, boot verifies it,
  rebuild replaces repartitioning, and the declared topic authorizes cache-origin
  invalidation. Also fixes the environment/schema boundary for provisioning.
- Sources:
  - crates/kafkaman-core/src/lib.rs
  - crates/kafkaman-sqlx/src/lib.rs
  - wiki/proposals/13-topic-convergence-and-environment-provisioning.proposal.md
- Related:
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/proposals/12-entity-only-message-model.proposal.md
  - wiki/plans/topic-convergence.plan.md

## Decisions

1. **A topic's required configuration is part of its `MessageDescriptor`, not of
   each contract's `KafkaMessage` impl.** `TopicSpec` defaults to
   `cleanup.policy=compact` alone. Since proposal 12 narrowed the purview so that
   every in-purview type is a compact entity snapshot, compaction is a property of
   the model rather than of any individual contract, and contracts declare nothing
   unless they are overriding partitioning.

2. **`cleanup.policy` must be `compact` alone.** `compact,delete` is rejected: it
   still ages records out by time, which defeats rebuild-from-log and therefore
   the entity-first guarantee.

3. **Topic configuration is converged at service boot, before any loop is
   spawned,** at the same point in `start()` where `migrate()` converges the
   database changelog. Modes are `verify` (default; read-only; fails boot on a
   missing topic or wrong policy), `create` (opt-in; creates to spec), and `off`
   (escape hatch for clusters denying metadata reads; warns at boot).

4. **`verify`, not `create`, is the default.** Application principals are
   routinely denied `CreateTopics` by ACL, so creation-at-boot cannot be the
   baseline behaviour of a library.

5. **`create` refuses to guess a partition count.** It requires one declared and
   fails boot otherwise.

6. **`verify` fails on cleanup policy and only warns on partition-count drift.**
   Failing would turn an intentional, completed repartition into a fleet-wide
   boot failure.

7. **An unreachable broker at boot is a boot failure, after bounded retry** — not
   a degraded start. A service whose broker is unreachable cannot do its job.

8. **Repartitioning is never performed. Topics are rebuilt.** To change partition
   count, create a new topic at the new count, republish every entity from the
   owner's state, cut consumers over, drop the old topic.

9. **The declared topic authorizes cache-origin invalidation, and only across a
   topic change.** Four cases hide behind a guarded upsert that wrote nothing:

   | Cached origin | Incoming record | Outcome |
   |---|---|---|
   | same topic, same partition | — | `Ignored`, the guard working |
   | same topic, different partition | — | **terminal**, as before |
   | different topic | on the declared topic | **migrate**: reset the guard |
   | different topic | not on the declared topic | `Ignored` if the cache is already on the declared topic, otherwise terminal |

   **Refined 2026-08-24 during implementation.** The first form of this decision
   said any origin change authorized by the declared topic was a migration, which
   would also have blessed an *in-place* partition change — the one operation
   decision 8 exists to forbid. A declaration says which topic is authoritative,
   not which partition an entity belongs on, so a same-topic partition move stays
   terminal and `cache_apply_halts_when_an_entity_changes_partition` still holds.

   The fourth row is a cutover detail the first form missed: once a cache has
   moved to the rebuilt topic, records still draining out of the received table
   from the retired one are merely stale. Failing them would fill the error table
   for the length of the migration.

10. **Provisioning covers environment, never schema.** Databases and topics are
    environment and may be provisioned externally; tables are schema, owned by the
    service that reads them and converged only by `migrate()` at its own boot.

11. **A contracts crate never gains a transport or a provisioning runtime.**
    Declaration lives in the contract layer; the runtime is a separate binary.

## Why

The entity-first model already states that snapshots are full state, keyed by
entity, on a compacted topic. Nothing enforced the last clause. Topics were
auto-created by the broker, and the example was observed on 2026-08-24 running
with `cleanup.policy=delete` on both `products` and `orders` — silently unable to
support the rebuild the model promises.

Decision 8 follows from the thesis rather than working around it. Because every
message is a full snapshot sourced from the owner's state, **the log is a derived
artifact, not the system of record**, so any topic can be rebuilt from the
producer's database at a cost bounded by entity count rather than event count.
That turns partition count from an irreversible choice into a recoverable one,
and it is a property an event-sourced log cannot offer.

Decision 9 exists because the guarded upsert compares `applied_topic` and
`applied_partition`, so a topic move otherwise wedges the row permanently. As M5
shipped, `classify_skipped_cache_apply` detected this and raised
`Error::CacheOriginMismatch` — terminal, per-row — precisely so it could not
freeze "with no error and no metric". That is the right answer for a
misconfigured consumer and the wrong one for a rebuilt topic. What was missing
was a way to tell them apart, and the declared topic from decision 1 supplies
it, which is what makes automatic invalidation safe rather than reckless.

Decision 10 draws the line where the existing design already draws it: changelogs
are per-service and asymmetric (`order` declares
`CreateCacheTable(ProductSnapshot)`, `product` the mirror), so a central table
provisioner would have to know both services' private schemas — recoupling
exactly what two separate databases exist to keep apart.

## Alternatives considered

- **A broker-level default** (`redpanda.log_cleanup_policy=compact`). Rejected:
  hides the requirement where no reader connects it to the model, applies bluntly
  to every auto-created topic, and leaves every non-demo deployment exposed.
- **`fn topic_spec()` on `KafkaMessage`, implemented per contract.** Rejected:
  invites every implementor to re-decide a settled question and permits declaring
  a topic kafkaman cannot honour.
- **Creation as the default mode.** Rejected on ACLs (decision 4).
- **Automatic cache-origin invalidation on any origin change.** Rejected: a
  consumer misconfigured onto the wrong topic would silently reset its cache.
- **A provisioner that also creates tables**, giving one place that builds
  everything. Rejected on decision 10's reasoning.
- **A `provision` feature on the contracts crate.** Rejected: cargo unifies
  features across a workspace build, so the services would compile the transport
  and provisioning path they never call.

## Consequences accepted

- `MessageDescriptor` gains a public field — breaking for anyone constructing it
  literally. `MessageDescriptor::new` keeps working and defaults the spec.
- `kafkaman-rdkafka` gains an `AdminClient` dependency surface.
- Every test fixture that relied on broker auto-creation must provision compacted
  topics: `kafkaman-test::Harness`, `tests/durable-send`,
  `tests/distributed-cache`.
- Boot gains a broker round trip before any loop starts, so a broker outage now
  fails boot where it previously produced a service that started and retried.
- `off` can reopen the gap by configuration. Accepted, mitigated by warning.

## Evidence

Observed 2026-08-24 via Redpanda Console against the example stack:

```
orders     cleanupPolicy=delete   partitions=1
products   cleanupPolicy=delete   partitions=1
```

At the time of that reading no `AdminClient`, `describe_configs`, or
`cleanup.policy` reference existed anywhere in `crates/`. The gap was already
recorded under **Deferred** in both `m5-entity-first-cache-api.compat.md` and
`m5-entity-first-outbox-supersede.compat.md`, and
`entity-first-propagation.plan.md` records that "Boot-time broker topic
validation remains deferred after M5."

Execution and verification gates are in
[plans/topic-convergence.plan.md](../plans/topic-convergence.plan.md).

## Revisit if

- A supported Kafka deployment cannot express `cleanup.policy=compact` alone, or
  a broker's compaction semantics change such that `compact,delete` becomes safe
  for rebuild.
- Entity counts grow to where a full republish is no longer operationally
  feasible, which would make decision 8 unaffordable and put partition count back
  in the irreversible column.
- Boot-time broker access proves unavailable often enough that `off` becomes the
  common case rather than the exception, which would mean verification has to move
  somewhere other than boot.
