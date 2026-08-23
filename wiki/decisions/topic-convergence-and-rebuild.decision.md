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

9. **The declared topic authorizes cache-origin invalidation.** When a record's
   origin differs from the cache's `applied_topic` / `applied_partition` *and the
   record's topic is the declared one*, that is a migration: reset the guard and
   accept the new origin. Any other origin change stays terminal, as today.

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
`applied_partition`, so a topic move otherwise wedges the row permanently.
`classify_skipped_cache_apply` already detects this and raises
`Error::CacheOriginMismatch` — terminal, per-row — precisely so it cannot freeze
"with no error and no metric". What was missing was a safe way to *resolve* it,
and the declared topic from decision 1 supplies the authorization that makes
automatic invalidation safe rather than reckless.

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

No `AdminClient`, `describe_configs`, or `cleanup.policy` reference exists
anywhere in `crates/`. The gap was already recorded under **Deferred** in
`m5-entity-first-cache-api.compat.md` and
`m5-entity-first-outbox-supersede.compat.md`, and
`entity-first-propagation.plan.md` records "Boot-time broker topic validation
remains pending" as of 2026-08-13.

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
