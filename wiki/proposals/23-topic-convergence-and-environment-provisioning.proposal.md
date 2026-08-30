# Topic Convergence and Environment Provisioning

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-24
- Category: Propagation model
- Scope: Closes the deferred boot-time topic validation by making a topic's
  required configuration part of the message descriptor, converged at boot under
  a mode switch; establishes topic rebuild as the answer to repartitioning; and
  fixes where a demonstration provisioner may and may not reach.
- Sources:
  - crates/kafkaman-core/src/lib.rs
  - crates/kafkaman-sqlx/src/lib.rs
  - examples/compose.yaml
  - wiki/compatibility/m5-entity-first-cache-api.compat.md
  - wiki/plans/entity-first-propagation.plan.md
- Related:
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/decisions/topic-convergence-and-rebuild.decision.md
  - wiki/proposals/12-entity-only-message-model.proposal.md
  - wiki/plans/topic-convergence.plan.md

## Context

The example services run on topics whose `cleanup.policy` is `delete`, while
`README.md` states that every in-purview message is *"published to a compacted
Kafka topic"*. Observed directly through Redpanda Console on 2026-08-24:

```
orders     cleanupPolicy=delete   partitions=1
products   cleanupPolicy=delete   partitions=1
```

Nothing in the codebase creates topics — there is no `AdminClient`,
`describe_configs`, or `cleanup.policy` reference anywhere in `crates/` — so the
topics are auto-created by the broker and its default wins.

This is not a new discovery. It is recorded under **Deferred** in both
`m5-entity-first-cache-api.compat.md` and
`m5-entity-first-outbox-supersede.compat.md`, is listed In Scope in
`entity-first-propagation.plan.md`, and that plan states plainly that
*"Boot-time broker topic validation remains deferred after M5."* What is new is
only the evidence that the gap is load-bearing in practice rather than
theoretical.

**Amended 2026-08-25.** This proposal was written against the pre-closeout
wording of those documents. The M5 closeout (`332721a`) removed a second bullet
this proposal originally leaned on — *"Topic/partition mismatch invalidation
path"* — and reframed that behaviour as shipped rather than deferred. The
argument below is unaffected on its main point, because boot-time topic
validation is still Deferred in both notes; what changed is that §3 now
*supersedes* a closed M5 behaviour instead of *completing* an open deferral.

**Why it matters.** A `delete`-retention topic silently breaks the property the
entity-first model rests on. Full snapshots make a compacted topic self-healing
and repeated application a no-op; on a `delete` topic records age out, and a late
or re-bootstrapping consumer cannot rebuild the entity from the single record
compaction would have kept for its key. The cache stops being reconstructible and
nothing reports it.

## Proposal

Four changes, deliberately at four different altitudes.

### 1. The required topic shape belongs on the descriptor

`MessageDescriptor` already carries `topic` and already flows into
`ResolvedConfig::from_config(config, [descriptor()])` at every service boot — so
kafkaman already knows the full set of topics a service touches. It gains the
topic's required *configuration* alongside its name:

```rust
pub struct MessageDescriptor {
    pub message_type: SqlIdentifier,
    pub topic: String,
    pub topic_spec: TopicSpec,
}

pub struct TopicSpec {
    /// Always `Compact` for an in-purview type. Not `compact,delete`: that
    /// still ages records out by time, which defeats rebuild-from-log.
    pub cleanup_policy: CleanupPolicy,
    pub partitions: Option<i32>,
    pub replication_factor: Option<i16>,
}
```

**No per-type declaration is required, and `examples/contracts` does not
change.** Proposal 12 narrowed the purview so that *every* in-purview kafkaman
type is a compact entity snapshot; compaction is therefore a property of the
model, not of any individual contract. `TopicSpec::default()` is compacted, and a
contract overrides only partitioning, and only when it has a reason to.

This is worth stating explicitly because the obvious alternative — a
`fn topic_spec()` each contract implements — invites every implementor to
re-decide something the model has already settled, and lets one of them declare a
non-compacted topic the rest of kafkaman cannot honour.

### 2. Convergence at boot, under a mode switch

The database side of this problem is already solved here: services declare
`CreateOutboxTable` / `CreateReceivedTable` / `CreateCacheTable` in a changelog,
and `migrate()` converges it at boot — idempotent, advisory-locked, recorded in
`changelog_history` with checksums. Topics are the same shape of problem on the
broker side and belong at the same point in `start()`.

```toml
[topics]
mode = "verify"   # default
```

| Mode | Behaviour |
|---|---|
| `verify` | Read topic configs. Fail boot if a topic is missing or its policy is wrong. Never writes. **Default.** |
| `create` | Create missing topics to spec; still fail if an existing one is wrong. Opt-in. |
| `off` | Skip entirely, for clusters that deny metadata reads. Warns at boot so it cannot be silently forgotten. |

`verify` is exactly the deferred validation the plan already specifies, so this
extends that work rather than replacing it.

**Why `verify` and not `create` is the default.** Application principals are
routinely denied `CreateTopics` by ACL, so a library that creates at boot is
unusable in those clusters.

**Where it runs.** After config resolution and before any loop is spawned. A
service that would consume from a wrongly-configured topic must not reach the
point of consuming from it.

**On an unreachable broker.** Fail, after a bounded retry. The earlier plan's
phrasing was "when broker metadata is available", which reads as
degrade-gracefully; that is right for `off`, but a service whose broker is
unreachable at boot cannot do its job anyway, so treating it as a boot failure is
both stricter and simpler than a mode that half-starts.

### 3. Topic rebuild replaces repartitioning

Partition count looked like a decision that had to be right first time, because
repartitioning a keyed compacted topic changes which partition each entity hashes
to — and offsets compare only within a topic-partition, which is exactly what the
guarded upsert assumes never changes.

It is not, and the reason is the entity-first thesis itself: **because every
message is a full snapshot sourced from the owner's state, the log is a derived
artifact rather than the system of record.** Any topic can therefore be rebuilt
from the producer's database. The procedure is to create `products.v2` with the
partition count wanted, republish every entity from state, cut consumers over,
and drop v1. Nothing is repartitioned.

The property that makes this practical is that **cost is bounded by entity count,
not event count**. Republishing 100k products is 100k records however many times
each has changed. An event-sourced log cannot offer this.

**Today this is a wall rather than a door.** The guarded upsert requires topic and
partition to match:

```sql
WHERE {cache}.applied_topic     = EXCLUDED.applied_topic
  AND {cache}.applied_partition = EXCLUDED.applied_partition
  AND EXCLUDED.applied_offset   > {cache}.applied_offset
```

`classify_skipped_cache_apply` detects the move deliberately — the code's own
comment notes that otherwise the row *"would freeze forever with no error and no
metric"* — and raises `Error::CacheOriginMismatch`, which
`received_failure_disposition` classifies as **terminal**, per-row rather than
tripping a breaker. So a v2 republish today makes every entity's first v2 record
fail terminally into the error table. Detection landed; the resolution did not.

The M5 closeout recorded that outcome as finished — *"A topic or partition
mismatch now fails as `Error::CacheOriginMismatch` instead of being silently
ignored"* — which is true, and is precisely the wall. Failing loudly is the right
answer for a misconfigured consumer and the wrong one for a rebuilt topic, and
nothing distinguished them.

**The declared topic is the authorization for that path.** Invalidation must not
be automatic — "accept any new origin" would mean a consumer misconfigured onto
the wrong topic silently resets its cache. But once §1 gives kafkaman a declared
topic per type, the two cases separate cleanly:

- the record's topic differs from the cached one **and is the declared topic** →
  authorized migration: reset the guard, accept the new origin;
- same topic, different partition → **terminal**, unchanged. A declaration says
  which topic is authoritative, not which partition an entity sits on, so an
  in-place repartition — the operation §3 exists to avoid — is not blessed by it;
- a record from a topic the cache has already migrated *off* → ignored as stale,
  so a cutover does not fill the error table with its own stragglers;
- any other origin change → terminal, exactly as today.

Topic convergence is therefore what makes the invalidation path safe: the
declaration is the only thing in the system that can tell a rebuild from a
mistake.

### 4. A provisioner for the example environment

A new binary, `examples/provision`, run once between the infrastructure and the
services:

```
postgres + redpanda  →  provision  →  order + product
```

It creates the two databases, then calls kafkaman's topic convergence in `create`
mode. In compose it is a one-shot service the others gate on with
`condition: service_completed_successfully`.

This replaces `initdb/10-databases.sql` and removes the demo's reliance on broker
auto-creation, so the example finally demonstrates the model it documents — in
tested Rust rather than SQL and shell.

## What this proposal rejects

### The provisioner does not live in `examples/contracts`

That crate's own module documentation forbids it: *"Nothing else belongs in this
crate. Handlers, HTTP surface, and storage are each service's own business; the
contract is only the shape on the wire."* Its `Cargo.toml` deliberately takes the
kafkaman facade **without** the `rdkafka` feature, commenting that "a contracts
crate needs the message vocabulary, not a transport."

There is also a mechanical reason a `provision` feature would not stay optional:
cargo unifies features across a workspace build, so once the provisioner enabled
it, both services would compile with the transport and provisioning path they
never call.

The declaration stays in the contract layer. The runtime is a separate binary.

### The provisioner does not create tables

Tempting — one place that builds everything — and wrong for three reasons.

1. **It would bypass the change engine.** `migrate()` is the M2 change-management
   engine: per-changeset checksums, `applied_by` and context audit,
   advisory-locked concurrency, `changelog_history`. A provisioner creating tables
   either duplicates that or discards it.
2. **Changelogs are per-service and asymmetric.** `order` declares
   `CreateCacheTable(ProductSnapshot)`; `product` declares
   `CreateCacheTable(OrderSnapshot)`. A central provisioner must know both
   services' private schemas — recoupling precisely what two separate databases
   exist to keep apart.
3. **The dependency direction forbids the tidy version.** `contracts` cannot
   depend on the services, because they depend on it.

Tables stay where they are: converged by `migrate()` at each service's own boot.

The line this draws is between *environment* and *schema*. Databases and topics
are environment — they exist before the service and are not its private business.
Tables are schema, owned by the service that reads them.

### The demo does not fix this with a broker default

`--set redpanda.log_cleanup_policy=compact` in `compose.yaml` is one line and
would make the symptom disappear. It is rejected because it hides the requirement
in a broker flag no reader connects to the README's claim, applies bluntly to
every auto-created topic, and leaves the library gap — every other kafkaman
deployment keeps the same silent failure.

## Resolved questions

1. **Partition count on `create`: refuse to guess.** `create` requires partitions
   declared explicitly and fails boot if they are not. §3 makes a wrong choice
   recoverable rather than permanent, but a rebuild is still a real operation with
   a cutover window. Being explicit costs one config line; being wrong costs a
   migration.
2. **`verify` warns on partition-count drift, and fails only on cleanup policy.**
   Failing would turn an intentional, already-completed repartition into a
   fleet-wide boot failure. Warning keeps the signal without the outage.
3. **`off` is permitted**, because clusters that deny metadata reads exist and
   should not be locked out. It warns at boot so the gap cannot be reopened
   silently.
4. **The provisioner lives under `examples/`**, not `scripts/`. It is
   example-specific, and AGENTS.md's Library Pack keeps runnable examples there.

## Open questions

1. **Cutover mechanics for a rebuild.** The producer should publish to v2 only
   after the republish completes, and consumers switch after that; otherwise there
   is a window in which neither topic is authoritative. Whether kafkaman offers a
   supported procedure or only the invalidation primitive is undecided.
2. **Live writes during republish.** A bulk republish could emit a stale snapshot
   after a fresh live write. Key-serialized outbound supersede should cover it
   *provided* the republish goes through the same outbox path rather than around
   it — needs confirming against the supersede implementation.
3. **A positive state-sourced republish API does not exist.** The negative half
   already shipped — `Replay::outbox` returns `Err(UnsafeOutboxReplay)`
   unconditionally — but a rebuild needs a supported way to re-read current
   domain state and enqueue it through the normal supersede path, and that
   surface is still deferred. (An earlier draft of this proposal wrongly listed
   the *rejection* as deferred; it is not.)

## Verification

- The example topics report `cleanup.policy=compact` in Redpanda Console after
  `just examples demo`, where they currently report `delete`.
- **Deliberately misconfigure and confirm the failure.** Pre-create `products`
  with `cleanup.policy=delete`, then boot `order` in `verify` mode: it must refuse
  to start, naming the topic and the offending policy. Catching this is the entire
  point, so it must be demonstrated rather than assumed.
- A service booted against a topic configured `compact,delete` is rejected, not
  accepted — the policy must be `compact` alone.
- `mode = "create"` with no declared partition count fails boot, naming the
  missing setting.
- `mode = "create"` against a broker that denies `CreateTopics` fails with an
  error naming the ACL problem, not a generic broker error.
- A rebuild round trip: republish every entity to a v2 topic with a different
  partition count, and confirm consumer caches converge rather than filling the
  error table with `CacheOriginMismatch`.
- `cargo test --workspace --all-features` stays green with every fixture
  provisioning its own compacted topics.
