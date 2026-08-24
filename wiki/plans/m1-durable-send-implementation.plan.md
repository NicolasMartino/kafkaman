# M1 Implementation Plan - Durable Send (code-level)

- Document Class: Plan
- Status: Completed
- Date: 2026-06-20
- Category: Implementation plan
- Scope: Concrete engineering plan for the [first-PoC](first-poc-outbox-publisher.plan.md) durable-send slice: crate layout, dependencies, core types, SQL/DDL, minimal migrate engine, transactional enqueue, relay claim/lease mechanics, publisher boundary, `Harness` seed, Axum example, and an ordered outside-in TDD task list mapped to the PoC gates. Send side only; consume/inbox, retry/DLQ, full config/env maturity, and `kafkaman-axum` are out of scope.
- Sources:
  - wiki/plans/first-poc-outbox-publisher.plan.md
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/decisions/library-test-strategy.decision.md
  - wiki/decisions/consumer-test-tooling.decision.md
- Related:
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md

## How To Read This

This plan is the **how**. The [first-PoC plan](first-poc-outbox-publisher.plan.md)
is the **what/why** and owns the durable-send verification gates. Signatures below
are implementation targets for M1, not public API promises beyond the promoted M1
spec.

Review fixes already incorporated here:

- relay claim and publish marking use an explicit `claim_id` + lease, not an
  impossible "publish while holding one DB tx" model.
- changesets are object-safe in M1; no `async fn` on `dyn Changeset`.
- enqueue and claim APIs carry typed message-table identity.
- SQL identifiers are validated/quoted through a dedicated type.
- the `Harness` has a Docker-free capturing-publisher default and an opt-in
  Redpanda/full-loop mode.
- publish failure has a durable requeue primitive.
- verification checks both "record was published" and "row was marked Published".

## Workspace

```text
kafkaman/
├── Cargo.toml                  # workspace resolver = "2"
├── crates/
│   ├── kafkaman-core/           # pure types, traits, identifiers
│   ├── kafkaman-sqlx/           # migrate, enqueue, claim, mark
│   ├── kafkaman-rdkafka/        # Kafka publisher implementation
│   ├── kafkaman-worker/         # relay_once/run, generic over publisher trait
│   ├── kafkaman-test/           # Harness seed
│   └── kafkaman/                # facade re-export crate (optional in M1)
├── apps/
│   └── axum-outbox/             # runnable Axum + SQLx durable-send example
└── tests/
    └── durable_send.rs          # workspace-level tests that use kafkaman-test
```

`kafkaman-test` is a normal crate in the workspace, but its **default feature set
must stay Docker-free**. It depends on `core`, `sqlx`, and `worker` with a
capturing publisher by default. `rdkafka`, Redpanda helpers, and `testcontainers`
live behind an explicit full-loop feature so consumers do not pay the broker build
cost for DB-only handler/send tests.

White-box unit tests stay inside the crates they test. The dogfooding rule means
"prefer the Harness for behavior at or above the Harness boundary," not "force all
DDL and SQL-builder tests through the Harness."

## Dependencies

- `tokio` (`rt-multi-thread`, `macros`), `sqlx` (`runtime-tokio`, `postgres`,
  `uuid`, `time`, `json`), `serde`, `serde_json`, `uuid`, `time`, `thiserror`,
  `tracing`.
- `rdkafka` only in `kafkaman-rdkafka` and full-loop test features.
- `testcontainers` only in workspace integration tests / full-loop feature.
- `axum` + `axum-sqlx-tx` only in `apps/axum-outbox`. M1 does **not** build
  `kafkaman-axum`; the example calls `kafkaman-sqlx::enqueue(...)` directly.

## kafkaman-core

```rust
pub struct SqlIdentifier(String);
```

`SqlIdentifier` validates generated schema/table parts before they reach SQL
formatting: ASCII lowercase letters, digits, and underscores only; starts with a
letter; bounded length; reserved words rejected or escaped by construction. SQLx
cannot bind identifiers, so all identifier interpolation goes through this type.

```rust
pub struct MessageDescriptor {
    pub message_type: SqlIdentifier,
    pub topic: String,
}

pub trait KafkaMessage: Serialize {
    const MESSAGE_TYPE: &'static str;
    const TOPIC: &'static str;

    fn partition_key(&self) -> Option<String> {
        None
    }
}

pub struct Envelope<P> {
    pub message_id: Uuid,
    pub idempotency_key: Option<String>,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub headers: BTreeMap<String, String>,
    pub payload: P,
    pub occurred_at: OffsetDateTime,
}
```

The topic and table identity come from `P: KafkaMessage`, not from arbitrary
runtime strings. `idempotency_key` is carried now for envelope compatibility, but
consumer-side dedup is M3.

```rust
pub enum OutboxStatus {
    Pending,
    Publishing,
    Published,
    Failed,
}

pub struct OutboxRow {
    pub message_id: Uuid,
    pub status: OutboxStatus,
    pub attempts: i32,
    pub next_attempt_at: OffsetDateTime,
    pub last_error: Option<String>,
    pub claim_id: Option<Uuid>,
    pub claimed_by: Option<String>,
    pub claim_expires_at: Option<OffsetDateTime>,
    pub topic: String,
    pub partition_key: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub payload: serde_json::Value,
}
```

State machine for M1:

- `Pending -> Publishing`: claim lease acquired; `attempts += 1`; `claim_id`,
  `claimed_by`, and `claim_expires_at` set.
- `Publishing -> Published`: Kafka ack received and `mark_published` succeeds
  with the same `claim_id`.
- `Publishing -> Pending`: publish failed or lease expired and the row is
  reclaimed.
- `Publishing -> Publishing`: another worker reclaims an expired lease with a new
  `claim_id`.
- `Failed` is reserved for manual/admin use until the M4 retry/DLQ policy.

## Object-safe M1 Changesets

```rust
pub trait Changeset: Send + Sync {
    fn version(&self) -> i64; // sequential integer, not timestamp
    fn name(&self) -> &str;
    fn build(&self, cfg: &ResolvedConfig, b: &mut ChangeBuilder) -> Result<()>;
}
```

M1 changesets are synchronous SQL builders. `migrate()` executes the generated SQL
inside transactions. This avoids `async fn` on `dyn Changeset`; if future
changesets need asynchronous inspection, use boxed futures or an enum deliberately
instead of accidentally making the trait non-object-safe.

`ResolvedConfig` in M1 is a small struct containing schema name, lease duration,
poll interval, fixed retry delay, and message descriptors. The full config loader
is M2.

## kafkaman-sqlx

### Minimal `migrate()`

```rust
pub async fn migrate(
    pool: &PgPool,
    cfg: &ResolvedConfig,
    changesets: &[Box<dyn Changeset>],
) -> Result<()>;
```

M1 behavior:

- `CREATE SCHEMA IF NOT EXISTS <cfg.schema>`.
- ensure `<schema>.changelog_history(version BIGINT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`.
- apply changesets ordered by `version()`.
- for each missing version, build SQL, execute it in one tx, insert history row,
  commit.
- second run is a no-op.
- no checksums/audit details/operational changesets yet; those remain M2.

Built-in M1 changesets:

```rust
pub struct InitSchema;
pub struct CreateOutboxTable {
    pub descriptor: MessageDescriptor,
}
```

### Per-type outbox DDL

```sql
CREATE TABLE <schema>.outbox_<message_type> (
    message_id UUID PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'Pending',
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    claim_id UUID,
    claimed_by TEXT,
    claim_expires_at TIMESTAMPTZ,
    topic TEXT NOT NULL,
    partition_key TEXT,
    correlation_id UUID NOT NULL,
    causation_id UUID,
    headers JSONB NOT NULL DEFAULT '{}',
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX <idx> ON <schema>.outbox_<message_type>
    (status, next_attempt_at, claim_expires_at, created_at);
```

M1 may add a `CHECK (status IN (...))` if it is cheap. It is not required for the
PoC, but the Rust enum and SQL strings must stay centralized.

### Enqueue

```rust
pub async fn enqueue<P>(
    tx: &mut PgTransaction<'_>,
    cfg: &ResolvedConfig,
    evt: &Envelope<P>,
) -> Result<()>
where
    P: KafkaMessage + Serialize;
```

`enqueue` derives `MessageDescriptor` from `P`, resolves the validated
`OutboxTable`, and inserts the row into the caller's transaction. Atomicity is
only: "caller commits business write + outbox row together." It does not imply
Kafka publication before commit.

### Claim and mark

```rust
pub struct Claim {
    pub id: Uuid,
    pub worker_id: String,
    pub lease_until: OffsetDateTime,
}

pub async fn claim_batch(
    tx: &mut PgTransaction<'_>,
    table: &OutboxTable,
    worker_id: &str,
    lease_for: Duration,
    limit: i64,
) -> Result<Vec<ClaimedOutboxRow>>;

pub async fn mark_published(
    pool: &PgPool,
    table: &OutboxTable,
    message_id: Uuid,
    claim_id: Uuid,
) -> Result<MarkOutcome>;

pub async fn mark_publish_failed(
    pool: &PgPool,
    table: &OutboxTable,
    message_id: Uuid,
    claim_id: Uuid,
    error: &str,
    retry_at: OffsetDateTime,
) -> Result<MarkOutcome>;
```

Claim query:

```sql
SELECT ...
FROM <table>
WHERE
    (status = 'Pending' AND next_attempt_at <= now())
    OR (status = 'Publishing' AND claim_expires_at <= now())
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT $limit;

UPDATE <table>
SET status = 'Publishing',
    attempts = attempts + 1,
    claim_id = $claim_id,
    claimed_by = $worker_id,
    claim_expires_at = now() + $lease_for
WHERE message_id IN (...);
```

`mark_published` updates only when `status = 'Publishing' AND claim_id = $claim_id`.
A stale relay that lost its lease must not be able to mark a row that another
worker has reclaimed. `mark_publish_failed` also validates `claim_id`, writes
`last_error`, clears claim fields, sets `status = 'Pending'`, and sets
`next_attempt_at` to the fixed M1 retry delay. M4 replaces that fixed delay with
the real retry/backoff/DLQ policy.

## kafkaman-rdkafka

```rust
pub struct RdkafkaPublisher {
    producer: FutureProducer,
}

impl Publisher for RdkafkaPublisher {
    async fn publish(&self, row: &ClaimedOutboxRow) -> Result<PublishAck>;
}
```

Kafka record mapping:

- `topic = row.topic`
- `key = row.partition_key`
- payload = serialized JSON bytes from `row.payload`
- headers = envelope headers plus correlation/causation/message id

The publisher performs no DB writes. It surfaces delivery ack/error only.

## kafkaman-worker

```rust
pub trait Publisher: Send + Sync {
    async fn publish(&self, row: &ClaimedOutboxRow) -> Result<PublishAck>;
}

pub async fn relay_once<P: Publisher>(
    pool: &PgPool,
    publisher: &P,
    table: &OutboxTable,
    cfg: &RelayConfig,
) -> Result<RelayStats>;

pub async fn run<P: Publisher>(
    pool: PgPool,
    publisher: P,
    cfg: RelayConfig,
    shutdown: CancellationToken,
) -> Result<()>;
```

Relay model:

1. short DB tx claims rows and commits `Publishing` leases.
2. publish each claimed row outside that claim tx.
3. on ack, call `mark_published(message_id, claim_id)`.
4. on publish error, call `mark_publish_failed(message_id, claim_id, error, retry_at)`.

Crash windows:

- crash after business commit before relay publish: row is `Pending`; next relay
  publishes it.
- crash after claim before publish: row is `Publishing`; lease expiry allows
  reclaim and publish.
- crash after Kafka ack before mark: row is `Publishing`; lease expiry allows
  republish. This documents the at-least-once duplicate window.

## kafkaman-test Harness Seed

```rust
pub enum HarnessPublisher {
    Capturing,
    #[cfg(feature = "redpanda")]
    Redpanda { broker_url: String },
}

pub struct Harness { /* pool, ephemeral schema, cfg, publisher */ }

impl Harness {
    pub async fn connect(database_url: &str) -> Result<Self>; // Capturing publisher

    #[cfg(feature = "redpanda")]
    pub async fn connect_redpanda(database_url: &str, broker_url: &str) -> Result<Self>;

    pub async fn enqueue<P>(&self, evt: &Envelope<P>) -> Result<()>
    where
        P: KafkaMessage + Serialize;

    pub async fn relay_once<P: KafkaMessage>(&self) -> Result<RelayStats>;
    pub async fn assert_status<P: KafkaMessage>(&self, message_id: Uuid, expected: OutboxStatus);
    pub async fn outbox_row<P: KafkaMessage>(&self, message_id: Uuid) -> Result<OutboxRow>;
    pub async fn published_on(&self, topic: &str) -> Vec<PublishedRecord>; // capture or Redpanda
}
```

The Harness never auto-starts the background loop. Tests drive `relay_once()`
explicitly. The default path needs only Postgres and a capturing publisher; the
Redpanda path is opt-in full-loop coverage.

## Implementation Tasks

Each behavior-level task starts by writing or extending a failing Harness test.
Primitive layers may also add white-box tests where the Harness is built from the
thing being tested.

0. **Workspace + stubs.** Create crates, features, and examples. `cargo check
   --workspace` fails only because APIs are intentionally missing for the first
   behavior test.
1. **Headline failing test.** In `tests/durable_send.rs`: enqueue in a SQL tx,
   commit, run `relay_once::<OrderCreated>()`, assert one published record and row
   `Published`.
2. **Core types.** Implement `SqlIdentifier`, `KafkaMessage`, `MessageDescriptor`,
   `Envelope`, statuses, row structs, and object-safe `Changeset`.
3. **Migrate engine + DDL.** Implement `InitSchema` and `CreateOutboxTable`.
   White-box tests cover identifier validation and rendered DDL; Harness test
   covers idempotent convergence.
4. **Enqueue + claim + mark primitives.** Add white-box SQLx tests for
   enqueue-in-caller-tx, `FOR UPDATE SKIP LOCKED`, claim token/lease behavior,
   stale mark rejection, publish failure requeue, and lease-expiry reclaim.
5. **Worker with capturing publisher.** Implement `relay_once` and `run` generic
   over `Publisher`; green the headline Harness test without Kafka.
6. **rdkafka publisher + full-loop feature.** Add Redpanda/testcontainers or
   docker-compose-gated test proving the same Harness path can publish to a real
   broker.
7. **Harness seed.** Flesh out ephemeral schema setup, PoC changelog, capturing
   publisher assertions, and Redpanda feature path.
8. **Axum example.** Runnable example with local `changelog.rs`, two-phase `main`
   (`migrate(pool, changelog())` then spawn `relay::run`), and an endpoint that
   writes business state + `enqueue(&mut tx, ...)` in one transaction.
9. **Crash gates through the Harness.** (a) commit before relay publish -> record
   still published; (b) ack before mark -> row republished after lease expiry.

## Verification Gates

- `cargo check --workspace` and `cargo nextest run` pass.
- `migrate()` is idempotent.
- `CreateOutboxTable` generalizes to at least two message types while only one is
  exercised end-to-end.
- normal delivery asserts **both** a published/captured record and row `Published`.
- crash between commit and publish proves no loss.
- crash after ack before mark proves at-least-once republish after lease expiry.
- stale claim cannot mark a row after another worker has reclaimed it.
- publish error records `last_error`, increments attempts through claim, clears the
  claim, and requeues with `next_attempt_at`.

## What Closes This Plan

All gates green. Promote only the M1-proven subset to specs: envelope shape,
validated identifier/table naming, per-type outbox DDL, the M1 `migrate()` subset,
transactional enqueue, relay claim/lease semantics, and the send-side Harness API.
Do **not** promote the full change-engine/config/retry contracts until their later
milestones validate them.

## Post-Review Fix Pass (2026-06-21)

Two implementation reviews
([review](../reviews/m1-durable-send-implementation-review.reference.md),
[re-review](../reviews/m1-durable-send-implementation-rereview.reference.md))
were resolved after the initial completion. Outcome:

- **Redpanda / full-loop path implemented** (was the standing scope gap). The
  plan's broker-backed gate now exists as a `redpanda`-feature-gated test that
  starts Redpanda as a testcontainer, publishes through `RdkafkaPublisher` via
  `Harness::connect_redpanda` (new `HarnessPublisher::Redpanda` variant), and
  asserts the consumed record's payload, key, and `kafkaman-*` headers. It is a
  testcontainer gate rather than docker-compose, but proves the same path.
- **Status SQL centralized** through `OutboxStatus` helpers; **lease and retry
  scheduling moved to the database clock**; **worker `run()` no longer dies** on a
  transient relay error (logs via `tracing` and continues); **`migrate()` is
  advisory-locked** against concurrent boots; **`idempotency_key` is durable** and
  forwarded, with an additive `AddIdempotencyKey` upgrade changeset for
  pre-existing tables; **reserved `kafkaman-` headers are rejected** at enqueue;
  the **Harness registration race** is closed; **strict clippy** passes.
- **Coverage:** workspace line coverage is gated at 80% via `cargo llvm-cov`
  (`just test coverage`); the example now lives in `apps/` so it counts toward the
  total. See the compatibility note for schema/API impact.
  **Reversed 2026-08-24.** `apps/` was renamed back to `examples/` by the
  [two-service distributed cache example](two-service-distributed-cache-example.plan.md),
  which `cargo llvm-cov` excludes — so the coverage total now measures `crates/`
  alone. Deliberate: the gate should describe the library, not be propped up by
  demonstration code.

Schema and API impact is recorded in
[compatibility/m1-durable-send-schema-and-api-changes.compatibility.md](../compatibility/m1-durable-send-schema-and-api-changes.compatibility.md).
