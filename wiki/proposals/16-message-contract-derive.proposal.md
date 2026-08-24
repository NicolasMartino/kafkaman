# Message Contract Derive

- Document Class: Proposal
- Status: Proposed
- Date: 2026-08-26
- Category: Developer experience
- Scope: Proposes `#[derive(KafkaMessage)]` to remove the hand-written contract
  boilerplate every message type carries today, and fixes the two rules that
  matter more than the macro itself.
- Sources:
  - crates/kafkaman-core/src/message.rs
  - crates/kafkaman-sqlx/src/outbox_enqueue.rs
  - examples/contracts/src/lib.rs
  - Design session, 2026-08-26
- Related:
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/consumer-test-tooling.decision.md
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/proposals/15-dispatch-concurrency-and-middleware.proposal.md
- Promotion Target: a decision fixing the attribute surface and the
  never-infer-the-wire-contract rule.
- Revised: 2026-08-27 (the `partition_key` finding verified against the enqueue
  path; it inverts, and the prerequisite it uncovered is open question 4)

## Context

`KafkaMessage` is small — two associated constants, a required `entity_key`, an
optional `partition_key` (`crates/kafkaman-core/src/message.rs:82-102`). It is
also written out by hand **18 times** across `crates/`, `examples/` and `tests/`.

The cost is not the typing. `examples/contracts/src/lib.rs:3-6` documents the
actual hazard, in a doc comment that exists because someone already thought about
it:

> a duplicated `KafkaMessage` impl drifts silently: a changed `TOPIC`,
> `MESSAGE_TYPE`, or `entity_key` is a runtime mismatch with no compile error,
> and the symptom is a cache that simply never converges.

`wiki/decisions/message-consumption-and-handler-model.decision.md:194` has
assumed a derive since M3 — "The payload type implements a kafkaman
`KafkaMessage` trait (via derive)" — and `:107-108` builds on it. It was never
built. The builder work
made the rest of the surface declarative, which leaves this as the most visible
remaining piece of hand-written ceremony.

## Proposal

```rust
#[derive(KafkaMessage, Serialize, Deserialize)]
#[kafkaman(message_type = "product_snapshot", topic = "products")]
pub struct ProductSnapshot {
    #[kafkaman(entity_key)]
    pub product_id: Uuid,
    pub name: String,
    pub price_cents: i64,
}
```

- `message_type` and `topic` are **required** attributes.
- `#[kafkaman(entity_key)]` marks exactly one field; the generated
  `entity_key(&self) -> String` uses its `ToString`.
- `#[kafkaman(partition_key)]` optionally marks a field for `partition_key`.
- A new `crates/kafkaman-macros` proc-macro crate, re-exported from the facade so
  an application never names a second kafkaman crate — the same rule the facade
  already applies to `rdkafka` and `axum`.

## The two rules that matter more than the macro

### 1. Never infer the wire contract from the type name

`product_snapshot` is the snake_case of `ProductSnapshot`, so deriving
`MESSAGE_TYPE` from the identifier would save a line and look clever.

It would also mean **renaming a Rust struct silently changes the wire contract**
— consumers stop matching, the cache stops converging, and nothing fails to
compile. That is precisely the failure class this codebase is organised to
prevent, and the same reasoning that
`wiki/decisions/schema-and-change-management.decision.md` uses to forbid editing
a table template in place: a change that is invisible to the tooling is worse
than a change that is verbose.

The same applies to `topic`.

### 2. The derive is sugar, never load-bearing

Hand-written impls must keep compiling and keep being supported.
`wiki/decisions/consumer-test-tooling.decision.md:80` states this as a standing
principle — "macros are opt-in sugar over explicit APIs, never load-bearing
magic" — and records that it is why `#[kafkaman::handler]` was dropped.

Concretely: the derive must generate exactly the impl a user would write, with
no hidden registration, no inventory-style linker tricks, and no behaviour that
is unavailable to someone who writes the impl out.

## A finding this settles, and the prerequisite it uncovered

Both example contracts define `partition_key` returning the same value as
`entity_key` (`examples/contracts/src/lib.rs:79-85` and `:101-107`):

```rust
fn partition_key(&self) -> Option<String> { Some(self.product_id.to_string()) }
fn entity_key(&self) -> String { self.product_id.to_string() }
```

But the trait defaults `partition_key` to `None`, and its doc comment says that
is safe "because the publisher falls back to the entity key; see
`OutboxRow::record_key` for why that fallback is load-bearing rather than a
convenience" (`crates/kafkaman-core/src/message.rs:89-91`).

**Verified 2026-08-27 against the enqueue path. The answer is not the expected
one: redundant for routing, load-bearing for headers.**

For routing the comment is exactly right. `record_key` is
`self.partition_key.as_deref().or(self.entity_key.as_deref())`
(`crates/kafkaman-core/src/rows.rs:198`), the publisher keys the record from it
and from nothing else (`crates/kafkaman-rdkafka/src/publisher.rs:74`), and
`crates/kafkaman-core/src/tests/rows.rs:56-69` pins the fallback. Deleting those
four lines per type would not move a single record to a different partition.

But the declaration has a second effect with nothing to do with routing.
`crates/kafkaman-sqlx/src/outbox_enqueue.rs:54` attaches the `kafkaman-entity-key`
header — carried for foreign consumers that cannot deserialize the typed payload
— **only when the declared partition key differs from the entity key**. Declaring
them equal, as both examples do, suppresses it. Returning `None` does not:
`None != Some(entity_key)` is true, so under a derive that omits `partition_key`
both example types would begin emitting a header they do not emit today, stating
exactly what the record key already says.

That condition is the actual defect here. It tests the **declared** partition key
where the header's own purpose is about the **effective** record key, and the
fallback makes those two different things: a type declaring nothing is still
published under its entity key. `RegionalOrder` shows the case the header exists
for — declared key `eu-west`, entity key `order-77`, header present and asserted
at `tests/durable-send/tests/redpanda_full_loop/publish_and_consume.rs:103`.
A type whose keys coincide is not that case, whether it says so or stays silent.

So the ordering matters, and it is the opposite of what this section originally
implied. The derive should still omit `partition_key` unless a field is marked —
but **only after** the enqueue condition is narrowed to compare the effective
record key. Land the derive first and it is a silent wire change for every
existing type that declares the two keys equal, not a cleanup. Narrow the
condition first and the four lines become genuinely removable, which is what they
looked like all along.

## Consequences

Positive:

- One place per type states the wire contract, so drift between `TOPIC`,
  `MESSAGE_TYPE` and `entity_key` becomes impossible rather than merely unlikely.
- `examples/contracts` becomes a readable statement of the two entity shapes
  rather than 60 lines of impl.
- Removes a `partition_key` from every entity type whose keys coincide — but only
  once the enqueue header condition above is narrowed; before that it is a wire
  change, not a removal.

Costs and risks:

- A new proc-macro crate, and the first `syn` / `quote` / `proc-macro2`
  dependencies in the workspace. They are ubiquitous, but they are new.
- Proc-macro errors are worse than hand-written ones unless deliberately
  invested in. A missing `entity_key`, a missing `topic`, or two marked fields
  must produce a message that names the attribute and the fix — `trybuild` cases
  for each.
- 18 call sites to convert, several in test fixtures where the explicit impl is
  arguably clearer as documentation. Converting them is not obligatory; the rule
  above says both forms stay valid.

## Alternatives Considered

- **Infer `message_type` from the type name**, with an attribute to override.
  Rejected above: it makes a rename a silent wire-contract change.
- **A declarative `kafka_message! { .. }` macro** rather than a derive. Rejected:
  a derive composes with `#[derive(Serialize, Deserialize)]` on the same struct,
  which is how every one of these types is already written.
- **Do nothing.** Defensible — the trait is four items and the impls are
  mechanical. The argument against is the drift comment in
  `examples/contracts/src/lib.rs`, which shows the hazard was noticed and
  documented rather than removed.

## Open Questions

1. Should `entity_key` accept an expression form — `#[kafkaman(entity_key =
   "self.tenant_id.to_string() + &self.sku")]` — for composite identities, or
   should composite keys keep writing the impl by hand?
2. Does the derive emit `descriptor()`, or leave the trait default? The default
   is fallible (`MessageDescriptor::new` validates the identifier), and a derive
   that made it infallible at compile time would be a genuine improvement — but
   requires const validation of the identifier rules.
3. Should `topic` accept the `TopicSpec` overrides (`partitions`,
   `replication_factor`), or does that belong exclusively to the deployment, as
   `examples/provision` currently assumes?
4. Narrowing the `kafkaman-entity-key` condition to the effective record key is a
   prerequisite for the derive's `partition_key` default, but it is a
   wire-visible change to a header foreign consumers may read, and it is not a
   macro change at all. Does it land inside this proposal's scope, or as its own
   compat-noted change ahead of it? The sequencing is not optional; only the
   packaging is.
