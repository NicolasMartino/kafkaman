# Message Consumption and Handler Model

- Document Class: Decision
- Status: Draft
- Date: 2026-06-20
- Category: Runtime and integration
- Scope: How kafkaman consumes from Kafka durably, how received messages are stored and de-duplicated, and the handler programming model (a Tower stack with extractors). Retry/backoff/DLQ policy is defined separately; this decision reserves and uses the fields and seams for it.
- Sources:
  - raw/design/2026-06-20-kafkaman-architecture-discussion.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/decisions/message-identity-and-header-namespace.decision.md
  - wiki/decisions/retry-backoff-dlq-policy.decision.md
- Related:
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/proposals/01-kafkaman-objectives.proposal.md
  - wiki/plans/first-poc-outbox-publisher.plan.md

## Decision

1. **Two schedulers — "messages must flow."** Consumption is split so user code
   can never stall the broker:
   - **Ingest scheduler** (Kafka-facing): consumes, writes the received row into
     the durable per-type table, and **commits the Kafka offset immediately after
     that durable write** — never after the handler. A slow, failing, or poison
     handler can therefore never block the partition or trigger consumer-group
     rebalance churn.
   - **Dispatch scheduler** (DB-facing): claims **due** rows
     (`WHERE status IN ('Pending','Retryable') AND next_attempt_at <= now()`) with
     `FOR UPDATE SKIP LOCKED`, marks them `Processing`, drives the user's handler
     stack, and on the outcome moves the row to `Processed` (Ok) or back to
     `Retryable` / on to terminal `Failed` (Err — see point 6). This is the only
     place user code runs. **The claim predicate and the status set are fixed by
     this decision** (the dispatcher is built on them); only the retry *policy*
     that decides Retryable-vs-Failed is deferred.

2. **Per-type received tables (per the [schema decision](schema-and-change-management.decision.md)).**
   Each row carries: message **identity** (message_id + idempotency key), topic /
   partition / offset, partition **key**, **payload** + type/version, headers,
   `correlation_id` / `causation_id`, `status` (the receive **state machine** —
   `Pending → Processing → Processed` on success, `Processing → Retryable →
   Processing` on a retryable failure, `Processing → Failed` on a terminal one;
   see point 6), `attempts` (monotonic int), **`errors`** (see point 3),
   `next_attempt_at` (the re-drive gate), and timestamps (`received_at`,
   `processed_at`).

3. **Dedup is a log; failures accumulate in a bounded `errors` JSONB array.**
   - **Dedup:** every persisted V1 message has a required `idempotency_key`.
     Ingest does `INSERT … ON CONFLICT DO NOTHING` on the per-type
     `UNIQUE(idempotency_key)`. A Kafka redelivery or semantic duplicate is a
     **logged no-op** — it never blocks ingest and the offset still advances.
   - **`errors` is a JSONB array**, not a single `last_error` string: each failed
     attempt appends `{ attempt, at, error, … }`, so operators see the **recent
     failure history** for writing dedicated fix-up code. **Safeguarded against
     unbounded growth:** the array is a bounded ring keeping only the most recent
     N entries (default ~20, configurable) — under a retry storm it drops the
     oldest rather than ballooning. `attempts` (the monotonic counter) is the
     authoritative count; `errors` is the bounded **recent** history — explicitly
     **not** a full audit log. If complete failure history is ever required, that
     is a separate audit/event-table concern, not this ring.
   - The `Failed` status + `errors` are the **remediation surface**: query the
     per-type table, write fix-up code, or re-drive. Nothing about dedup or
     failure ever reaches back and locks Kafka.

4. **Handler model: an axum-shaped Tower stack — its own thing, not HTTP.** The
   consumer side is a `kafkaman::MessageRouter` that **is a `tower::Service<Message>`**
   (dispatch keyed by `message_type`, the way axum's `Router` keys by path). It is
   **not** built on `http::Request`; a Kafka message is never masqueraded as an
   HTTP request.
   - **Explicit, axum-style registration** (no handler-discovery macro):
     `.handler::<OrderShipped>(on_shipped)`. The message *type* carries its
     `topic` / `message_type` via a derive on the payload struct
     (`#[derive(KafkaMessage)]`), so the builder stays a plain explicit list —
     consistent with "changesets/handlers are explicit lists, not magic
     discovery" ([schema decision](schema-and-change-management.decision.md) point 9).
   - **Extractors mirror axum's ergonomics** via a kafkaman `FromMessage` /
     `FromMessageParts` trait (kafkaman's own, *not* axum's `http`-bound
     `FromRequestParts`). Handlers read just like axum handlers:

     ```rust
     async fn on_shipped(
         Json(evt): Json<OrderShipped>,   // typed payload decode
         mut tx: kafkaman::Rx,            // the kafkaman-OWNED receive transaction
         State(st): State<AppState>,      // app state
         http: Extension<HttpClient>,     // escape hatch: a REST call here is at-least-once, NOT effective-once (point 6)
     ) -> Result<(), HandlerError> { … }
     ```

5. **Tower reuse: inherit the engine, skip the HTTP masquerade.** Because Tower is
   transport-agnostic, **generic** middleware works on a `Message` unchanged —
   `Timeout`, `ConcurrencyLimit`, `Retry`, `Buffer`, `LoadShed`, `RateLimit`.
   What is **not** reused: axum's `http`-bound extractors and `tower-http` layers
   (Trace/CORS/compression) — they are tied to `http::Request` and do not apply to
   a Kafka message. kafkaman re-implements the thin extractor trait and ships its
   own message-aware helpers (e.g. `TracingLayer`). Net: ~90% of the ecosystem
   (the Service/Layer machinery + generic middleware) is inherited; only the small
   extractor trait is re-implemented; nothing pretends Kafka is HTTP.

6. **The receive transaction relocates to kafkaman.** The dispatch scheduler runs
   the Tower stack **inside a kafkaman-owned transaction** handed to the handler
   as the `Rx` extractor. On `Ok`, the handler's business writes and kafkaman's
   "mark `Processed`" **commit together** — idempotent / effective-once. On
   `Err` (or a layer short-circuit such as timeout/load-shed, which counts as a
   failed attempt): roll back, append to `errors` (bounded), `attempts++`, set
   `next_attempt_at`, and move the row to **`Retryable`** so the dispatch claim
   (point 1) re-drives it once due. The offset is already committed (point 1), so
   Kafka keeps flowing regardless. (Terminal-vs-retryable classification and the
   `Retryable → Failed` / DLQ transition are the deferred retry decision's job —
   this decision only *reserves* the `Failed` terminal state and the
   `next_attempt_at` gate.)

   **Scope of "effective-once" — read carefully.** The guarantee covers exactly
   the writes made **through `Rx`** (the kafkaman-owned tx): those commit
   atomically with `mark Processed`, so a re-drive cannot double-apply them.
   **External side effects are NOT covered.** An HTTP call, a publish to another
   system, or any write outside `Rx` runs at **at-least-once** and *will* re-fire
   on re-drive. Such effects must either carry their own idempotency (a key the
   peer honors) or be chained through kafkaman's own outbox (enqueue inside `Rx`
   and let the relay deliver after commit). The `http: Extension<HttpClient>` in
   the handler example is therefore a deliberately **non-transactional** escape
   hatch, not an effective-once path — kafkaman cannot and does not promise
   exactly-once for effects it does not own.

7. **Hybrid wiring — two independent towers, one process.** The host builds the
   consumer tower and (optionally) the Axum tower; the embedded topology runs both
   under one shutdown:

   ```
   1. config   → kafkaman::Config::load()
   2. migrate  → kafkaman::migrate(&pool, changelog())
   3. consumers→ MessageRouter::new().handler::<T>(..).layer(..).with_state(..)
   4. http     → axum::Router (CorrelationLayer + axum-sqlx-tx)   // optional
   5. serve    → kafkaman_axum::serve(addr, app).with_runtime(runtime).run()
   ```

   Worker-role drops step 4 and calls `runtime.run(shutdown).await` — the consumer
   tower stands alone with no Axum, which is the proof the two towers are
   independent.

## Why

- **Two schedulers** are what make "messages must flow" real: the Kafka-facing
  side does only a fast durable write, so broker liveness is decoupled from
  handler health. This is the durable-execution model from the
  [messaging-scope decision](messaging-scope-and-receive-model.decision.md), made
  concrete.
- **Dedup-as-log + JSONB `errors`** turns at-least-once *delivery* into
  effective-once *processing* while preserving a rich, operator-facing failure
  history — bounded so a poison message can't grow a row without limit.
- **Reusing Tower but not HTTP** captures the real win (the entire generic
  middleware ecosystem + the extractor ergonomics developers already know)
  without the leaky abstraction of faking `http::Request` semantics (methods,
  paths, status codes) that mean nothing for a Kafka message.
- **kafkaman owning the receive tx** is the receive-side mirror of the send side's
  ambient tx: there too, business write + bookkeeping commit atomically.

## Assumptions / Dependencies

- The payload type implements a kafkaman `KafkaMessage` trait (via derive) exposing
  `topic` + `message_type`; registration and the per-type table key off it.
- Effective-once depends on the identity `UNIQUE` being correct per type (the
  schema decision's per-table `UNIQUE`).

## Alternatives Considered

- **Single inline-consume scheduler** (run the handler in the Kafka poll loop,
  commit the offset after the handler): rejected — a slow/failing handler stalls
  the partition and causes rebalance churn. Violates "messages must flow."
- **A single `last_error` string column:** rejected in favor of a bounded JSONB
  `errors` array — operators want the failure *history*, not just the latest, and
  the bound prevents unbounded growth.
- **Modeling messages as `http::Request` to reuse axum extractors / `tower-http`
  verbatim:** rejected — leaky (HTTP semantics are meaningless for Kafka) and
  confusing. kafkaman re-implements the small `FromMessage` trait instead.
- **Annotation/macro handler discovery (`#[kafkaman::handler(...)]`):** set aside
  in favor of explicit axum-style `.handler::<T>(..)` registration — explicit
  lists are easier to reason about and match the send/router ergonomics; the only
  derive is on the *payload type* for `topic`/`message_type` metadata.

## Consequences / Tradeoffs Accepted

- Two schedulers + a received table per type is more moving parts than inline
  consume — bought with broker decoupling, a retry surface, and re-drive.
- kafkaman maintains its own extractor trait and message-aware layers (small,
  ongoing cost) instead of leaning on axum's `http` extractors.
- A duplicate within the dedup window is silently skipped (logged), not surfaced
  to the handler — intentional, but means dedup is invisible unless inspected.

## Ratified Sub-Decision - Dedup Identity

Dedup hinges on a per-type `UNIQUE`, and the accepted V1 identity is the
required business `idempotency_key`.

Options considered:

- **`(topic, partition, offset)`** - dedups Kafka redeliveries only. Two
  semantically identical events published at different offsets are not collapsed.
- **`message_id`** - dedups one envelope instance across redeliveries and
  rebalances, but not the same business event emitted with a new envelope id.
- **`idempotency_key` when present, else `message_id`** - flexible, but creates
  two semantic classes of V1 messages.
- **Required `idempotency_key`** - accepted. It gives kafkaman one semantic
  dedup contract and forces producers to state the business identity of each
  durable message.

The separate
[message identity and header namespace decision](message-identity-and-header-namespace.decision.md)
owns the envelope-level contract and the reserved `kafkaman-*` header namespace.

## Revisit When

- The **retry / backoff / DLQ** follow-up decision lands — it will use `attempts`,
  `next_attempt_at`, `errors`, and a terminal→DLQ transition this decision already
  reserves.
- Per-key **ordering** guarantees are needed (the dispatch scheduler's
  `SKIP LOCKED` claim is unordered across keys by default).
- Dispatch **polling lag** becomes material → add `LISTEN/NOTIFY` wakeups (the
  escape hatch noted for OQ3).
