# Handler Panic Containment and Fault Injection

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-29
- Category: Runtime behaviour and examples
- Scope: Fixes what a panicking application handler does to a kafkaman service, where fault injection is allowed to live, and that a config section is not honoured until something deliberately fails.
- Sources:
  - wiki/proposals/21-failure-examples-and-panic-containment.proposal.md
  - wiki/decisions/method-level-timing-and-span-depth.decision.md
  - crates/kafkaman-sqlx/src/catch_panic.rs
  - crates/kafkaman-sqlx/src/dispatch.rs
  - crates/kafkaman-sqlx/src/dispatch_failure.rs
  - crates/kafkaman-sqlx/src/operability.rs
  - crates/kafkaman/src/runtime/builder.rs
  - crates/kafkaman-axum/src/lib.rs
  - examples/product/src/faults.rs
  - examples/faults.sh
- Related:
  - wiki/plans/failure-examples.plan.md
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Decision

1. **A panicking handler must not take the service down.**
   The panic is caught at the handler call boundary and converted to
   `Error::HandlerPanicked`, an ordinary handler failure. The row retries on its
   normal budget and dead-letters like any other. The dispatch loop keeps
   running and the HTTP server keeps serving for isolated rows.

2. **The boundary is application-owned code, and nothing wider.**
   Handler futures are wrapped at both dispatch positions. The ingest path also
   wraps application-owned payload work: `Deserialize`, `Serialize`, and
   `KafkaMessage::entity_key()` before a received row exists. A panic in the
   dispatch loop, relay loop, ingester loop, or kafkaman bookkeeping remains a
   bug in kafkaman, and swallowing it would leave a process that looks healthy
   while a loop is dead — the failure the fail-fast supervisor was built to
   prevent.

3. **A caught panic is `ReceivedFailureKind::Handler`, and retryable.**
   Not a new failure kind: those carry permanent RFC 9457 URIs written into
   every stored problem detail, and a panic *is* a handler failure. The
   distinction is carried where it costs nothing — `handler.outcome = "panicked"`
   on the `kafkaman.handler` span, and a stored message beginning
   `handler panicked:`.

   Retryable rather than terminal, unlike `CacheOriginMismatch`, because a panic
   is not deterministic *by construction*: an `unwrap` on a value a concurrent
   writer had not committed yet succeeds on the retry, and that is the case
   worth surviving.

4. **A fleet-wide panic gets a breaker, and it counts distinct rows.**
   Retryability is a per-row decision; a bad deploy that makes every row panic
   is a process-level signal. The worker loop stops after ten consecutive
   *distinct rows* panic, records `kafkaman.scheduler.rows{status="panicked"}`,
   and returns `ConsecutivePanickingRowLimitExceeded` so supervision can surface
   it. A row that gets through clears the streak.

   Distinct rows and not panics, and the difference is the whole decision. The
   first implementation counted panics, and could not tell a bad deploy from one
   poison message: `dispatch_once` claims one row per cycle and only a
   *successful* claim ends a streak, so a single row retrying on the default
   `max_attempts` — which is also 10 — produced ten panics with nothing between
   them. It tripped the breaker on the very attempt that dead-lettered the row.
   The message had been handled exactly as designed, and the service stopped
   anyway: precisely the "one bad message kills the service" failure this whole
   decision exists to remove, reintroduced by the thing meant to bound it.

   Counting rows removes the ambiguity rather than tuning around it. One row
   contributes one entry however often it is retried, so no retry budget can
   reach the limit alone and the two defaults need no relationship to each other.

5. **Containment is not permission.**
   Handlers should still return `Err`. That is the path with a failure class, a
   retry schedule, and a message an operator can read. What changed is the blast
   radius when one does not.

6. **No configuration knob for panic *policy*; one for the breaker threshold.**
   Whether a panic is caught is not configurable — one behaviour, and a
   deployment that genuinely wants the old one already has it: `panic = "abort"`
   removes the unwind there is nothing to catch.

   The breaker threshold is a different question and is configurable, through
   `[dispatcher].max_consecutive_panicking_rows`. It is not a semantic choice but
   an operational one: how much evidence of a bad deploy is enough to stop this
   service, which depends on how many message types it consumes and how much
   traffic each sees. It sits beside `poll_interval` in the same section rather
   than in `[retry]`, which is per message type where both of these are per loop.

7. **Fault injection lives in the examples and nowhere else.**
   `examples/product` carries a `/faults` endpoint; no published crate carries
   anything like it. It is in the OpenAPI spec rather than hidden, because a
   demo's way of breaking itself should be discoverable.

8. **The example services mount the operator routes.**
   An operability surface no example exercises is one that rots — which it had.
   Reads and the destructive redrive stay two routers, mounted together only
   because the stack is local and disposable.

## What Deliberate Failure Uncovered

Neither of the following was found by reading. Both were found by making
something fail on purpose, which is the argument for the examples this decision
accompanies.

### The operator routes had never worked

Every summary route — `/outbox`, `/received`, `/stuck`, `/dlq` — answered **500**
on both example services. Each handler iterates every configured message type and
builds *both* an outbox and a received table for it, then queries whichever the
migrations never created. A service that publishes one type and consumes another
— which is every realistic service — hits this on the first request.

`ResolvedConfig` cannot fix it: it records the descriptors a service exchanges
but not which side of each it is on, because roles are declared above
`kafkaman-sqlx` and a hand-wired service has no registry at all. **The schema is
the authority** — an outbox table exists exactly when the service publishes the
type — so `service_tables` asks it, once per request, and the summaries cover
what is there.

It survived because the only test registered both tables for the *same* message
type, a shape no real service has.

The redrive route carried the same hole from the other side: a type this service
only *publishes* resolves to a registered descriptor and then fails against a
received table that was never created. That is now a 404 naming the reason —
`AdminError::NotRedrivable`, kept separate from `UnknownMessageType` because the
repair differs: the caller named a real message type and pointed it at the wrong
service, rather than misspelling one. The refusal carries a `TableAccess`,
because the same 404 also covers an unmigrated schema and a database role holding
`SELECT` but not the `UPDATE` redrive issues — three repairs the prose had been
guessing between.

### `RuntimeBuilder` discarded the entire `[retry]` section

`ReceivedTable::new` fills in `RetryPolicy::default()`; `for_descriptor` attaches
the message type's resolved policy. The builder used `new`, because taking a
schema and a descriptor was the shape its generic table helper wanted. So every
service booted the blessed way ignored `max_attempts`, `initial_backoff`,
`max_backoff`, `multiplier`, `errors_limit` and `dlq` from its own config file.

Silently, and that is the point: a default policy retries perfectly well. The
example declared eight attempts at 250ms doubling to a 10s cap and got ten at 1s
doubling to 300s. Measured from the row's own error history, the observed gaps
were 0.63, 1.59, 3.37, 7.91, 9.27, 24.97, 36.19 and 112.03 seconds — the library
defaults, exactly. Nothing but a permanently failing message could show it, and
until now nothing could produce one.

`service_manual.rs` used `for_message` and was correct throughout, so the two
boot paths the examples exist to prove interchangeable were not.

## Options Considered

### A. Leave the panic fail-fast and document it

Rejected. The behaviour is worse than it reads: the process exits, compose
restarts it, the still-`Pending` row is re-claimed, and after three restarts the
service is dead for good. One bad message, permanently.

### B. `catch_unwind` around the dispatch loop

Rejected. See decision 2.

### C. Add `futures` for `FutureExt::catch_unwind`

Rejected. It widens the dependency graph of a crate every adopter links, for
thirty lines. Both handler futures are already `Pin<Box<dyn Future>>`, which is
`Unpin`, so polling through the box inside `std::panic::catch_unwind` needs no
structural pinning and therefore no `unsafe` — which matters under the
workspace's `unsafe_code = "forbid"`.

### D. A dedicated `ReceivedFailureKind::HandlerPanicked`

Rejected, narrowly. An operator triaging a DLQ does want to separate panics from
returned errors, and that field is where such a filter would belong. But the
variant list is a permanent URI vocabulary, and the same separation is available
from a span attribute and a message prefix at no compatibility cost. Revisit if a
DLQ filter on it is actually asked for.

### E. Fault injection behind a library feature flag

Rejected. A `#[cfg]`-gated way to make handlers fail is a maintenance surface on
every crate it touches, for something one example needs.

## Consequences

- `kafkaman-sqlx` gains `Error::HandlerPanicked`,
  `Error::ApplicationPanicked`, `DispatchStats.panicked`,
  `DispatchStats.panicked_message_id`, `service_tables`, `service_table_access`,
  `ServiceTables`, and `TableAccess`. `kafkaman-rdkafka` gains
  `Error::PayloadPanicked`; `kafkaman-worker` gains
  `Error::ConsecutivePanickingRowLimitExceeded`; `kafkaman-core` gains
  `DispatcherConfig`, `Error::InvalidDispatcherConfig`, and `panic_message`.
  These are source-sensitive for exhaustive matches or struct literals and are
  recorded in the compatibility note.
- A new optional `[dispatcher]` config section carries the breaker threshold and
  the dispatcher's poll interval, and `run_dispatcher` takes a
  `DispatcherConfig` in place of two loose parameters.
- `kafkaman.handler` gains `handler.outcome`, and dispatcher metrics gain
  `status="panicked"` / `outcome="panicked"` observations.
- Handler panics now cost a `catch_unwind` per handler call. The default panic
  hook still prints, so a panic is as visible in the logs as it ever was.
- A service booted through `RuntimeBuilder` now retries the way its config says,
  which changes observable behaviour for anyone who declared a `[retry]` section
  and never noticed it was ignored. That is a fix, and it will look like a
  behaviour change.
- `examples/faults.sh` asserts the configured attempt count end to end, so the
  builder regression cannot come back unnoticed.

## Revisit If

- A DLQ filter on "panicked versus returned" is asked for, which would settle
  option D the other way.
- A second call site starts persisting or transmitting handler outcomes.
- Anyone needs the fail-fast behaviour back for reasons `panic = "abort"` does
  not cover.
