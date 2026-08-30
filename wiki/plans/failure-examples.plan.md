# Failure Examples Plan

- Document Class: Plan
- Status: Completed
- Date: 2026-08-29
- Category: Examples and runtime execution
- Scope: Records the panic boundary, the fault switch, the six-scenario walkthrough, the Kibana failure panel, and the two library bugs that only a deliberate failure could surface.
- Sources:
  - wiki/proposals/21-failure-examples-and-panic-containment.proposal.md
  - wiki/decisions/handler-panic-containment-and-fault-injection.decision.md
  - crates/kafkaman-sqlx/src/catch_panic.rs
  - crates/kafkaman/src/runtime/builder.rs
  - crates/kafkaman-sqlx/src/operability.rs
  - examples/faults.sh
  - examples/kibana-dashboard.sh
- Related:
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Deliverable

Make every failure path in the example stack drivable on demand, asserted, and
visible in Kibana — and stop a panicking handler from killing the service.

## Phase 1 - The panic boundary

Status: Completed.

`crates/kafkaman-sqlx/src/catch_panic.rs`, about a hundred lines including its
reasoning, wraps a boxed handler future and polls it inside
`std::panic::catch_unwind`. No new dependency and no `unsafe`: both handler
futures are already `Pin<Box<dyn Future + Send>>`, which is `Unpin`, so
`get_mut` is safe and nothing needs structural pinning.

Both `.handle(...)` sites in `converge_and_dispatch` go through one `run_handler`
helper rather than repeating four lines twice, so the boundary cannot be applied
to one position and forgotten on the other — which is the failure mode it exists
to prevent.

A caught panic becomes `Error::HandlerPanicked(String)`, classified as a
retryable `ReceivedFailureKind::Handler`, with `handler.outcome = "panicked"`
recorded on the `kafkaman.handler` span. The message is truncated at 512
characters: unlike a returned error, a panic message is not authored for storage,
and `assert_eq!` on two large structures produces kilobytes of `Debug` output
into a column that keeps `errors_limit` of them.

The transaction hazard needed no new code. `rollback_handler_and_record_received_failure`
already abandons an unusable transaction and records the failure on a fresh
connection, which is precisely the case a panic mid-statement produces.

**Proved by deliberate breakage.** Three tests in
`tests/durable-send/tests/durable_receive/dispatch_handler_panic.rs`: the row is
parked `Retryable` with the panic message and the handler's writes rolled back;
the budget exhausts to `Failed`; and — the one that matters — the dispatch loop
is still running afterwards, shown by a second message behind the poison one
being processed. With the catch removed all three fail, the third with a
`JoinError` from the dead task, which is the old behaviour exactly.

One existing test had encoded that old behaviour.
`crash_during_dispatch_rolls_back_and_can_be_redriven` panicked in a handler and
asserted the dispatch task panicked. Its actual subject is the transaction
guarantee when a dispatch stops mid-flight, so the simulation moved to
`JoinHandle::abort` while the handler parks with its transaction open — a truer
model of a killed process than the panic ever was, and every assertion below it
held unchanged.

## Phase 2 - The fault switch

Status: Completed.

`examples/product/src/faults.rs`: a `static Mutex<FaultState>`, which
`Mutex::new` makes a `const` initializer, so there is still no lazy setup and no
`OnceLock`. `POST /faults` with `{mode, remaining?}`, `GET`, `DELETE`, all
`#[utoipa::path]`-annotated so they appear in Swagger UI.

One mutex rather than the three separate atomics this plan first called for. The
three values are one state — arming with `{mode: "error", remaining: 2}` has to
become visible as that pair and not as a mode without its budget, and claiming a
budget unit has to read the mode and decrement together or two dispatch threads
both spend the last attempt. The lock is held for the length of one `check()`
and contended only by handlers that are already deliberately failing, so the
cost the atomics were avoiding is not a cost this switch pays.

A `static` rather than state threaded through `AppState` because the value
genuinely has one instance per process, and both boot paths — `service` and
`service_manual` — have to see the same one. Threading it would have meant a new
`AppState` field, a new `dispatch_router` parameter, and a clone captured by each
handler closure: three places to keep in step for one switch. `faults::check()`
is called at the top of `derive_availability` and `apply_order_snapshot`, so both
boot paths behave identically.

`remaining` is the entire difference between the two failure stories: bounded is
transient and converges unattended, absent is permanent and dead-letters.

## Phase 3 - The operator routes, and the bug under them

Status: Completed.

Mounting `admin_router` merged with `redrive_router` at `/internal/kafkaman` in
both services is two lines. It immediately returned **500 from all four summary
routes on both services**.

Each handler iterates every configured message type and builds both an outbox and
a received table for it, then queries whichever the migrations never created.
`product` publishes `product_snapshot` and consumes `order_snapshot`; `order`
does the mirror image; neither has all four tables. Every realistic service hits
this on the first request, and it had never been noticed because nothing mounted
the routes and the one test registered both tables for the same message type.

Fixed with `service_tables`, which asks the schema — one `to_regclass` over an
`unnest`ed array, one round trip for the whole set — and filters each summary to
the tables that exist. The configuration cannot answer this: it records the
descriptors a service exchanges but not which side of each it is on, because
roles are declared above `kafkaman-sqlx` and a hand-wired service has no registry
at all.

The redrive route had the same hole from the other side — a publish-only type
resolves to a registered descriptor and then fails against a received table that
was never created — so it now answers 404 with `AdminError::NotRedrivable` rather
than 500, carrying a `TableAccess` that names which of the three possible repairs
applies.

`the_summaries_cover_only_the_tables_this_service_has` pins all of it, and has to
drop two tables the harness creates unconditionally to build the fixture — which
is itself the reason the bug hid.

## Phase 4 - The walkthrough, and the second bug

Status: Completed.

`examples/faults.sh`, six scenarios, all asserted, in the style of `smoke.sh`.
Scenarios 5 and 6 drive Docker and skip with a note when it is absent. `just
examples faults` runs all six; `just examples all` runs 1 and 2 so the telemetry
has a failure side, deliberately leaving the dead-lettered row rather than
redriving it.

Scenario 4 proves panic containment with no Docker at all, using the fault
switch's own state: `fired` lives in the panicking process's memory and is reset
by arming, so reading `3` back after three panics is proof the process never
restarted. A restart would report `0` and a disarmed fault.

**Writing scenario 2 is what found the second bug.** A permanently failing
handler took **64 seconds and more than ten attempts** to dead-letter, against a
config declaring eight attempts at 250ms doubling to a 10s cap. The gaps read off
the row's own error history were 0.63, 1.59, 3.37, 7.91, 9.27, 24.97, 36.19 and
112.03 seconds — the library defaults, exactly.

`RuntimeBuilder` built its received tables with `ReceivedTable::new`, which fills
in `RetryPolicy::default()`, rather than `for_descriptor`, which attaches the
message type's resolved policy. **Every service booted the blessed way ignored
its entire `[retry]` section.** `service_manual.rs` used `for_message` and was
correct, so the two boot paths that the `distributed-cache` suite exists to prove
interchangeable were not.

Fixed by widening `table_for`'s closure to take the whole `ResolvedConfig`, which
is what makes `for_descriptor` fit. Afterwards the same scenario dead-letters in
**19 seconds at exactly 8 attempts**, and the script asserts that number rather
than merely asserting that something failed — it is the only end-to-end evidence
that a service retries on its own configuration.

## Phase 4a - A red test found on the way past

Status: Completed. **Pre-existing, and not caused by this work** — verified by
stashing every change and watching it fail identically on `HEAD`.

`an_enqueue_with_no_caller_span_still_anchors_the_trace` asserts that with no
caller span, `kafkaman.enqueue` is the root of its trace. It had a parent.

The cause is the `kafkaman::internal` tier from the previous change:
`outbox_enqueue.rs` annotates `pub async fn enqueue`, which calls
`enqueue_on_connection`, which opens the `kafkaman.enqueue` phase span. So under
the tier the phase span correctly has a function span above it — the same shape
the live 66-span waterfall shows — and `TracePipeline::install()` applies **no
filter at all**, so the test saw a tier a default deployment does not run.

The unfiltered pipeline is deliberate for the other trace tests, which are the
tier's regression proof and assert its spans are present. So the fix is a second
constructor, `install_at_default_filter`, used by this test alone, applying
`EnvFilter::new("info")` per layer the way `kafkaman_otel::init` does. The
assertion is unchanged; it now runs against the configuration it is about.

## Phase 5 - Kibana and documentation

Status: Completed.

**The field was verified, not guessed.** After scenario 1 ran, one error span was
read straight out of Elasticsearch. The OTel mapping puts the status at
`status.code` (value `"Error"`) with the recorded message at `status.message`,
and `attributes.handler.outcome` carries `panicked`. The pipeline preserves the
error signal end to end, which was an open question until measured.

Against the same stack, after the walkthrough: **96 spans carrying
`status.code: Error`** and **153 `WARN`/`ERROR` log records**, from zero of each
before.

`examples/kibana-dashboard.sh` gains a fifth saved search, `kafkaman failures`,
querying `data_stream.type: traces and status.code: Error`. That is exhaustive
with no exclusions to maintain, because nothing sets a span status on success.
The four-panel tiling became five; the `y` offsets were moved together, since
overlapping ones do not error and just stack panels silently.

`examples/README.md` gains "When it goes wrong": the switch, the six scenarios,
the panic change and why it is containment rather than permission, the Kibana
query, and the operator routes with their missing-auth caveat stated plainly.

## Verification

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `just lint`
- `cargo test -p durable-send-tests -p observability-tests --all-features`, green
  after the pre-existing failure in Phase 4a
- `just examples faults` — all six scenarios, against the live stack
- Live: all eight operator routes answering 200 on both services, having answered
  500 before; 96 error spans and 153 WARN/ERROR log records in Elasticsearch,
  having been zero of each before; the `kafkaman failures` saved search resolving
  to 96 documents

## Out Of Scope

- Catching panics in kafkaman-owned loop/bookkeeping code.
- A configuration knob for panic policy.
- Fault injection in any published crate.
- A dedicated `ReceivedFailureKind` for panics — see the decision's option D.
