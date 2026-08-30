# Runtime Composition and Topology

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-20
- Category: Runtime and integration
- Scope: How kafkaman's background subsystems are run and shut down, how a host wires them next to (or without) Axum, and the send-side request UX. The receive/consumption runtime (handler dispatch, kafkaman-owned receive tx) is deferred to a separate decision.
- Sources:
  - raw/design/2026-06-20-kafkaman-architecture-discussion.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/decisions/configuration-and-environment-model.decision.md
- Related:
  - wiki/proposals/01-kafkaman-objectives.proposal.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/decisions/runtime-builder-and-axum-composition.decision.md
  - wiki/plans/first-poc-outbox-publisher.plan.md


## Amendment, 2026-08-26: `kafkaman-axum` is not being built as a crate

This decision plans a separate `kafkaman-axum` crate and refers to it throughout
— request-scoped pieces, the `axum-sqlx-tx` coupling, the setup guide, the
version-compatibility surface. `wiki/decisions/runtime-builder-and-axum-composition.decision.md`
supersedes the packaging: the composition ships as `kafkaman::axum` behind an
`axum` feature on the facade, because that facade already gates `rdkafka` — a
C-linking dependency — behind a feature "so an application never has to name a
second kafkaman crate", and a pure-Rust optional dependency does not warrant
weaker treatment.

Read every `kafkaman-axum` below as `kafkaman::axum`. The *boundary* this
decision draws is unchanged and is the reason the later decision holds: core
kafkaman stays HTTP-free, background loops stay separable from the request path,
and worker-role binaries remain first-class. Only the crate boundary became a
module boundary.

### What actually shipped, 2026-08-26

The module is much narrower than this decision plans, and the difference matters
enough to state rather than leave a reader to infer from a name substitution.

`kafkaman::axum` is `serve(listener, app).with_runtime(runtime)` and a handle
with `addr`, `wait`, and `shutdown`. It binds no socket, owns no router, defines
no state, and installs no signal handler. Its one idea is that the HTTP server
becomes *one more supervised loop* in the runtime's `JoinSet`, sharing the
cancellation token with the relay, the ingester, and the dispatcher — so the
first kafkaman loop to die stops the service accepting traffic without anything
having to watch for it.

**Point 5 below was not built, and the substitution rule does not apply to it.**
`kafkaman::axum` takes no `axum-sqlx-tx` dependency, and there is no `Sender`
extractor, no ambient-`Tx` coupling, and no `send_non_transactional` escape
hatch. The send-side request UX described there remains unbuilt design space.
The transactional property it exists to provide is already available without any
of it: `enqueue` takes the caller's `&mut Transaction`, so an outbox row and the
business write it accompanies commit together by construction —
`examples/order/src/http.rs` does exactly that with a plain Axum handler and an
ordinary sqlx transaction.

Consequently the `axum-sqlx-tx` version-compatibility surface listed under
*Consequences* does not exist either, because the dependency does not.

## Amendment, 2026-08-31: subsystem selection and default topology

Point 3 below is corrected by M7. Subsystem selection exists, but the default is
not the smallest non-destructive set. `RuntimeBuilder::new()` defaults to
`Subsystems::all()` so existing builder adopters keep the same behavior they had
before M7; purging still starts only when `[retention]` is configured. The
copyable no-purge worker shape is `Subsystems::PIPELINE`, which selects relay,
ingest, dispatch, and queue metrics while leaving `PURGE` out.

This preserves the decision's main boundary: topology is explicit host-owned
deployment shape, worker role means the host's own binary without HTTP, and
kafkaman still ships no standalone daemon.

## Ratification, 2026-08-31: accepted, with point 5 recorded as not built

The V1 roadmap held this decision at `Draft` behind a two-stage gate: the
send-side half was eligible at M1 exit, the receive half only once the
consumption decision landed. That decision is `Accepted`, M7 delivered the
graceful-shutdown ordering the last "Revisit When" bullet asked this decision to
reconcile with, and the two amendments above already record how the packaging and
the default topology changed. The gate has passed and the status moves to
`Accepted`.

Points 1, 2, 3, 4, and 6 shipped: subsystems are spawnable units assembled by
`RuntimeBuilder::into_tasks()`, every loop honors the shared `CancellationToken`,
topology is a host choice with no standalone daemon, request-path concerns stay
Axum-native, and `kafkaman_sqlx::enqueue(&mut tx, evt)` remains the
framework-agnostic core.

**Point 5 did not ship at all, and its absence is not cosmetic.** There is no
`axum-sqlx-tx` dependency anywhere in the workspace, no `Sender` extractor, and
no quarantined `send_non_transactional`. The ambient auto-committing transaction
this point builds its whole send-side UX on does not exist; what exists is point
6's explicit form, which is what the examples use — `kafkaman::sqlx::enqueue`
against a transaction the handler opens and commits itself. The atomicity
guarantee is intact, because it was always a property of sharing the
transaction rather than of who commits it; what is missing is the ergonomic
layer that would have made the sharing implicit, and with it the named,
observable escape hatch that made the non-transactional path expensive to reach
for. A host that wants to dual-write today simply does not pass the transaction.

## Decision

1. **Schedulers are spawnable units, not a process.** The host builds a
   `kafkaman::Runtime` in its own crate (where the compiled-in `#[handler]`s are
   in scope). The runtime yields futures the host spawns:
   - `runtime.run(shutdown)` — one future driving **all** selected subsystems
     (the simple case).
   - `runtime.into_tasks()` — one **named future per subsystem** (the power
     escape hatch: independent spawn / restart / per-subsystem observability).
   kafkaman never calls `tokio::spawn` itself and never owns the Tokio runtime —
   it hands back futures; the host decides where they run.

2. **Shutdown contract.** Every subsystem future honors a
   `tokio_util::CancellationToken`. The host installs the signal handler, trips
   the token, and awaits the drain. There is no kafkaman-owned signal handling
   except inside the optional Axum helper (point 4).

3. **Topology is a host choice — there is no standalone daemon.** The same crate
   runs **embedded** (alongside Axum) or in a **worker role** (no HTTP) by
   selecting subsystems on the builder. **Subsystem selection is explicit.** The
   original draft made the default the smallest non-destructive set; the
   2026-08-31 amendment above supersedes that detail with
   `RuntimeBuilder::new()` defaulting to `Subsystems::all()` for compatibility,
   while `Subsystems::PIPELINE` is the no-purge worker preset. A host opts into
   `Purge`/retention enforcement deliberately; retention still starts only when
   `[retention]` is configured and `PURGE` is selected. A
   generic standalone `kafkaman` binary is *not viable in Rust* — handlers are
   user Rust compiled into the host, so dispatch requires the host crate. **"Worker
   role" therefore means *the host's own binary booted without HTTP*, not a
   separate shipped daemon** (one phrase, used everywhere — see the objectives
   proposal's `kafkaman-worker` crate note).

4. **Request-path concerns are Axum-native; background loops are spawnables.**
   Continuous loops (relay, consumers, retry, purge) are **never** Tower
   middleware — that would re-couple them to HTTP (killing worker-role), give
   them no clean lifecycle owner, and leave nowhere to hang graceful drain.
   `kafkaman-axum` provides only request-scoped, pull-driven pieces:
   - `CorrelationLayer` — a Tower layer that reads/creates `correlation_id` and
     a W3C `traceparent` for the request.
   - admin/health/DLQ-inspection **routes**.
   - `serve(addr, app).with_runtime(runtime).run()` — a helper that composes the
     HTTP graceful-shutdown and the scheduler drain under one signal. It does
     **not** own the `Router` or the Tokio runtime; it only owns the shutdown
     composition.

5. **Send-side UX is opinionated around `axum-sqlx-tx`.** `kafkaman-axum` takes a
   real dependency on `axum-sqlx-tx` and pairs with its ambient, lazily-begun,
   auto-committing `Tx<Postgres>`:
   - A `Sender` extractor carries the request `Ctx` (correlation/causation) and a
     producer handle — and **no transaction of its own**, so it never contends
     for the ambient tx slot.
   - `sender.enqueue(&mut tx, evt).key(..).await?` inserts the outbox row into
     the host's **ambient request transaction**. The handler writes **no
     `tx.commit()`**: `axum-sqlx-tx` commits the business write + outbox row
     **atomically on a success response**, and rolls **both** back otherwise
     (failed request ⇒ no message — atomic in both directions).
   - **Non-transactional escape hatch (quarantined).**
     `sender.send_non_transactional(evt).await?` has no ambient tx and
     **explicitly forgoes** the core atomicity guarantee (dual-write risk). It is
     deliberately *not* named `send`/`send_now` — the name states the cost — it is
     **not** on the default blessed `Sender` surface (a host reaches for it
     explicitly, e.g. via a distinct `UnsafeSender`/method behind an import), and
     it emits a counter + span so its use is observable in production. Accepted as
     the exception per the
     [messaging-scope decision](messaging-scope-and-receive-model.decision.md) —
     never the default path.

6. **Core enqueue stays framework-agnostic.** `kafkaman-sqlx::enqueue(&mut tx, evt)`
   is generic over the executor (`impl PgExecutor` / `&mut PgTransaction`), so
   non-Axum hosts and the worker side can enqueue too. The opinionated
   `axum-sqlx-tx` coupling lives **only** in `kafkaman-axum`.

## Why

- **Spawnable units realize core promise #4 ("fits, does not replace").** The
  host keeps ownership of its process model, scaling, and runtime; kafkaman
  contributes futures, not a framework.
- **Worker-role must run without HTTP**, which is exactly why schedulers cannot
  be middleware — the constraint forces the spawnable shape.
- **The opinionated send pairing makes the single most important correctness
  property — atomic business-write + outbox — the default**, by deleting the
  forgettable `tx.commit()`. Going opinionated (a real `axum-sqlx-tx` dependency)
  was chosen over compatible-by-genericity so the blessed path is tight and the
  atomicity is not something a handler can opt out of by accident.
- **A generic core keeps the library reusable**; only the Axum sugar is
  opinionated.

## Assumptions / Documented Dependencies

- **Commit policy governs send behavior.** Atomicity relies on `axum-sqlx-tx`'s
  commit-on-success policy: the host's configured success-status range decides
  whether the ambient tx commits — and therefore whether queued messages are
  sent. This is *correct* (a failed request sends nothing), but a misconfigured
  commit range would silently change send behavior. This must be documented
  prominently in the `kafkaman-axum` setup guide.

## Alternatives Considered

- **A standalone generic `kafkaman` daemon:** not viable — compiled-in user
  handlers cannot be dispatched from a separate generic binary without dynamic
  loading/FFI, which we reject.
- **Schedulers as Tower middleware/layers:** rejected — re-couples background
  loops to HTTP (breaks worker-role), no per-connection lifecycle owner, no
  graceful-drain hook. A "middleware" that ignores the request and spawns a loop
  is a background task in disguise.
- **kafkaman wrapping/owning the Tokio (or Hyper) runtime builder:** rejected —
  the moment the library owns `main`/the runtime it becomes a framework and the
  host loses compose-it-yourself flexibility. Give the primitive (spawnable
  futures), offer sugar (the Axum helper), do not own the runtime.
- **A kafkaman-owned `Tx` extractor + explicit `commit()`:** rejected once
  `axum-sqlx-tx` is assumed — it reinvents transaction management and reintroduces
  the forgettable commit the opinionated pairing removes.
- **Compatible-by-genericity (no `axum-sqlx-tx` dependency):** considered and
  rejected in favor of the tighter opinionated integration (decided 2026-06-20).
  The generic `enqueue(&mut tx)` still lives in `kafkaman-sqlx` for non-Axum and
  worker-side use, so reusability is preserved.

## Consequences / Tradeoffs Accepted

- `kafkaman-axum` carries an `axum-sqlx-tx` dependency and its version-compatibility
  surface.
- Send behavior is tied to the host's commit-policy configuration (the documented
  assumption above).
- Handlers omit `commit()` — correct under `axum-sqlx-tx`, but a reader must know
  the layer is in play; mitigated by it being the blessed, documented setup.

## Revisit When

- A second transport or a non-Postgres transaction story appears (re-examine the
  generic/opinionated split; the `axum-sqlx-tx` coupling this bullet was written
  against was never built — see the 2026-08-31 ratification).
- A host needs per-subsystem **process** isolation beyond what `into_tasks()`
  (per-subsystem task isolation) provides.
- An opinionated send-side extractor is revisited — the point 5 UX is deferred,
  not rejected, and would reopen the ambient-transaction question.
- ~~The receive/consumption decision lands and adds subsystems or shutdown
  ordering constraints this draft should reconcile with.~~ Fired: that decision
  is `Accepted` and M7 settled the shutdown ordering; reconciled by the
  2026-08-31 amendment and ratification above.
