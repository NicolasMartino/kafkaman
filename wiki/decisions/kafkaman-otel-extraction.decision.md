# kafkaman-otel Extraction

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-27
- Category: Observability developer experience
- Scope: Settles whether the example's OpenTelemetry pipeline becomes a shipped crate, where the SDK boundary moves to accommodate it, what the public surface is, and what version commitment the crate carries.
- Sources:
  - wiki/proposals/17-kafkaman-otel-convenience-crate.proposal.md
  - crates/kafkaman-otel/src/lib.rs (extracted from the since-deleted examples/telemetry)
  - wiki/plans/opentelemetry-completion.plan.md (Phase 4, step 4)
  - justfile (`opt-out` recipe)
- Related:
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/decisions/telemetry-backend-and-example-topology.decision.md
  - wiki/plans/kafkaman-otel-extraction.plan.md
  - wiki/compatibility/kafkaman-otel-surface.compat.md

## Decision

1. **The pipeline becomes `crates/kafkaman-otel`, an opt-in companion crate.**
   It holds what `examples/telemetry` holds today: provider construction, signal
   selection from the OTLP environment variables, subscriber layer wiring, and
   ordered shutdown. `examples/telemetry` is deleted and both example services
   depend on the crate.

2. **The SDK boundary moves by exactly one crate, and it is named.**
   `telemetry-pipeline-ownership` item 1 stands unchanged: no crate that
   `kafkaman` depends on may link an SDK or an exporter. What changes is item 6's
   "exactly two places" count. `kafkaman-otel` is the third, it depends on no
   other kafkaman crate, and nothing depends on it. It is a leaf, by
   construction — which is what keeps the boundary checkable.

3. **The facade does not re-export it.**
   No `kafkaman = { features = ["otel"] }`, no `kafkaman::otel`. That would put
   `opentelemetry_sdk` into `crates/kafkaman`'s `--all-features` graph and fail
   `just opt-out`, which is the assertion the boundary is made of. Adopters name
   `kafkaman-otel` directly. This knowingly breaks the facade's "depend on this
   crate alone" promise for this one capability.

4. **`RuntimeBuilder` gains nothing.**
   No `with_telemetry`, no flush hook, no OpenTelemetry type anywhere in
   kafkaman's configuration or composition surface. Coupling stays at zero:
   `kafkaman` compiles and behaves identically whether or not `kafkaman-otel` is
   in the graph.

5. **The public surface is two tiers.**
   `init(service_name)` for the common case, and `builder(service_name)` →
   `Telemetry` for a host that owns its subscriber and wants the layers back.
   `build()` installs the *providers* and returns layers; only `init` installs a
   subscriber. A crate that seizes the global subscriber is unusable by anyone
   who already has one.

6. **Transport is OTLP over HTTP only, plaintext, and TLS is the adopter's to
   enable.** Plaintext HTTP with the blocking `reqwest` client, matching
   `telemetry-backend-and-example-topology`.

   A `tls` feature was written and then removed, and the reason is worth keeping:
   `opentelemetry-otlp/reqwest-rustls` resolves to `reqwest/default-tls`, which on
   `reqwest` 0.13 is rustls with the `aws-lc-rs` provider — a cmake-and-C-toolchain
   build dependency. Cargo records a feature's dependencies in the lock file
   whether or not the feature is enabled, so *declaring* it would make every
   `--all-features` invocation in this workspace — `just lint`, and the CI clippy,
   doc, and test jobs — build `aws-lc-rs`, for a capability nothing here uses.

   An adopter enables it from their own manifest instead, on the same `0.32`
   line; Cargo unifies features across the graph, so it reaches the exporter this
   crate links with no code change. That also leaves the crypto provider and root
   store as their choice, which is the right place for it.

   No gRPC: it is a second exporter constructor and a second untested transport,
   and the collector ecosystem takes HTTP on 4318 as readily as gRPC on 4317.

7. **The crate carries a companion version commitment, stated in its own docs.**
   It tracks `opentelemetry`'s release cadence and breaks when
   `opentelemetry` breaks. It does not claim the stability the rest of the
   workspace aims for, and its README and crate docs say so rather than leaving
   an adopter to discover it at the next 0.x bump.

## Amendment Required Elsewhere

This decision contradicts `telemetry-pipeline-ownership.decision.md` item 6 as
written — "Exporter dependencies live in exactly two places: `apps/` and
`tests/`." (That item now reads `examples/`, which is what `apps/` was renamed
to on 2026-08-24; the quotation above is left as it stood when this was
written.) That decision is amended rather than superseded; the amendment is
recorded in its own *Amendments* section, following the precedent set by its
2026-08-25 `tracing-opentelemetry` entry. The substance of item 1 is untouched.

## The Hazard

**A convenience crate that pins a version is a liability the moment the version
moves.** Every signature in the surface names an `opentelemetry` 0.32 type:
`SdkMeterProvider`, `SdkTracerProvider`, `SdkLoggerProvider`, and the layer
types from `tracing-opentelemetry` 0.33. There is no abstraction that survives
0.33 — a trait would still have 0.32 types in its associated items, and a
version-agnostic surface would have to hide the layers, which is precisely what
tier two exists to expose.

The ecosystem releases these in lockstep: `opentelemetry`, `opentelemetry_sdk`,
`opentelemetry-otlp`, `opentelemetry-appender-tracing`, and
`tracing-opentelemetry` all bump together, several times a year, with breaking
changes. Every one of those bumps is a `kafkaman-otel` release, and an adopter
who cannot take the bump on our schedule cannot use the crate.

What makes this acceptable rather than merely accepted is that **the fallback is
complete**. An adopter on the wrong version copies the crate's source — which is
what `examples/telemetry` was — and loses nothing but the convenience. There is
no capability behind this crate that `kafkaman` does not offer without it. That
is the property to preserve across every future change to it: the moment
`kafkaman-otel` becomes load-bearing, the version pin stops being a convenience
cost and becomes an adoption barrier.

## Options Considered

**A. Leave the pipeline in `examples/telemetry`.**
Zero version commitment, and the file is copyable today. Rejected: it leaves
every adopter to rediscover the two traps we already fell into with the code in
front of us — the plaintext-only transport, and the shutdown ordering that a `?`
in a `select!` arm silently skipped in both binaries.

**B. Extract to `kafkaman-otel`, no facade re-export.** *Accepted.*
Costs one amendment and the facade promise for one capability. Buys an adopter a
working pipeline in two calls, and gives the `https://` case a home it never had.

**C. Extract and re-export behind a `kafkaman` feature.**
The best UX on offer: `kafkaman::otel::init(..)`, one dependency, one namespace.
Rejected on the gate — `cargo tree -p kafkaman --all-features -i
opentelemetry_sdk` would succeed, and that is the exact condition `just opt-out`
was written to fail. Weakening the gate to allow it would make the boundary
documentation again instead of an assertion, which is the state that let the
`metrics`/`traces` opt-out be false for a while without anything failing.

**D. Define a kafkaman trait, implement it in the crate.**
Keeps the facade honest and suggests pluggability. Rejected as ceremony: one
implementation, and the alternative implementation it gestures at is already
available by not using the crate. It would also fail for the same reason as C if
the trait were to carry SDK types.

**E. Ship gRPC alongside HTTP.**
Common in collector deployments. Deferred rather than rejected: it is a distinct
exporter constructor behind a `cfg`, doubling the transport surface for a path
nothing in this project exercises. Revisit when an adopter needs 4317, and add it
with a test rather than on speculation.

## Consequences

- The workspace gains a member whose dependency graph is deliberately unlike
  every other one under `crates/`. The `just opt-out` list becomes an allowlist
  by omission, and gains a comment saying so, because a list that silently omits
  one entry invites a well-meaning correction.
- `examples/order` and `examples/product` become the crate's first consumers,
  which keeps it honest: a change that breaks the surface breaks the example.
- The `metrics`/`traces` opt-out story is unaffected. An adopter who wants no
  OpenTelemetry in their graph still turns off two `kafkaman` features and does
  not name this crate.
- A future `opentelemetry` bump becomes a two-step release: the workspace pin,
  then `kafkaman-otel`. This is new release-process work that did not exist when
  the pipeline lived in an example.

## Verification

- `just opt-out` passes with all seven library crates free of the SDK, and
  `kafkaman-otel` deliberately outside the checked set.
- `cargo tree -p kafkaman --all-features -i kafkaman-otel` fails, proving the
  facade does not reach the crate.
- A test asserts `init` with no OTLP environment variable installs no provider
  and returns a `Telemetry` whose `shutdown` succeeds.
- A test asserts the tier-two layers compose into a host-owned registry.
- `just examples observe` still exports metrics, traces, and logs to
  Elasticsearch through the extracted crate.
