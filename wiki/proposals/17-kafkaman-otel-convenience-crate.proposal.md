# kafkaman-otel Convenience Crate

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-27
- Category: Observability developer experience
- Scope: Proposes extracting the OpenTelemetry pipeline the example services install into an opt-in `kafkaman-otel` crate, so an adopter reaches a working metrics/traces/logs export in one call instead of transcribing 243 lines, without any library crate acquiring an SDK dependency.
- Sources:
  - crates/kafkaman-otel/src/lib.rs (extracted from the since-deleted examples/telemetry)
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/plans/opentelemetry-completion.plan.md (Phase 4, step 4)
  - wiki/decisions/telemetry-backend-and-example-topology.decision.md
  - justfile (`opt-out` recipe)
  - Cargo.lock (resolved `reqwest` 0.13.4 feature set)
- Related:
  - wiki/decisions/kafkaman-otel-extraction.decision.md
  - wiki/plans/kafkaman-otel-extraction.plan.md
  - wiki/proposals/13-telemetry-pipeline-completion.proposal.md
  - wiki/compatibility/kafkaman-otel-surface.compat.md

## Why This Proposal Exists

Proposal 13 asked *does any of this telemetry reach a backend?* The answer was
no, and the OpenTelemetry completion plan fixed it: the example services now
install a `MeterProvider`, a `TracerProvider`, a `LoggerProvider`, a
`tracing-opentelemetry` span bridge, and an OTel log appender, and export them
over OTLP/HTTP.

This proposal asks the question that follows: **what does an adopter have to
write to get the same thing?**

The answer is 243 lines, and we know the number exactly, because on 2026-08-27
both example services were found to contain byte-identical copies of it. That
duplication was resolved by extracting `examples/telemetry`. The extraction is
the evidence: the wiring is not a per-service concern that each host naturally
shapes differently. It is the same file twice.

Phase 4 step 4 of the OpenTelemetry completion plan anticipated this exactly:

> A `kafkaman-otel` convenience crate is deferred, not rejected. One call that
> builds the standard pipeline is attractive, but it pins adopters to our choice
> of exporter crate versions — a real cost in an ecosystem that releases breaking
> 0.x versions in lockstep. Document the wiring first; extract the crate only if
> the example proves it is genuinely repetitive.

The example has now proved it repetitive. This proposal is the extraction that
condition was written to authorize.

## Evidence

| Claim | Verification |
| --- | --- |
| The wiring is identical across services | `diff examples/order/src/telemetry.rs examples/product/src/telemetry.rs` reported no difference across all 161 lines before extraction |
| Only the service name differs | The two call sites passed `"kafkaman-example-order"` and `"kafkaman-example-product"`; nothing else varied |
| The ordering contract is easy to get wrong | `telemetry-pipeline-ownership.decision.md` item 4 documents it; nothing enforces it, and the API offers no way to detect a violation (option E, deferred) |
| The shutdown ordering is easy to get wrong | A `?` inside a `select!` arm skipped both the drain and the flush in both binaries, and shipped; found in review 2026-08-27 |
| Plaintext-only export is an invisible trap | The workspace resolves `reqwest` 0.13.4 with no `rustls` or `native-tls` in its dependency list, so an `https://` endpoint fails at run time with nothing failing at build time |
| No SDK is in any library crate an adopter reaches | `just opt-out` asserts it across `--all-features` for the seven facade-reachable `crates/` members |

The fourth and fifth rows are the ones that should carry weight. The first three
are ergonomics — an adopter who writes 243 lines gets a working pipeline. The
last two are correctness traps that a careful adopter still falls into, because
neither the type system nor the build says anything.

## What Is Proposed

An opt-in crate, `crates/kafkaman-otel`, holding exactly what
`examples/telemetry` holds today, with a public surface in two tiers.

**Tier one — the 90% case.** One call before the runtime, one call after the
drain:

```rust
let telemetry = kafkaman_otel::init("order-service")?;
let result = run(options).await;
let flushed = telemetry.shutdown();
result?;
flushed
```

Signal selection stays environment-driven, per the OTLP specification:
`OTEL_EXPORTER_OTLP_ENDPOINT` enables all three, a signal-specific variable
enables only its own, and nothing set installs a plain `fmt` subscriber with a
`shutdown` that is a no-op. That last behaviour is the non-obvious part and the
main thing the crate carries — it is what keeps a default `just examples demo`
from logging export failures at a backend that is not running.

**Tier two — the host owns its subscriber.** A crate that installs the global
subscriber and nothing else is unusable by anyone who already has one, which is
most non-trivial applications. So `build()` installs the *providers* — which is
what the ordering contract concerns — and returns the layers:

```rust
let telemetry = kafkaman_otel::builder("order-service")
    .service_version(env!("CARGO_PKG_VERSION"))
    .resource_attribute("deployment.environment", "staging")
    .metric_interval(Duration::from_secs(30))
    .without_logs()
    .build()?;

tracing_subscriber::registry()
    .with(my_env_filter)
    .with(my_json_layer)
    .with(telemetry.trace_layer())
    .with(telemetry.log_layer())
    .init();
```

`init` is a thin wrapper over `builder(..).build()` plus a default registry.

## What Is Deliberately Not Proposed

**No hook into `RuntimeBuilder`.** The obvious-looking move —
`RuntimeBuilder::with_telemetry(..)` — is the one thing that cannot happen.
`RuntimeBuilder` lives in `crates/kafkaman`, the first name in the `just opt-out`
forbidden list, and an SDK there decides the host's provider, exporter, and
version with no way to take it back. This is `telemetry-pipeline-ownership`
items 1 and 6, and it is asserted mechanically rather than trusted.

**No re-export from the facade.** `kafkaman = { features = ["otel"] }` would put
the SDK back into `crates/kafkaman`'s `--all-features` graph and fail the same
gate. `kafkaman-otel` is named directly by adopters who want it. This is a real
break in the facade's "depend on this crate alone" promise, and it is the price
of the boundary rather than an oversight.

**No shutdown hook on `Serve`.** Considered and rejected. It would not have
caught the 2026-08-27 defect — that was a `?` skipping `shutdown()` entirely, and
a hook hanging off `shutdown()` dies with it. It also costs manual `Debug` impls
on `Serve` and `RunningService`, because a boxed closure kills the derives the
workspace lints require.

**No gRPC transport.** OTLP over HTTP only. A `grpc-tonic` path is a different
exporter constructor and a second untested transport; the collector ecosystem
accepts HTTP on 4318 as readily as gRPC on 4317, and an adopter who needs Tonic
writes the pipeline themselves — which is the fallback the whole design already
accepts.

**No enforcement of the startup ordering.** `telemetry-pipeline-ownership`
option E deferred warning-on-noop because the OpenTelemetry API exposes no
reliable way to ask whether the installed provider is the no-op one. Nothing here
changes that. The contract stays documentation plus a test.

## The Cost, Stated Plainly

Every signature in the proposed surface names an `opentelemetry` 0.32 type.
There is no escape hatch that survives 0.33: an adopter pinned to a different
version cannot use the crate at all, and must fall back to writing the pipeline
by hand.

This is the cost Phase 4 step 4 named, and accepting this proposal accepts it.
Two things make it bearable:

1. **The fallback is complete, not degraded.** `kafkaman` compiles and runs
   identically with or without this crate in the graph. An adopter on the wrong
   `opentelemetry` version loses a convenience, not a capability.
2. **The crate is versioned as a companion, not as part of the library.** It
   tracks `opentelemetry`'s release cadence and says so, rather than pretending
   to the stability the rest of the workspace aims for.

## Options Considered

**A. Leave it in `examples/telemetry`.** Zero commitment, and the file is
copyable today. Rejected: it makes every adopter responsible for the two traps
in the evidence table — the plaintext-only transport and the shutdown ordering —
each of which we already got wrong once with the code in front of us.

**B. Extract to `kafkaman-otel`.** *Accepted.* The subject of this proposal.

**C. Extract, and re-export through the facade behind a feature.** The nicest
UX: `kafkaman::otel::init(..)`. Rejected on the gate — `cargo tree -p kafkaman
--all-features -i opentelemetry_sdk` would succeed, which is exactly the
condition `just opt-out` exists to fail.

**D. Put the pipeline behind a trait kafkaman defines, implemented by the crate.**
Keeps the facade honest and lets adopters swap implementations. Rejected as
ceremony: the trait would have one implementation, and the abstraction it buys —
"some other telemetry pipeline" — is already available by not using the crate.

## Consequences

- `crates/kafkaman-otel` becomes the third place an exporter dependency may
  appear, after `examples/` and `tests/`. This contradicts
  `telemetry-pipeline-ownership` item 6 as written and requires an amendment to
  it, following the precedent of that decision's 2026-08-25 `tracing-opentelemetry`
  amendment.
- The `just opt-out` forbidden list gains a comment explaining why one crate
  under `crates/` is deliberately absent from it. The list is an allowlist by
  omission, which is worth stating so the next reader does not "fix" it.
- `examples/telemetry` is deleted; both example services depend on
  `kafkaman-otel` instead, and become the crate's first consumer and living
  documentation.
- Adopters gain an `https://` OTLP endpoint story the example never had. Not as
  a crate feature: declaring one would drag `aws-lc-rs` into every
  `--all-features` build in the workspace. It is enabled from the adopter's own
  manifest and documented in the crate docs. See the extraction decision, item 6.

## Verification

- `just opt-out` still passes, with `kafkaman-otel` outside the checked set and
  all seven library crates still free of the SDK.
- The two example services build and run against the crate, and
  `just examples observe` still exports to Elasticsearch.
- A test asserts that with no OTLP environment variable set, `init` installs no
  provider and `shutdown` succeeds.
- A test asserts the builder's layers compose into a host-owned registry.
