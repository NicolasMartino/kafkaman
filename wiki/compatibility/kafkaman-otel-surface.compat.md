# kafkaman-otel Surface Compatibility

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-27
- Category: Observability pipeline API
- Scope: Records the public surface of the new `kafkaman-otel` crate, why it is not reachable through the `kafkaman` facade, the version commitment it carries, and what an adopter has to do to get TLS.
- Sources:
  - crates/kafkaman-otel/src/lib.rs
  - crates/kafkaman-otel/Cargo.toml
  - justfile (`opt-out` recipe)
  - .github/workflows/ci.yml (opt-out step)
- Related:
  - wiki/decisions/kafkaman-otel-extraction.decision.md
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/plans/kafkaman-otel-extraction.plan.md
  - wiki/proposals/17-kafkaman-otel-convenience-crate.proposal.md
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Public API Changes

**Purely additive, and in a new crate.** Nothing in `kafkaman` or any existing
crate changed shape, was renamed, or was re-gated. An application that never
names `kafkaman-otel` sees no difference of any kind — same dependency graph,
same features, same behaviour.

### New crate: `kafkaman-otel`

```toml
kafkaman-otel = "0.1"
```

| Item | Signature |
| --- | --- |
| `init` | `fn init(service_name: impl Into<Cow<'static, str>>) -> Result<Telemetry, Error>` |
| `builder` | `fn builder(service_name: impl Into<Cow<'static, str>>) -> Builder` |
| `Builder::service_version` | `fn(self, impl Into<Cow<'static, str>>) -> Self` |
| `Builder::resource_attribute` | `fn(self, impl Into<Cow<'static, str>>, impl Into<opentelemetry::Value>) -> Self` |
| `Builder::metric_interval` | `fn(self, Duration) -> Self` |
| `Builder::without_metrics` / `without_traces` / `without_logs` | `fn(self) -> Self` |
| `Builder::build` | `fn(self) -> Result<Telemetry, Error>` |
| `Telemetry::is_enabled` | `fn(&self) -> bool` |
| `Telemetry::service_name` | `fn(&self) -> &str` |
| `Telemetry::trace_layer` | `fn<S>(&self) -> Option<impl Layer<S>>` where `S: Subscriber + for<'a> LookupSpan<'a>` |
| `Telemetry::log_layer` | `fn<S>(&self) -> Option<impl Layer<S>>` — same bounds |
| `Telemetry::shutdown` | `fn(self) -> Result<(), Error>` |
| `Error` | `#[non_exhaustive] enum { Exporter { signal, source }, Subscriber(..), Shutdown { signal, source } }` |

`Error` is `#[non_exhaustive]`, so adding a variant later is not a breaking
change. Match with a `_` arm.

### It is not on the facade, and will not be

There is no `kafkaman::otel`, and no `kafkaman = { features = ["otel"] }`. This
is the one capability the facade's "depend on this crate alone" promise does not
cover, and it is deliberate: re-exporting would put `opentelemetry_sdk` into
`crates/kafkaman`'s `--all-features` dependency graph, which is exactly the
condition `just opt-out` exists to fail.

Both the `justfile` and the CI job now assert it directly rather than trusting
the comment:

```bash
if cargo tree -p kafkaman -e normal --all-features -i kafkaman-otel; then
  echo "kafkaman reaches kafkaman-otel; the facade must not re-export it" >&2
  exit 1
fi
```

## Migration

**For existing adopters: none.** No action, no change in behaviour.

**For anyone who copied `examples/telemetry`:** that directory is gone, absorbed
into this crate. The copy still works — it was self-contained, which was the
point — but swapping to the crate gets you the typed error, the builder tier, and
the tests. The call sites are compatible in shape:

```diff
- let telemetry = example_telemetry::init("order-service")?;
+ let telemetry = kafkaman_otel::init("order-service")?;
```

The one behavioural difference: `shutdown` now returns `Result<(), Error>`
rather than `Result<(), Box<dyn Error + Send + Sync>>`, so a caller boxing it
needs `.map_err(Into::into)`. Both example services do exactly that.

## Installation Is All-Or-Nothing, And Once Per Process

**`build` installs no global until every fallible step has succeeded.** It
constructs all three providers first and sets the globals afterwards, so an `Err`
means nothing was installed. The earlier shape installed each provider as it was
built, which left a live `MeterProvider` — and its exporter thread — running with
no handle to reach it when the tracer's exporter failed, because the `?` returned
before any `Telemetry` existed. A caller may now treat a failure as non-fatal and
carry on unexported.

`init` extends that to the subscriber: if the global subscriber is already taken,
it shuts the providers down before returning `Error::Subscriber` rather than
stranding them.

**Call it once per process.** The providers are global. A second `build`
replaces them while the first `Telemetry` still believes it owns them, and the
displaced providers then flush on whichever handle is shut down first. This is a
property of the OpenTelemetry globals, not of this crate, and nothing here can
detect it — the API exposes no way to ask what is currently installed. It is
stated in the crate docs for the same reason it is stated here.

## `OTEL_METRIC_EXPORT_INTERVAL` Is Honoured

The precedence is: an explicit `Builder::metric_interval` call, then the OTLP
specification's `OTEL_METRIC_EXPORT_INTERVAL`, then this crate's 15s default.

The earlier shape passed its default to `PeriodicReader::with_interval`
unconditionally, which overrode the environment variable the SDK reads for
itself — so an operator following the specification changed nothing and got no
diagnostic. The default stays 15s rather than deferring to the SDK's 60s, because
60s does not fit inside a typical container stop grace period and the final flush
has to.

`Duration::ZERO` is not zero: the SDK ignores a zero interval and falls back to
its own 60s. The crate documents that rather than rejecting it, since the SDK's
behaviour is the one that governs.

## Two Contracts This Surface Carries

**Install before building any kafkaman loop.** kafkaman creates its metric
instruments as each loop starts, and the OpenTelemetry API binds an instrument
to whichever provider is installed at that moment, permanently. A loop built
first is silent for its whole life, with no error and no log line. This is
`telemetry-pipeline-ownership` item 4, restated in the crate docs where an
adopter on docs.rs will meet it.

**Shut down after the runtime drains, on every exit path.** The last part is the
one that bites: a `?` inside a `tokio::select!` arm returns from the enclosing
function immediately, skipping both the drain and the flush. That shipped in
both example binaries and was found in review on 2026-08-27. The crate cannot
prevent it — see `kafkaman-otel-extraction` decision 4 on why no hook was added —
so it documents it instead.

## Versioning

`kafkaman-otel` is a **companion crate, not part of kafkaman's stability
promise.** Every signature above names an `opentelemetry` 0.32 type, and the
OpenTelemetry Rust crates release breaking versions in lockstep several times a
year. Each of those is a breaking release here, and a `kafkaman` release does not
imply one.

An adopter pinned to a different `opentelemetry` line cannot use this crate. The
fallback is complete rather than degraded: copy `crates/kafkaman-otel/src/lib.rs`
and own it, which is what `examples/telemetry` was. Nothing behind this crate is
unavailable without it, and preserving that is a constraint on how it may grow.

## Transport And TLS

OTLP over HTTP/protobuf, **plaintext**. The exporter's HTTP client resolves with
no TLS backend, so an `https://` endpoint fails at run time with nothing failing
at build time.

There is deliberately **no `tls` feature**. One was written and removed:
`opentelemetry-otlp/reqwest-rustls` resolves to `reqwest/default-tls`, which on
`reqwest` 0.13 is rustls with the `aws-lc-rs` provider — a cmake-and-C-toolchain
build dependency. Merely declaring the feature puts it in the lock file's
potential graph, so every `--all-features` invocation in this workspace would
have to build it for a capability nothing here uses.

Enable it from the adopter's manifest instead, on the same `0.32` line:

```toml
kafkaman-otel = "0.1"
opentelemetry-otlp = { version = "0.32", features = ["reqwest-rustls"] }
```

Cargo unifies features across the graph, so it reaches the exporter this crate
links and no code changes. Done this way round, the crypto provider and root
store stay the adopter's choice.

## Not Offered

- **gRPC.** HTTP only. A `grpc-tonic` path is a second exporter constructor and
  a second untested transport; the collector ecosystem accepts HTTP on 4318 as
  readily as gRPC on 4317. Deferred, not rejected — see
  `kafkaman-otel-extraction` option E.
- **A `RuntimeBuilder` hook.** No `with_telemetry`, no flush hook, no
  OpenTelemetry type in kafkaman's configuration or composition surface.
- **Startup-ordering enforcement.** The OpenTelemetry API exposes no reliable
  way to ask whether the installed provider is the no-op one, so the contract
  stays documentation plus a test.
