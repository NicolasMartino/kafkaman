# kafkaman-otel Extraction Plan

- Document Class: Plan
- Status: Completed
- Date: 2026-08-27
- Completed: 2026-08-27 (all five phases; see *Outcome* below)
- Category: Delivery execution
- Scope: Extract `examples/telemetry` into the opt-in `crates/kafkaman-otel` crate with a two-tier surface, repoint both example services at it, move the SDK-boundary assertion, and document the version commitment.
- Sources:
  - wiki/decisions/kafkaman-otel-extraction.decision.md
  - wiki/proposals/17-kafkaman-otel-convenience-crate.proposal.md
  - crates/kafkaman-otel/src/lib.rs (extracted from the since-deleted examples/telemetry)
  - justfile (`opt-out` recipe)
  - Cargo.toml (workspace members and dependency pins)
- Related:
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/plans/opentelemetry-completion.plan.md
  - wiki/compatibility/kafkaman-otel-surface.compat.md

## Starting Point

`examples/telemetry` exists and works. It was extracted on 2026-08-27 from two
byte-identical copies in the example binaries, and both services already call
`example_telemetry::init(name)` and `Telemetry::shutdown()` in the correct order.

So this is a move plus a surface, not a build from nothing. The pipeline logic —
signal selection, provider construction, layer wiring, ordered shutdown — is
written and exercised. What it lacks is tier two, a typed error, a TLS story, and
a home an adopter can name.

## Phase 1 — The crate

Create `crates/kafkaman-otel` with the module body from `examples/telemetry`,
then add what a shipped crate needs and an example file did not.

**Manifest.** Version-pinned through the workspace like every other member.
Depends on no kafkaman crate — the leaf property that decision 2 rests on.

~~```toml
[features]
default = []
# Adds a rustls backend to the OTLP HTTP client so an `https://` endpoint works.
tls = ["opentelemetry-otlp/reqwest-rustls"]
```~~

**Struck — the `tls` feature was written and then removed.** Declaring it drags
`aws-lc-rs` into every `--all-features` build in the workspace for a capability
nothing here uses; the crate ships with no feature and documents the one-line
manifest entry an adopter adds instead. The reasoning is in *Outcome* below and
in `wiki/decisions/kafkaman-otel-extraction.decision.md` item 6.

Plaintext, because the reference topology is a collector on the same network. The
resolved `reqwest` carries no TLS backend at all, which fails at run time with
nothing failing at build time — so the crate docs say so rather than leaving it
to be discovered.

**A typed error.** `thiserror`, matching the workspace. The example returned
`Box<dyn Error>`, which is right for a binary and wrong for a library: an adopter
cannot match on it to decide whether a failed exporter build should be fatal.
Variants for exporter construction, subscriber installation, and provider
shutdown.

**Tier two.** `builder(service_name) -> Builder` with `service_version`,
`resource_attribute`, `metric_interval`, and `without_metrics` /
`without_traces` / `without_logs`. `Builder::build()` installs the providers and
returns `Telemetry`; `Telemetry::trace_layer()` and `log_layer()` hand the layers
back for a host-owned registry.

The layer accessors are generic over the subscriber they compose into:

```rust
pub fn trace_layer<S>(&self) -> Option<impl tracing_subscriber::Layer<S>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
```

Both take `&self` and construct a fresh layer per call, so a host can compose
them and still own the `Telemetry` handle for shutdown.

**Tier one.** `init(service_name)` = `builder(..).build()` plus a default
registry with an `EnvFilter` and an `fmt` layer. It stays the documented path.

**Crate docs carry the two contracts.** The startup ordering from
`telemetry-pipeline-ownership` item 4 — install before building any kafkaman loop
— and the companion version commitment from decision 7. Both belong in
`//!` docs where an adopter reading docs.rs meets them, not only in the wiki.

## Phase 2 — Move the boundary assertion

`just opt-out` and the CI step that mirrors it both iterate a hard-coded list of
the seven facade-reachable crates. `kafkaman-otel` must stay out of it, and the omission must be
explained where the list is, or it reads as a bug.

Add the comment, and add the positive assertion decision 3 implies:

```bash
# kafkaman-otel is absent on purpose: it exists to hold the exporter.
# The boundary is that nothing reachable from `kafkaman` links an SDK,
# which the check below asserts directly.
if cargo tree -p kafkaman -e normal --all-features -i kafkaman-otel >/dev/null 2>&1; then
  echo "kafkaman reaches kafkaman-otel; the facade must not re-export it" >&2
  exit 1
fi
```

That check is the one that makes the amendment safe. Without it, "the facade does
not re-export it" is a comment, and comments do not fail builds.

## Phase 3 — Repoint the examples

Delete `examples/telemetry`. Both service manifests swap
`example-telemetry = { path = "../telemetry" }` for
`kafkaman-otel = { path = "../../crates/kafkaman-otel" }`, and both `main.rs`
call `kafkaman_otel::init`. The call sites are otherwise unchanged — the
shutdown ordering fixed earlier today is already correct.

Remove `examples/telemetry` from the workspace members list and add
`crates/kafkaman-otel`.

## Phase 4 — Tests

The example never had tests, because an example does not need them. A crate does.

1. **No endpoint configured.** `init` installs no provider, returns a
   `Telemetry` reporting itself disabled, and `shutdown` succeeds.
2. **Signal selection.** The generic `OTEL_EXPORTER_OTLP_ENDPOINT` enables all
   three; a signal-specific variable enables only its own; a blank value counts
   as unset.
3. **Tier two composes.** The layers from `build()` go into a host-owned
   registry and the result installs.

Environment-variable tests mutate process-global state, so they run in one
serialized test rather than three racing ones — the same hazard
`telemetry-pipeline-ownership` describes for global providers, in a different
guise.

## Phase 5 — Documentation

`wiki/compatibility/kafkaman-otel-surface.compat.md` records the new public
surface, the fact that it is additive to everything existing, and the version
commitment. `wiki/index.md` gains the four new pages. `wiki/log.md` records the
operation.

`examples/README.md` swaps its `examples/telemetry` reference for the crate and
keeps the plaintext-HTTP warning, now pointing at the `tls` feature as the
supported fix rather than describing a limitation with no remedy.

## Verification

- `just opt-out` — the seven facade-reachable crates clean, plus the new "facade
  does not reach kafkaman-otel" assertion.
- `just features` — all seven `kafkaman` feature combinations, unchanged by this
  work and therefore a regression check on it.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps`.
- `cargo test -p kafkaman-otel` — the four tests above.
- `cargo test --workspace --lib` — no regression against the 238 currently
  passing.
- ~~`cargo check -p kafkaman-otel --features tls`~~ — dropped with the feature;
  see *Outcome*.
- `just examples observe` end to end, which is also
  `wiki/plans/opentelemetry-completion.plan.md` Phase 4's still-pending exit.

**Exit:** an adopter reaches a working OTLP pipeline with one dependency and two
calls, `just opt-out` still proves no SDK is reachable from `kafkaman`, and both
examples run through the extracted crate.

## Outcome

All five phases landed on 2026-08-27. Two things went differently than planned,
both worth recording.

**The `tls` feature was written and then removed.** Phase 1 specified
`tls = ["opentelemetry-otlp/reqwest-rustls"]`. Declaring it broke `cargo check`
outright: `reqwest-rustls` resolves to `reqwest/default-tls`, which on `reqwest`
0.13 is rustls with the `aws-lc-rs` provider, and re-resolving the lock file
required `aws-lc-rs` 1.18 — a cmake-and-C-toolchain build dependency.

The build failure was a stale index, and refreshing it would have fixed the
resolve. The design problem it exposed did not go away: Cargo records a feature's
dependencies in the lock file whether or not the feature is enabled, so declaring
`tls` makes every `--all-features` invocation in this workspace — `just lint`,
and the CI clippy, doc, and test jobs — build `aws-lc-rs`, for a capability
nothing here uses. The feature was dropped in favour of a documented one-liner an
adopter puts in their own manifest, which reaches the same exporter through
feature unification and leaves the crypto provider their choice. The decision's
item 6 was rewritten to match.

**The tests needed a lock, not just grouping.** Phase 4 assumed grouping the
environment assertions into one `#[test]` would be enough. It was not: two of the
four tests set variables and the other two assert none are set, so all four
raced, visibly — consecutive runs reported a different number of failures. They
now share a `static ENV_LOCK: Mutex<()>` taken for the whole of each test body,
with poisoning recovered so one panicking test does not turn the rest into lock
errors. Confirmed stable over five consecutive runs.

Two smaller corrections found by the toolchain rather than by design:
`log_layer` needed a `for<'a> LookupSpan<'a>` bound the plan's sketch omitted,
and the crate needed the
`#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]`
opt-out every other crate here carries, since the workspace lints warn on
`expect` in a library.

Verification, all passing: `cargo fmt --check`, `cargo clippy --workspace
--all-targets --all-features -D warnings`, `RUSTDOCFLAGS="-D warnings" cargo doc
--workspace --all-features --no-deps`, `just opt-out` (including the new
facade-unreachability assertion), `just features` (seven combinations),
`cargo test -p kafkaman-otel` (4 unit + 2 doctests), and `cargo test --workspace
--lib` at 242 passed, up from 238 by exactly the four new tests.

Still pending, and unchanged by this work:
`wiki/plans/opentelemetry-completion.plan.md` Phase 4's end-to-end Kibana
verification.
