# Kafka Broker Authentication

- Document Class: Proposal
- Status: Proposed
- Date: 2026-09-08
- Category: Transport security
- Scope: Gives kafkaman a way to authenticate to a broker — SASL_SSL with
  SCRAM-SHA-256/512, configured through a new `[kafka]` section — closes a
  confirmed credential leak in config parse errors, closes the deferred
  ACL-denied `CreateTopics` path by making an authenticated test broker possible,
  and settles where broker credentials live.
- Sources:
  - Cargo.toml
  - Cargo.lock
  - crates/kafkaman-rdkafka/src/topics.rs
  - crates/kafkaman-rdkafka/src/publisher.rs
  - crates/kafkaman-rdkafka/src/consumer.rs
  - crates/kafkaman-config/src/config.rs
  - crates/kafkaman-config/src/error.rs
  - crates/kafkaman/src/runtime/builder.rs
  - kafkaman.example.toml
  - examples/Dockerfile
  - examples/order/src/boot.rs
  - justfile
  - tests/durable-send/src/containers.rs
- Related:
  - wiki/decisions/configuration-and-environment-model.decision.md
  - wiki/decisions/topic-convergence-and-rebuild.decision.md
  - wiki/decisions/kafkaman-otel-extraction.decision.md
  - wiki/compatibility/topic-convergence-api.compat.md
  - wiki/proposals/23-topic-convergence-and-environment-provisioning.proposal.md

## Context

kafkaman cannot authenticate to a broker. Every connection in the workspace is a
bare `bootstrap.servers` string, and there is no `sasl`, `security.protocol`, or
`ssl.*` reference anywhere under `crates/`, `examples/`, or `tests/`.

This is not merely unconfigured. `Cargo.toml:78` declares
`rdkafka = { version = "0.36", features = ["tokio"] }`, and `Cargo.lock:2393`
resolves `rdkafka-sys 4.10.0+2.12.1` with `libc` and `libz-sys` alone — no
`openssl-sys`, no `sasl2-sys`. In that crate's `build.rs` there are two
`build_librdkafka` functions, and **the active one is the `configure` branch**,
because `cmake-build` is not enabled here:

```rust
#[cfg(not(feature = "cmake-build"))]           // build.rs:107 — this is the one that runs
fn build_librdkafka() {
    ...
    if env::var("CARGO_FEATURE_SSL").is_ok() {
        configure_flags.push("--enable-ssl".into());
        ...
    } else {
        configure_flags.push("--disable-ssl".into());   // build.rs:128
    }
```

librdkafka is therefore configured `--disable-ssl`, and SCRAM — whose HMAC comes
from OpenSSL — is absent from the binary rather than switched off in config. No
amount of configuration reaches it.

The `cmake-build` branch at `build.rs:220` is worth reading as corroboration
because it names the linkage explicitly rather than leaving it to librdkafka's
`configure` autodetection — `CARGO_FEATURE_SSL` there defines both `WITH_SSL=1`
and `WITH_SASL_SCRAM=1` — but it is not the mechanism in play. Reproduce the
active state with:

```
cargo tree -p kafkaman-rdkafka -e normal --all-features -i openssl-sys   # finds nothing
```

**Why it matters, and why it is a contradiction rather than a gap.** Decision 4
of `topic-convergence-and-rebuild.decision.md` reads:

> **`verify`, not `create`, is the default.** Application principals are
> routinely denied `CreateTopics` by ACL, so creation-at-boot cannot be the
> baseline behaviour of a library.

The default topic mode is chosen *because* production clusters are ACL-secured.
`topics.rs:177-190` goes further and writes an error for that world — "the
configured principal is not authorized for this topic". There is no configured
principal, and there cannot be one. kafkaman reasons carefully about authorized
clusters while being unable to connect to one.

**What an adopter on such a cluster faces today.** `RdkafkaPublisher::new`
(`publisher.rs:26`) and `RdkafkaConsumer::new` (`consumer.rs:132`) both accept a
pre-built rdkafka client, so a host willing to hand-wire can configure SASL for
the publish and consume paths. `TopicAdmin` has no such constructor —
`from_brokers` (`topics.rs:43`) is its only one, and it builds its `ClientConfig`
internally. The single escape is `[topics] mode = "off"`, which the accepted
decision calls the escape hatch "for clusters that deny metadata reads" and which
"warns at boot so it cannot be silently forgotten." On the exact cluster shape the
topic-convergence decision was designed around, boot-time verification must be
switched off entirely.

**A confirmed leak that this work makes live.** `Config::parse` (`config.rs:68`)
parses the whole document into a `toml::Table`, and `ConfigError::Parse`
(`error.rs:15`) carries `toml::de::Error` through with `#[error("invalid TOML
config: {0}")]`. That error renders the offending source line, and its `Debug`
carries the entire document. Reproduced against `toml 0.8.23`:

```
--- Display ---
TOML parse error at line 2, column 44
  |
2 | sasl_password = "SENTINEL_REVIEW_PASSWORD" trailing
  |                                            ^
--- Debug ---
TomlError { message: "expected newline, `#`", raw: Some("[kafka]\nsasl_password = \"SENTINEL_REVIEW_PASSWORD\" trailing\n"), ... }
```

This is harmless today only because kafkaman's config contract holds no secrets.
The moment `sasl_password` becomes a key, a single typo anywhere in the file
prints the password — and a mistyped config is exactly when the error gets
pasted into a ticket. §4 treats this as the first requirement, not a corollary.

**A second, smaller gap.** The ACL-denied `CreateTopics` path has never been
tested. `topic-convergence-api.compat.md` records it under **Deferred**, and
`topic-convergence.plan.md` gives the reason: "Redpanda's dev-container mode has
no ACLs, so nothing exercises it." An authenticated test broker is the missing
precondition, so the two land together.

## Proposal

Seven changes. §4 is the one that must not be deferred.

### 1. A `[kafka]` section, optional in full

```toml
# OPTIONAL. Absent means a plaintext connection with the broker list supplied by
# the host through `RuntimeBuilder::brokers(..)` — which is what every example in
# this repository does, and what every deployment does today.
[kafka]
brokers = "broker:9093"
security_protocol = "sasl_ssl"        # plaintext (default) | sasl_ssl
sasl_mechanism = "scram-sha-256"      # scram-sha-256 | scram-sha-512
sasl_username = "svc-order"
sasl_password = "rendered-by-ci-cd"
ca_location = "/etc/ssl/certs/ca.pem" # see §6 — not safely optional everywhere
```

`security_protocol` defaults to `plaintext`, so the section may exist to carry
only `brokers`. Validation accumulates, in the style of `RetrySection::resolve`,
because a half-filled block has several independent problems at once:

- under `sasl_ssl`, each missing one of `sasl_mechanism` / `sasl_username` /
  `sasl_password` is its own issue at its own dotted key;
- under `plaintext`, each *present* `sasl_*` or `ca_location` is an error rather
  than ignored. A config that reads as authenticated while connecting in the
  clear is the precise failure this section exists to prevent;
- an empty username or password is an error, and no message echoes the password.

**Compatibility, stated narrowly.** Unknown top-level sections are ignored today,
so every config file in this repository keeps parsing — verified: neither
`examples/order/kafkaman.toml` nor `examples/product/kafkaman.toml` has a
`[kafka]` section. That is the limit of what can be checked here, and it is *not*
a general compatibility claim. `kafkaman.example.toml:25-29` has been publishing
the shape `[kafka] brokers = "..."` with the note that it "is ignored if set", so
a downstream adopter may well have written it and be calling `.brokers(..)` as
well. Under §3 that combination becomes a hard boot error.

This proposal therefore takes `[kafka]` as a **reserved kafkaman-owned section**,
which is a breaking change for that adopter, and owes them:

- a compatibility note recording the reservation, the new `AmbiguousBrokers`
  failure, and the one-line migration (delete the key, or drop the `.brokers(..)`
  call);
- an error message that names both sources and states the fix, so the migration
  is self-service.

Promotion target: `wiki/compatibility/kafka-authentication-api.compat.md`.

### 2. One place that applies security, three clients that use it

There are exactly three `ClientConfig` construction sites in library code:
`publisher.rs:38`, `consumer.rs:163`, `topics.rs:44`. They gain a shared base:

```rust
/// The `ClientConfig` every kafkaman client starts from.
///
/// One function rather than three copies, because a security setting that
/// reaches the producer and not the admin client is a boot that half-connects,
/// and the half that fails is the one that runs later, under load, rather than
/// the one that fails at boot where it would be noticed.
fn base_client_config(brokers: &str, auth: &KafkaAuth) -> ClientConfig
```

Each site gains a `connect(..)` constructor taking the auth, and **`from_brokers`
survives as a plaintext wrapper over it** — that is what keeps the roughly twenty
existing `from_brokers` call sites and the thirteen hand-rolled `ClientConfig`s
in `tests/` untouched.

**Each client keeps its current settings, which are load-bearing and carry their
own comments:** the producer's `enable.idempotence=true`, `acks=all`,
`message.timeout.ms=5000`; the consumer's `group.id`, `auto.offset.reset=earliest`
and `enable.auto.commit=false`; the admin client's
`allow.auto.create.topics=false`. `base_client_config` sets the broker list and
the security properties and nothing else; the per-client settings move into the
respective `connect` unchanged.

Where the types live:

| Type | Crate | Why |
|---|---|---|
| `SecretString` | `kafkaman-core` | A validated newtype primitive, beside `SqlIdentifier` |
| `SecurityProtocol`, `SaslMechanism`, `SaslCredentials`, `KafkaAuth`, `KafkaConnection` | `kafkaman-config` | `string_enum` is `pub(crate)` there, and `ObservabilityConfig` is the precedent for a config-crate type held on `ResolvedConfig` |
| `KafkaSection` | `kafkaman-config` (`sections.rs`) | Beside every other section struct |

`kafkaman-rdkafka` already depends on `kafkaman-config`, so no new edge is
introduced. `ResolvedConfig` (in `kafkaman-sqlx`) gains a `kafka` field, for the
same reason it already holds `topics`: only the transport crate consumes it, but
the decision is configuration, and configuration resolves in one place.

`TopicAdmin` also gains `new(client: AdminClient<DefaultClientContext>)` — the
same escape hatch the publisher and consumer already have, and whose absence is
half of the problem described in Context.

### 3. A broker list supplied twice is refused, not ranked

*Conditional on Open Question 2 keeping `brokers` in the section.* If it does:

| builder | config | result |
|---|---|---|
| set | set | `BuildError::AmbiguousBrokers`, naming both and stating the fix |
| set | absent | the builder's value |
| absent | set | the config's value |
| absent | absent | `BuildError::MissingBrokers`, as today |

Refusing is what keeps this inside the accepted config model rather than eroding
it. That decision rejects kafkaman owning a config framework in these words:
"it does **not** layer sources, merge profiles, or resolve precedence." A
precedence rule would be exactly the thing rejected; an error declines to resolve
and hands the ambiguity back.

Resolving brokers must move *after* `ResolvedConfig::from_config`
(`builder.rs:388`), which today runs after the `MissingBrokers` check at
`builder.rs:379`. Consequence to record in the compat note: a build that both has
a broken config and omits brokers now reports the accumulated config errors
instead of `MissingBrokers`. Nothing asserts the old ordering.

### 4. A credential must not be able to reach a log line

Three layers, in the order they are reachable. **The first is the one the
original draft of this proposal missed**, and it is reachable before any of the
new types exist.

**4a. Parse errors, first.** `ConfigError::Parse` must stop carrying
`toml::de::Error`'s rendering. `toml::de::Error` exposes `message()` and `span()`,
which is enough for a diagnostic that keeps full precision and echoes nothing:

```
invalid TOML in `kafkaman.toml` at line 2, column 44: expected newline, `#`
```

Verified against `toml 0.8.23`: `message()` returns `"expected newline, `#`"` and
`span()` returns `51..52`, from which line and column are computed without
retaining the source. The variant must hold the computed position and message as
owned data rather than the original error, because `#[source]` would let
`{:?}` on the error chain reach the raw document again. The same rule applies to
every nested error source on this path, not only the top one.

**4b. `SecretString`.** A newtype whose `Debug` prints
`SecretString([redacted])`, with no `Display`, no `Serialize`, no `Deref`, and no
`AsRef<str>` — each is a way for a credential to be formatted by accident. The
value is reachable only through `expose_secret()`, named so that
`grep -rn expose_secret` is the audit. Its `Deserialize` is a hand-written
visitor, because serde's default type-mismatch error echoes the offending value.

One residual, stated rather than hidden: a `sasl_password` written as a TOML
*integer* still produces serde's `invalid type: integer 42` before the visitor
runs. A password written as a bare integer is not a plausible mistake, and
closing it would mean hand-walking `toml::Value`. Accepted.

**4c. `Config`'s `Debug`.** Hand-written, printing section names and never the
table.

**The test.** A sentinel password formatted through `{:?}` and `{}` on every type
on the path — `ConfigError` (from a *malformed* document, which is 4a),
`Config`, `KafkaSection`, `ResolvedConfig`, `Runtime`, and every `BuildError`.
Asserted against formatted output rather than a list of types, so a future
`#[derive(Debug)]` on something in that path fails here rather than shipping. The
malformed-TOML case must be in the same test module as the well-formed one; the
guarantee that is easiest to lose is the one that applies before parsing
succeeds.

**What deliberately stays printable.** `Runtime`'s `Debug` prints `brokers`
(`builder.rs:504`) and `BuildError::Transport`'s `Display` interpolates it. Both
stay: a Kafka bootstrap string is `host:port` and carries no inline credential the
way a Postgres URL does, and it is the single most useful field in a boot-failure
trace. Written down so nobody re-derives it, and so nobody "fixes" it later.

### 5. `rdkafka/ssl`, and deliberately not `rdkafka/sasl`

Two separable things. The first is settled; the second is Open Question 1.

**Settled — which feature.** In `rdkafka-sys`'s manifest,
`gssapi = ["ssl", "sasl2-sys"]` and `sasl = ["gssapi"]`. The feature named `sasl`
is an alias for GSSAPI and pulls Cyrus SASL for a mechanism this proposal puts
out of scope; `ssl` alone is what SCRAM needs. `just opt-out` gains an assertion
that `sasl2-sys` stays out of the graph, so the reasoning is enforced rather than
merely recorded.

`ssl`, not `ssl-vendored`: vendored compiles OpenSSL from source in every CI job
and makes OpenSSL CVE response this project's problem instead of the
distribution's.

**Open — whether it is gated.** See Open Question 1. Either way,
`.github/workflows/ci.yml` replaces `libsasl2-dev` with `libssl-dev` and
`pkg-config` at its four install sites. Dropping `libsasl2-dev` is correct on its
own terms: nothing links Cyrus SASL today, and with GSSAPI out of scope nothing
will.

### 6. Trust material is a deployment concern, and this repo currently ships none

`ca_location` being absent means librdkafka falls back to its default trust
lookup, and that default is **platform-dependent**, not universal: Windows uses
the system certificate store, macOS probes known locations, and a dynamically
linked Linux build uses OpenSSL's compiled-in default path — which is empty
unless a CA bundle is installed.

This repository is a concrete example of it being empty. `examples/Dockerfile`
says, verbatim:

> librdkafka here is built with SSL, SASL, zstd and curl support disabled, and no
> dependency in these services speaks TLS, so there is no ca-certificates or
> libsasl2-2 to ship.

Turning on `rdkafka/ssl` makes the first half of that comment false, and the
second half becomes a live defect for anyone deriving a SASL_SSL deployment from
that image. The change therefore includes the runtime image, not only the CI
build: the comment is corrected, and `ca-certificates` is installed in the runtime
stage or the omission is documented as deliberate with the consequence spelled
out.

Testing follows the same split. The generated-CA fixture in §7 proves an
*explicit* `ca_location` works; it cannot prove default trust behaviour, because
it never exercises the default. Omitted `ca_location` needs its own case, and if
that cannot be made deterministic across platforms in CI, the honest outcome is
to document the default as platform-dependent and require `ca_location` in the
example config rather than to claim coverage.

### 7. An authenticated test broker, alongside the plaintext one

A new `redpanda_sasl()` harness in `tests/durable-send/src/containers.rs`, beside
the existing `start_redpanda_on`, gated behind a `redpanda-sasl` feature. The
roughly twenty-nine existing broker-backed tests stay on the plaintext harness,
untouched.

Certificates are generated per run with `rcgen` rather than checked in — fixture
certs expire, and a private key in git trips secret scanners. They are copied in
with testcontainers' `with_copy_to`, which runs between container creation and
start, so the files exist before Redpanda's entrypoint does. Hostname
verification stays on, so the server certificate carries SANs for `127.0.0.1`,
`localhost`, and `redpanda`.

**The tests, chosen so that each one can fail.** A single happy path over a valid
certificate proves almost nothing — it passes whether or not verification is
enabled, and whether or not the second mechanism works.

| Case | What it would catch |
|---|---|
| Round trip on SCRAM-SHA-256 | security properties reach producer, consumer *and* admin |
| Round trip on SCRAM-SHA-512 | the second mechanism's librdkafka literal |
| Wrong password | authentication failing rather than hanging |
| Untrusted certificate (CA not in `ca_location`) | that verification is on at all |
| Hostname mismatch (dial a name absent from the SANs) | that `ssl.endpoint.identification.algorithm` was not quietly disabled |
| Restricted principal, granted read/write and describe | the intended production shape actually works |
| Restricted principal, denied `CreateTopics` | the deferred ACL path |

The last two are a pair, and the second is the deferred item. The success case
matters as much: a test suite whose only ACL case is "granted nothing" does not
demonstrate that a realistically restricted service can run. That principal needs
`DESCRIBE` and `DESCRIBE_CONFIGS` on the entity topics — `verify` mode calls
`fetch_metadata` and `describe_configs` (`topics.rs:81-131`) — plus `READ`/`WRITE`
on the topics and `READ` on the consumer group.

For the denial case, note that Kafka filters metadata by authorization rather
than erroring, so an unauthorized principal sees an empty topic set, `observe`
returns `None`, and `create` mode proceeds to `create_topics` — which is where
`TopicAuthorizationFailed` surfaces. The fixture must therefore reach
`CreateTopics` rather than failing earlier. Assert that the message names the
topic *and* carries its `[topics] mode = "verify"` remedy, since turning an opaque
broker code into an instruction is the entire reason `admin_error` exists. Then
drive the same failure through `RuntimeBuilder::build()`, because a runtime that
starts degraded is the defect the deferral describes.

### 8. When authentication is proven

The `connect(..)` constructors build a client; librdkafka connects lazily, so
construction succeeding is not proof that credentials work. Today's boot already
reaches the broker through topic convergence at `builder.rs:400-404`, but only
when `[topics] mode` is `verify` or `create` — under `off` it returns
immediately, and the publisher and consumer are not constructed until
`into_tasks()`.

So the contract has to be stated rather than inherited:

- Under `verify` or `create`, bad credentials fail `build()`, bounded by
  `TopicAdmin`'s existing 10s `ADMIN_TIMEOUT`, and surface as an authentication
  failure rather than a generic broker fault — `admin_error` gains
  `SaslAuthenticationFailed` beside `TopicAuthorizationFailed`.
- Under `off`, `build()` performs no broker I/O, so **bad credentials are not
  detected at boot** and first surface in the relay or ingest loop. This is the
  existing meaning of `off` and this proposal does not change it, but it must be
  documented on the config key rather than discovered.

Whether `off` should gain an opt-in authentication probe is deliberately left to
a follow-up: it would be the first broker I/O in a mode whose entire purpose is
to perform none.

## What this proposal rejects

### Environment-variable interpolation in `kafkaman.toml`

`${VAULT_KAFKA_PASS}` will appear in the example file as the CI/CD template's
syntax, substituted before the process starts. kafkaman must not expand it. The
accepted config decision puts rendering in the pipeline — "The running instance
reads one already-resolved flat file" — and expansion is precisely a second
source layered under the first. It also moves failure from deploy time to boot
time, invites `${VAR:-default}` and then a template language, and works against
§4 by requiring the loader to hold both template and resolved value.

### Credentials as a host-owned programmatic API

The considered alternative was a `KafkaConnection` the host builds from its own
vault and passes to the builder, leaving `kafkaman.toml` entirely secret-free. It
has a real argument: it matches the later of the two contradictory positions, it
keeps credential parsing out of the library, and it would make §4a a
non-requirement.

Rejected because it contradicts the accepted config decision rather than
completing it, and because it splits configuration across two mechanisms — a file
for everything else, code for this one thing — in a project whose config model is
deliberately "one flat file, rendered per environment." Recorded because the
choice is close, and because §4a is the price of it.

### mTLS, SASL/PLAIN, and GSSAPI

Out of scope. mTLS is a plausible next step and the config surface leaves room for
it. SASL/PLAIN is excluded because it sends credentials in the clear unless
wrapped in TLS, and supporting it means supporting the footgun of pairing it with
a plaintext protocol. GSSAPI is excluded on cost: it is the only one of the three
that pulls a second native dependency, and it is rare in Redpanda deployments.

### Flipping the shared test harness to SASL

It would touch all three duplicated container starts, the thirteen hand-rolled
test client configs, and every `from_brokers` call — with the 80% line floor in
`just test coverage` as a hard gate if anything broke. The information gained does
not justify the blast radius. For the same reason this proposal does **not**
consolidate the three near-verbatim Redpanda startups in `containers.rs:81`,
`tests/distributed-cache/src/lib.rs:154`, and
`tests/durable-send/tests/topic_convergence.rs:38`. That wart deserves its own
change.

## Resolved questions

1. **Auth is opt-in and plaintext stays first-class.** Local clusters and the
   compose stack run without authentication, and that is a supported
   configuration, not a degraded one. Enforced in four places: the section is
   optional; `security_protocol` defaults to `plaintext`; `from_brokers` survives
   untouched; and `examples/compose.yaml` and both example `kafkaman.toml` files
   do not change.
2. **The example services keep reading `KAFKA_BROKERS` from the environment.**
   `.gitignore` un-ignores `!examples/*/kafkaman.toml` on the written grounds that
   they "carry no secrets." Those files stay credential-free and the comment stays
   true. The examples also remain the coverage for the plaintext path.
3. **SCRAM-SHA-256 and SCRAM-SHA-512, both**, and both tested. The second is one
   match arm and one librdkafka literal; excluding it would be an arbitrary limit.
4. **Parse-error redaction is in scope and lands first.** It is reachable before
   any new type exists, so deferring it would ship the section with the leak open.

## Open questions

1. **Unconditional `rdkafka/ssl`, or a `kafka-tls` cargo feature?** Gating spares
   adopters an OpenSSL build dependency, which is what `just opt-out` exists to
   enforce. Against it: every `--all-features` invocation here — `just lint`,
   `just test all`, `just test coverage`, `just msrv` — would enable such a
   feature anyway, so it saves this repository nothing while adding a
   build-versus-config mismatch that fails at boot in production and must be
   detected, errored, documented, and tested. The `kafkaman-otel` precedent cuts
   both ways: it refused a `tls` feature, but on the grounds that it was "for a
   capability nothing here uses," which is not true here. **Acceptance must close
   this**, because §5 and §6 read differently depending on the answer.
2. **Should `[kafka].brokers` exist at all,** or should the section carry
   credentials only and leave the address to `RuntimeBuilder::brokers()`?
   Excluding it makes §3 unnecessary, removes the reserved-section break in §1,
   and keeps `examples/*/boot.rs` accurate with no rewording. Including it is more
   consistent with "one flat file."
3. **Does config validation stat the CA file?** `ResolvedConfig::from_config` is
   pure today. Statting it would be the first filesystem access in config
   resolution, but the crate's own doc comment promises every problem the config
   can have is reported "before a pool is opened," and a mistyped CA path
   otherwise surfaces as an opaque handshake failure much later.
4. **`sasl_ssl` is underscored while `scram-sha-256` is hyphenated.**
   `serde_enum.rs:1-20` mandates hyphenated literals. `SASL_SSL` is what every
   broker document in the ecosystem calls it, and `sasl-ssl` would read as a typo.
   Accept the inconsistency, or take `sasl-ssl` for internal consistency.

## Verification

Security, which is the part that must not be taken on trust:

- A sentinel password appears in no `{:?}` or `{}` rendering of `ConfigError`
  **from a malformed document**, `Config`, `KafkaSection`, `ResolvedConfig`,
  `Runtime`, or any `BuildError`.
- `[kafka]` with `security_protocol = "plaintext"` plus a `sasl_password` fails
  boot naming the key, rather than connecting in the clear.
- An untrusted certificate and a hostname mismatch each fail the connection —
  without these, a passing round trip does not show verification is enabled.

Function:

- Round trips on both SCRAM-SHA-256 and SCRAM-SHA-512 against a SASL_SSL
  Redpanda, converging topics through an authenticated admin client.
- A wrong password fails the connection rather than hanging, within the stated
  bound.
- A restricted principal granted describe/read/write runs the full loop; the same
  principal denied `CreateTopics` fails `create` mode with a message naming the
  topic and carrying the `[topics] mode = "verify"` remedy — and the same failure
  through `RuntimeBuilder::build()` refuses to start.
- Setting both `RuntimeBuilder::brokers(..)` and `[kafka] brokers` fails boot
  naming both sources.

Build and gates — **two commands, not one**:

- `cargo tree -p kafkaman-rdkafka --all-features -i sasl2-sys` finds nothing.
- `just check` stays green. It runs lint, lockfile, features, msrv, audit, and
  `just test all`, and its own comment records that it deliberately excludes
  coverage.
- `just test coverage` stays green, which is where the 80% line floor is enforced
  and what CI runs separately. Following `just check` alone would complete this
  proposal's gate without ever measuring coverage.
