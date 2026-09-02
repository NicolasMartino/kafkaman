# kafkaman tasks. Requires Docker for integration/coverage (testcontainers
# start Postgres and Redpanda).

# List recipes.
default:
    @just --list

# Run tests. arg: all (default) | unit | integration | coverage
test arg="all":
    #!/usr/bin/env bash
    set -euo pipefail
    with_example_bins() {
      cargo build -p example-order -p example-product
      local target
      target=$(cargo metadata --format-version 1 --no-deps | jq -r .target_directory)
      EXAMPLE_ORDER_BIN="$target/debug/order" \
      EXAMPLE_PRODUCT_BIN="$target/debug/product" \
        "$@"
    }
    case "{{ arg }}" in
      all)         with_example_bins cargo test --workspace --all-features ;;
      unit)        cargo test --workspace --lib ;;
      integration) with_example_bins cargo test --workspace --all-features --test '*' ;;
      coverage|cov) with_example_bins cargo llvm-cov --workspace --all-features --fail-under-lines 80 ;;
      *) echo "usage: just test [all|unit|integration|coverage]" >&2; exit 1 ;;
    esac

# Open an HTML coverage report.
cov-html:
    cargo llvm-cov --workspace --all-features --html --open

# `cargo doc` is a gate, not a build step. Splitting the crate roots into
# modules broke seven `[`item`]` doc links that resolved while everything shared
# one namespace, and neither fmt nor clippy has anything to say about them.
[doc("Fast gate: everything that does not need Docker. Run this first.")]
lint:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    # The no-op twins. `metrics` and `traces` are default-on, so nothing else in
    # this gate ever compiles the disabled halves — and a twin that stops
    # compiling is discovered by an adopter, not by us.
    cargo check -p kafkaman --no-default-features
    just opt-out
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
    cargo test --workspace --lib

# Prove the OpenTelemetry opt-out is real, not just documented.
#
# Compiling with `--no-default-features` does not prove it. Cargo unifies
# features across the whole graph, so one internal crate depending on
# `kafkaman-core` with default features puts `opentelemetry` back in while every
# build still succeeds — which is exactly how the contract was false for a
# while. Only the dependency graph can answer the question, and `cargo tree`
# answers it in milliseconds, so this runs in the fast gate rather than the slow
# one.
#
# `cargo tree -i` exits non-zero when the package is not in the graph, so the
# assertion is that it *fails*.
opt-out:
    #!/usr/bin/env bash
    set -euo pipefail
    for features in "" "axum" "rdkafka" "axum,rdkafka"; do
      if cargo tree -p kafkaman -e normal --no-default-features \
           --features "$features" -i opentelemetry >/dev/null 2>&1; then
        echo "opentelemetry is linked with --no-default-features --features '$features'" >&2
        echo "an internal dependency is pulling kafkaman-core or kafkaman-sqlx with defaults" >&2
        exit 1
      fi
    done
    # The other half of the ownership boundary: no crate an adopter can reach
    # through the facade may depend on an SDK or an exporter, at any feature
    # combination. That is every crate below — `kafkaman-otel` is the deliberate
    # exception, handled after this loop. A library that
    # links the SDK decides the host's pipeline for it — which provider, which
    # exporter, which shutdown — and there is no way for the host to take that
    # back. `--all-features` is the strongest form of the question.
    for crate in kafkaman kafkaman-core kafkaman-config kafkaman-sqlx \
                 kafkaman-worker kafkaman-rdkafka kafkaman-axum; do
      for forbidden in opentelemetry_sdk opentelemetry-otlp opentelemetry-appender-tracing; do
        if cargo tree -p "$crate" -e normal --all-features -i "$forbidden" >/dev/null 2>&1; then
          echo "$crate depends on $forbidden; SDKs and exporters belong to the host" >&2
          exit 1
        fi
      done
    done
    # kafkaman-otel is absent from that list on purpose: it is the one crate
    # under `crates/` whose whole job is to hold the exporter, per
    # wiki/decisions/kafkaman-otel-extraction.decision.md. The list is therefore
    # an allowlist by omission, which is worth saying so nobody "fixes" it.
    #
    # What keeps that safe is the assertion below rather than the omission
    # above. The boundary is not "no exporter under crates/" — it is that
    # nothing an adopter gets by depending on `kafkaman` links an SDK. So ask
    # that question directly: the facade must not reach kafkaman-otel at all.
    if cargo tree -p kafkaman -e normal --all-features -i kafkaman-otel >/dev/null 2>&1; then
      echo "kafkaman reaches kafkaman-otel; the facade must not re-export it" >&2
      echo "see wiki/decisions/kafkaman-otel-extraction.decision.md decision 3" >&2
      exit 1
    fi
    # And the third question, which the two above cannot ask: when
    # `opentelemetry` *is* linked, is it linked narrowly? Cargo features are
    # additive and a host can never subtract one, so a library crate that
    # enables an axis it does not use has widened every adopter's graph
    # permanently. A metrics-only deployment must not carry the trace API, and
    # neither build has any business carrying `logs` — that signal belongs to
    # the appender, which is a host dependency.
    enabled() {
      cargo tree -p kafkaman -e features --no-default-features --features "$1" \
           -i opentelemetry 2>/dev/null \
        | sed -n 's/.*opentelemetry feature "\([a-z-]*\)".*/\1/p' | sort -u
    }
    for spec in "metrics:trace" "metrics:logs" "traces:metrics" "traces:logs"; do
      build="${spec%%:*}"
      forbidden="${spec##*:}"
      if enabled "$build" | grep -qx "$forbidden"; then
        echo "a '$build' build enables the opentelemetry '$forbidden' feature" >&2
        echo "narrow the axis in the crate that asks for it; a host cannot undo it" >&2
        exit 1
      fi
    done
    # The forbidden pairs above say what must not be there. This says what is:
    # a metrics build links the metrics API and nothing else, which is the
    # strongest claim that can be pinned. The traces axis cannot be — `trace`
    # drags in opentelemetry's own optional dependencies (`futures`, `thiserror`
    # and friends), a list that belongs to upstream and would turn one of their
    # patch releases into a red build here. That set is recorded in the
    # compatibility note instead of asserted.
    actual=$(enabled metrics | paste -sd, -)
    if [ "$actual" != "metrics" ]; then
      echo "a metrics build enables opentelemetry [$actual], expected [metrics]" >&2
      echo "something in the graph widened the axis; a host cannot undo it" >&2
      exit 1
    fi
    echo "opt-out holds: no opentelemetry without metrics or traces, no SDK reachable"
    echo "from the facade (kafkaman-otel excluded by design and asserted unreachable),"
    echo "and a metrics build links the metrics API alone"

# Every feature combination that ships.
#
# `metrics` and `traces` are independent switches, and `axum` ships without
# `rdkafka`, so five of these seven combinations are only ever compiled here. A `#[cfg]` that assumes its sibling
# feature is on compiles cleanly under `--all-features` and breaks for whoever
# turns exactly one of them off.
features:
    #!/usr/bin/env bash
    set -euo pipefail
    for features in "" "metrics" "traces" "metrics,traces" \
                    "axum" "axum,rdkafka" "axum,rdkafka,metrics,traces"; do
      echo "==> --no-default-features --features '$features'"
      cargo check -p kafkaman --all-targets --no-default-features --features "$features"
    done

# Build at the declared MSRV.
#
# `rust-version` is a promise to adopters, and an unverified promise is a guess.
# The pinned toolchain is used rather than `stable` precisely because `stable`
# would accept code the MSRV does not: a stabilisation that lands between the two
# compiles here and breaks for anyone holding us to the declared floor.
#
# `--locked`, because resolving a *newer* dependency than `Cargo.lock` pins can
# raise the effective MSRV without anything in this repository changing.
msrv:
    #!/usr/bin/env bash
    set -euo pipefail
    version=$(sed -n 's/^rust-version = "\(.*\)"/\1/p' Cargo.toml | head -1)
    echo "==> MSRV gate: Rust ${version}"
    rustup toolchain install "${version}" --profile minimal >/dev/null 2>&1 || true
    cargo "+${version}" check --workspace --all-features --locked

# Security advisories against Cargo.lock.
#
# Findings are triaged in `.cargo/audit.toml`, which requires a reason and a
# revisit condition per ignore. `cargo audit` reads the *lockfile*, which records
# packages no feature combination here builds, so an ignore is often a statement
# that a crate is unreachable rather than that a risk is accepted — the config
# says which.
audit:
    cargo audit

# Cargo.lock is current, and no manifest edit has silently outdated it.
#
# Its own recipe rather than `--locked` on every build: forcing that locally
# turns an ordinary dependency bump into a confusing failure, while a dedicated
# gate answers the question the release actually depends on.
lockfile:
    cargo metadata --locked --format-version 1 > /dev/null
    @echo "Cargo.lock is up to date"

# Format, lint, and run the full test suite. Mirrors .github/workflows/ci.yml.
#
# Ordered cheapest-first so the fastest gate is the one that fails. `msrv` and
# `audit` are in here because CI runs them: a `check` that passes locally and
# then fails on the runner is worse than a slower `check`. Both want the
# network — `msrv` may install a toolchain on first run, `audit` refreshes the
# advisory database — which is the price of the local gate meaning what its
# name says. `just test coverage` is deliberately not here; it is the same
# tests again under instrumentation, and CI is the right place to pay for that.
check:
    just lint
    just lockfile
    just features
    just msrv
    just audit
    just test all

# Everything `check` runs, plus the gates CI keeps separate because they are slow
# or need a toolchain download. Run before tagging.
check-release:
    just check
    just msrv
    just audit
    just publish-order

# The order the crates must be published in, and a package check for each.
#
# `cargo publish` resolves path dependencies from crates.io, so a crate cannot be
# published before everything it depends on. The order below is the dependency
# topology; publishing out of it fails with "no matching package named ...",
# which reads like a missing crate rather than a sequencing mistake.
#
# `--no-verify` because the compile is already covered by `just check`; this
# recipe is about metadata and ordering. Nothing here contacts crates.io to
# write — it is a rehearsal.
publish-order:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "publish in this order:"
    for crate in kafkaman-core kafkaman-config kafkaman-sqlx kafkaman-worker \
                 kafkaman-axum kafkaman-rdkafka kafkaman-otel kafkaman kafkaman-test; do
      echo "  - $crate"
    done
    echo ""
    # Only the leaves can be packaged before anything is published; the rest
    # would need their dependencies to exist on crates.io first. Checking the
    # leaves still catches the metadata errors that block a release.
    for crate in kafkaman-core kafkaman-otel; do
      echo "==> cargo package -p $crate"
      cargo package -p "$crate" --no-verify --allow-dirty >/dev/null
    done
    echo "leaf crates package cleanly; the rest publish in the order above"

# Normal successful tests rely on Testcontainers' Drop cleanup. This fallback is
# intentionally label-scoped so it cannot remove unrelated Postgres or Redpanda
# containers that happen to use the same images.
[doc("Remove kafkaman testcontainers left behind by interrupted test runs.")]
clean-containers:
    #!/usr/bin/env bash
    set -euo pipefail
    ids=$(docker ps -aq --filter label=com.kafkaman.project=kafkaman \
                       --filter label=com.kafkaman.managed-by=testcontainers)
    if [ -n "$ids" ]; then docker rm -f $ids; else echo "nothing to clean"; fi

# `all` is everything, and the default: services, the telemetry backend, the
# message UI, a Kibana data view, and a demo run with enough traffic to be worth
# looking at. `demo` is the same propagation walkthrough without the backend, for
# when you do not want to pay for Elasticsearch; `observe` is `demo` plus the
# backend but without the message UI or the volume; `up` starts only the
# infrastructure and leaves the services to cargo. The rest attach to whichever
# is already running.
#
# `down` is the only destructive arm: it removes the compose volumes, so the
# Postgres data directory and Redpanda's log go with it. Everything the stack has
# recorded — outbox history, dead-lettered rows, the DLQ you were about to
# inspect — is gone. Stop the containers without that with
# `docker compose -f examples/compose.yaml stop`.
[doc("Drive the example stack. arg: all (default) | demo | observe | up | ui | faults | handoffs | telemetry-test | logs | down (DESTRUCTIVE: removes volumes)")]
examples arg="all":
    #!/usr/bin/env bash
    set -euo pipefail
    compose=(docker compose -f examples/compose.yaml)
    pg="${POSTGRES_PORT:-5432}"
    case "{{ arg }}" in
      all)
        # Every profile at once. `observe` and `ui` already compose cleanly, so
        # this arm is their union plus the two things that turn "the stack is
        # running" into "the telemetry is worth opening": a data view, and enough
        # traffic to fill a histogram.
        #
        # Elasticsearch is what makes this slow — a gigabyte of heap and 30-60s
        # to go yellow, which is why the timeout matches `observe`'s rather than
        # `demo`'s. If that is not what you want, `just examples demo` is the
        # same propagation walkthrough with nothing behind it.
        OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 \
          "${compose[@]}" --profile services --profile observability --profile ui \
            up -d --build --wait --wait-timeout 900
        # The collector declares no healthcheck, so compose can only gate it on
        # `service_started` and `--wait` proves nothing about its config. A
        # collector that started and then failed its pipeline looks exactly like
        # a healthy stack silently dropping everything, which is the failure the
        # profile was rebuilt to stop happening quietly.
        health="http://127.0.0.1:${OTEL_HEALTH_PORT:-13133}"
        for _ in $(seq 30); do
          if curl -fsS "$health" >/dev/null 2>&1; then break; fi
          sleep 1
        done
        if ! curl -fsS "$health" >/dev/null 2>&1; then
          echo "the OTLP collector is not answering on $health after 30s" >&2
          echo "logs: docker compose -f examples/compose.yaml logs otel-collector" >&2
          exit 1
        fi
        examples/kibana-dashboard.sh
        # Volume on top of the assertions: one product leaves the latency
        # histograms with a single observation each, which renders in Kibana as
        # something that looks broken.
        VOLUME_PRODUCTS="${VOLUME_PRODUCTS:-12}" examples/smoke.sh
        # Two of the seven fault scenarios, so the telemetry has a failure side
        # at all. Without this the stack is green everywhere: no span carries a
        # failure status, no log record is above INFO, and every DLQ is empty —
        # which is a demo of half the system.
        #
        # 1 and 2 only: they are the quick ones, and they deliberately leave a
        # dead-lettered row behind rather than redriving it, so the DLQ has
        # something in it when the dashboard is opened. `just examples faults`
        # runs all seven, including the three that need Docker.
        FAULT_SCENARIOS="1 2" examples/faults.sh
        echo ""
        echo "The whole stack is running:"
        echo "  order     http://127.0.0.1:3001/swagger-ui"
        echo "  product   http://127.0.0.1:3002/swagger-ui"
        echo "  Kibana    http://127.0.0.1:${KIBANA_PORT:-5601}/app/dashboards#/view/kafkaman-telemetry-dashboard?_g=(time:(from:now-4h,to:now),filters:!())"
        echo "  APM       http://127.0.0.1:${KIBANA_PORT:-5601}/app/apm/services?rangeFrom=now-4h&rangeTo=now&environment=ENVIRONMENT_ALL"
        echo "  messages  http://127.0.0.1:${CONSOLE_PORT:-8080}"
        echo ""
        echo "A dead-lettered row is waiting in product's DLQ, put there on purpose:"
        echo "  curl -s http://127.0.0.1:3002/internal/kafkaman/dlq | jq"
        echo "More failure modes, including a broker outage: just examples faults"
        echo ""
        echo "APM Trace samples show the product-to-order waterfall in one trace."
        echo "The dashboard separates traces, Kafka handoffs, queue metrics, and"
        echo "service logs so Discover's mixed OTel data view does not read like"
        echo "log spam. Linked-mode handoff URLs: just examples handoffs"
        echo ""
        echo "Tear it down with: just examples down"
        ;;
      demo)
        # The `services` profile pulls in the one-shot `provision` container,
        # which both services gate on; `--wait` treats its clean exit as
        # satisfied rather than as a service that failed to stay up.
        #
        # The endpoint is pinned empty rather than left to the shell. Compose
        # reads `${OTEL_EXPORTER_OTLP_ENDPOINT:-}` from the environment, so a
        # developer who exports one for their own tooling would otherwise get
        # containers exporting at an address that means something else inside
        # the compose network — usually their own loopback, which is nothing.
        OTEL_EXPORTER_OTLP_ENDPOINT= \
          "${compose[@]}" --profile services up -d --build --wait --wait-timeout 600
        examples/smoke.sh
        echo ""
        echo "The stack is still running:"
        echo "  order    http://127.0.0.1:3001/swagger-ui"
        echo "  product  http://127.0.0.1:3002/swagger-ui"
        echo "  message UI:  just examples ui"
        echo "Tear it down with: just examples down"
        ;;
      observe)
        # To the collector, not to Elasticsearch. Elasticsearch's native `/_otlp`
        # endpoint serves metrics only — `/v1/traces` and `/v1/logs` answer 400
        # `no handler found` — and it discards explicit-bucket histograms without
        # reporting anything. The collector speaks all three signals and owns the
        # Elasticsearch mapping. See examples/otel-collector.yaml.
        OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 \
          "${compose[@]}" --profile services --profile observability \
            up -d --build --wait --wait-timeout 900
        # `--wait` proves the collector container started, not that it is
        # listening: it declares no healthcheck, so compose has to gate on
        # `service_started`. Probe the health
        # extension from here instead. A collector that started and then failed
        # its configuration looks like a healthy stack silently dropping
        # everything, which is precisely the failure this profile was rebuilt to
        # stop happening quietly.
        health="http://127.0.0.1:${OTEL_HEALTH_PORT:-13133}"
        for _ in $(seq 30); do
          if curl -fsS "$health" >/dev/null 2>&1; then break; fi
          sleep 1
        done
        if ! curl -fsS "$health" >/dev/null 2>&1; then
          echo "the OTLP collector is not answering on $health after 30s" >&2
          echo "logs: docker compose -f examples/compose.yaml logs otel-collector" >&2
          exit 1
        fi
        examples/kibana-dashboard.sh
        examples/smoke.sh
        echo ""
        echo "The observed stack is still running:"
        echo "  order    http://127.0.0.1:3001/swagger-ui"
        echo "  product  http://127.0.0.1:3002/swagger-ui"
        echo "  Kibana   http://127.0.0.1:${KIBANA_PORT:-5601}/app/dashboards#/view/kafkaman-telemetry-dashboard?_g=(time:(from:now-4h,to:now),filters:!())"
        echo "  APM      http://127.0.0.1:${KIBANA_PORT:-5601}/app/apm/services?rangeFrom=now-4h&rangeTo=now&environment=ENVIRONMENT_ALL"
        echo "  message UI:  just examples ui"
        echo ""
        echo "One product's worth of telemetry, so the histograms have a single"
        echo "observation each. 'just examples all' drives volume as well."
        echo "Linked-mode handoff URLs: just examples handoffs"
        echo ""
        echo "Tear it down with: just examples down"
        ;;
      up)
        # Infrastructure only, for a normal cargo loop.
        #
        # The provisioner runs here too, but on the host rather than as a
        # container: this shape deliberately builds no images, and you are about
        # to `cargo run` the services anyway, so it shares that build instead of
        # adding a Docker one. The containerised equivalent is the `provision`
        # service in the `services` profile.
        "${compose[@]}" up -d --wait --wait-timeout 180
        ADMIN_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:${pg}/postgres" \
        KAFKA_BROKERS=127.0.0.1:19092 \
        cargo run --quiet -p example-provision
        echo ""
        echo "Postgres  127.0.0.1:${pg}  (databases: product_service, order_service)"
        echo "Redpanda  127.0.0.1:19092  (topics: products, orders — compacted)"
        echo ""
        echo "Then, in two terminals:"
        echo "  cd examples/product && DATABASE_URL=postgres://postgres:postgres@127.0.0.1:${pg}/product_service KAFKA_BROKERS=127.0.0.1:19092 cargo run"
        echo "  cd examples/order   && DATABASE_URL=postgres://postgres:postgres@127.0.0.1:${pg}/order_service   KAFKA_BROKERS=127.0.0.1:19092 cargo run"
        ;;
      ui)
        # Adds Redpanda Console beside whatever is already up.
        "${compose[@]}" --profile ui up -d --wait --wait-timeout 180
        echo ""
        echo "Redpanda Console  http://127.0.0.1:${CONSOLE_PORT:-8080}"
        echo "  Topics -> products / orders to read the snapshots the services exchange."
        ;;
      faults)
        # The counterpart to `smoke.sh`: the same stack, driven through its
        # failure paths instead of its happy one. Attaches to whatever is
        # already running rather than starting anything, because two of the six
        # scenarios stop and start containers and would fight a concurrent `up`.
        if ! curl -fsS -o /dev/null --max-time 5 "http://127.0.0.1:3002/faults"; then
          echo "no fault endpoint on http://127.0.0.1:3002" >&2
          echo "start the services first: just examples all (or demo)" >&2
          exit 1
        fi
        examples/faults.sh
        ;;
      handoffs)
        # For linked Kafka trace handoff mode, Kibana APM shows async span links
        # but does not reliably turn the producer-side link count into a
        # downstream consumer waterfall URL. This helper joins the indexed link
        # documents and prints both URLs.
        #
        # Checked here rather than left to curl: without the observability
        # profile the helper's first request fails with a connection error that
        # says nothing about which command to run instead.
        es="http://127.0.0.1:${ELASTICSEARCH_PORT:-9200}"
        if ! curl -fsS -o /dev/null --max-time 5 "$es/_cluster/health"; then
          echo "no Elasticsearch on $es" >&2
          echo "start the telemetry backend first: just examples all (or observe)" >&2
          exit 1
        fi
        examples/trace-handoffs.sh
        ;;
      telemetry-test)
        # The automated counterpart to `observe`: not a stack to look at, but the
        # gate that proves the example *binaries* install telemetry and flush it
        # on shutdown. `tests/distributed-cache` starts the same services as
        # library calls and so never reaches `main.rs`, which is where
        # `kafkaman_otel::init` and `Telemetry::shutdown()` live.
        #
        # Its own containers, through testcontainers — this does not attach to
        # whatever compose has running, and does not care whether anything is up.
        cargo build -p example-order -p example-product
        target=$(cargo metadata --format-version 1 --no-deps | jq -r .target_directory)
        # Passed in rather than discovered: `CARGO_BIN_EXE_*` is only defined for
        # integration tests of the package declaring the binary, so the test
        # cannot find these itself. It fails rather than skips when they are
        # unset, which is why this recipe is the documented way to run it.
        EXAMPLE_ORDER_BIN="$target/debug/order" \
        EXAMPLE_PRODUCT_BIN="$target/debug/product" \
          cargo test -p example-telemetry-tests --test binary_telemetry -- --nocapture
        ;;
      logs)  "${compose[@]}" --profile services logs -f order product ;;
      # DESTRUCTIVE. `-v` so the next start is genuinely clean: the volumes hold
      # the Postgres data directory and Redpanda's log, and provisioning is what
      # rebuilds both. It also means every row the stack ever wrote is deleted —
      # `docker compose -f examples/compose.yaml stop` is the non-destructive
      # way to get the ports back.
      down)  "${compose[@]}" --profile services --profile ui --profile observability down -v ;;
      *) echo "usage: just examples [all|demo|observe|up|ui|faults|handoffs|telemetry-test|logs|down]" >&2; exit 1 ;;
    esac
