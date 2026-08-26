# kafkaman tasks. Requires Docker for integration/coverage (testcontainers
# start Postgres and Redpanda).

# List recipes.
default:
    @just --list

# Run tests. arg: all (default) | unit | integration | coverage
test arg="all":
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{ arg }}" in
      all)         cargo test --workspace --all-features ;;
      unit)        cargo test --workspace --lib ;;
      integration) cargo test --workspace --all-features --test '*' ;;
      coverage|cov) cargo llvm-cov --workspace --all-features --fail-under-lines 80 ;;
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
    # The other half of the ownership boundary: no crate under `crates/` may
    # depend on an SDK or an exporter, at any feature combination. A library that
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
    echo "opt-out holds: no opentelemetry without metrics or traces, no SDK in crates/,"
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

# Format, lint, and run the full test suite. Mirrors .github/workflows/ci.yml.
check:
    just lint
    just features
    just test all

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

# `demo` is the whole thing in containers; `observe` adds Elasticsearch and
# Kibana; `up` starts only the infrastructure and leaves the services to cargo.
# The other three attach to whichever of the two is already running.
[doc("Drive the example stack. arg: demo (default) | observe | up | ui | logs | down")]
examples arg="demo":
    #!/usr/bin/env bash
    set -euo pipefail
    compose=(docker compose -f examples/compose.yaml)
    pg="${POSTGRES_PORT:-5432}"
    case "{{ arg }}" in
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
        # Direct to Elasticsearch's native OTLP/HTTP endpoint. The exporter
        # appends /v1/metrics, /v1/traces, and /v1/logs to this base path.
        OTEL_EXPORTER_OTLP_ENDPOINT=http://elasticsearch:9200/_otlp \
          "${compose[@]}" --profile services --profile observability \
            up -d --build --wait --wait-timeout 900
        examples/smoke.sh
        echo ""
        echo "The observed stack is still running:"
        echo "  order    http://127.0.0.1:3001/swagger-ui"
        echo "  product  http://127.0.0.1:3002/swagger-ui"
        echo "  Kibana   http://127.0.0.1:${KIBANA_PORT:-5601}"
        echo "  message UI:  just examples ui"
        echo ""
        echo "Kibana ships with no data view for the kafkaman signals yet, so it"
        echo "opens empty. Discover -> create a data view over the indices"
        echo "Elasticsearch's OTLP endpoint writes to; a packaged dashboard is"
        echo "still pending (wiki/plans/opentelemetry-completion.plan.md)."
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
      logs)  "${compose[@]}" --profile services logs -f order product ;;
      # `-v` so the next start is genuinely clean: the volumes hold the Postgres
      # data directory and Redpanda's log, and provisioning is what rebuilds
      # both.
      down)  "${compose[@]}" --profile services --profile ui --profile observability down -v ;;
      *) echo "usage: just examples [demo|observe|up|ui|logs|down]" >&2; exit 1 ;;
    esac
