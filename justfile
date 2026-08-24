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
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
    cargo test --workspace --lib

# Format, lint, and run the full test suite. Mirrors .github/workflows/ci.yml.
check:
    just lint
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

# `demo` is the whole thing in containers; `up` starts only the infrastructure
# and leaves the services to cargo. The other three attach to whichever of the
# two is already running.
[doc("Drive the example stack. arg: demo (default) | up | ui | logs | down")]
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
        "${compose[@]}" --profile services up -d --build --wait --wait-timeout 600
        examples/smoke.sh
        echo ""
        echo "The stack is still running:"
        echo "  order    http://127.0.0.1:3001/swagger-ui"
        echo "  product  http://127.0.0.1:3002/swagger-ui"
        echo "  message UI:  just examples ui"
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
      down)  "${compose[@]}" --profile services --profile ui down -v ;;
      *) echo "usage: just examples [demo|up|ui|logs|down]" >&2; exit 1 ;;
    esac
