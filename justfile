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

# Fast gate: everything that does not need Docker. Run this first.
#
# `cargo doc` is a gate, not a build step. Splitting the crate roots into
# modules broke seven `[`item`]` doc links that resolved while everything shared
# one namespace, and neither fmt nor clippy has anything to say about them.
lint:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
    cargo test --workspace --lib

# Format, lint, and run the full test suite. Mirrors .github/workflows/ci.yml.
check:
    just lint
    just test all

# Remove kafkaman testcontainers left behind by interrupted test runs.
#
# Normal successful tests rely on Testcontainers' Drop cleanup. This fallback is
# intentionally label-scoped so it cannot remove unrelated Postgres or Redpanda
# containers that happen to use the same images.
clean-containers:
    #!/usr/bin/env bash
    set -euo pipefail
    ids=$(docker ps -aq --filter label=com.kafkaman.project=kafkaman \
                       --filter label=com.kafkaman.managed-by=testcontainers)
    if [ -n "$ids" ]; then docker rm -f $ids; else echo "nothing to clean"; fi
