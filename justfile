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

# Format, lint, and run the full test suite.
check:
    cargo fmt --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    just test all
