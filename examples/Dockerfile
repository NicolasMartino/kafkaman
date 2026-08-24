# syntax=docker/dockerfile:1.7
#
# One Dockerfile, three binaries.
#
# `order`, `product` and `provision` are members of the same cargo workspace and
# all three link librdkafka, which rdkafka-sys compiles from bundled C sources
# rather than linking from the system. A Dockerfile per binary would pay that
# build, plus the whole shared dependency graph, three times over. So a single
# builder stage produces all three and three thin runtime stages take one each;
# compose selects between them with `target:`.
#
# The build context is the workspace root, not `examples/`, because the services
# depend on `crates/` and `examples/contracts`:
#
#   docker build -f examples/Dockerfile --target order .

# ---------------------------------------------------------------------------
# Builder: three binaries, one cargo invocation.
# ---------------------------------------------------------------------------
# The non-slim image is deliberate. It already carries gcc, g++, make,
# pkg-config and zlib1g-dev, which is exactly the toolchain librdkafka's mklove
# `./configure` needs — and `make libs` descends into src-cpp, so a C++ compiler
# is required even though nothing here uses the C++ API. Builder layers never
# ship, so the extra size costs nothing at runtime.
#
# zlib1g-dev is the one that would be easy to miss on a slim base: mklove's
# ENABLE_ZLIB defaults to "try", so a missing zlib.h *silently disables* zlib
# rather than failing the build. This workspace's librdkafka is compiled
# WITH_ZLIB=1 — check target/*/build/rdkafka-sys-*/out/config.h — so losing it
# would be a real behaviour change, discovered only against a broker that sends
# gzip-compressed batches.
#
# Deliberately NOT installed: libsasl2-dev. rdkafka-sys passes --disable-gssapi
# whenever its `gssapi` feature is off, and mklove's libsasl2 module returns
# before probing for headers. The evidence is in the same build directory:
# Makefile.config says `LIBS+= -lm -lz -ldl -lpthread` and config.h defines no
# WITH_SASL_CYRUS. (The apt line in .github/workflows/ci.yml is a no-op for the
# same reason.)
FROM rust:1-bookworm AS builder

ENV CARGO_TERM_COLOR=always \
    CARGO_NET_RETRY=10 \
    CARGO_INCREMENTAL=0

WORKDIR /src
COPY . .

# One invocation builds all three binaries, which is the entire point of the
# shared stage.
#
# The cache mounts survive between builds and make a rebuild cheap. They are
# mounts, though: nothing under them exists in the resulting layer. The binaries
# have to be copied somewhere real inside the *same* RUN or they vanish when it
# ends — which is why the `cp` is chained here rather than living in a later
# COPY.
#
# `sharing=locked` because compose builds three targets from this stage; should
# BuildKit ever run them concurrently they serialise rather than corrupting one
# another's cargo state.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    set -eux; \
    cargo build --release --locked \
        --package example-order \
        --package example-product \
        --package example-provision; \
    mkdir -p /out; \
    cp target/release/order target/release/product target/release/provision /out/

# ---------------------------------------------------------------------------
# Runtime base, shared by both services.
# ---------------------------------------------------------------------------
# Nothing to install but curl. The binaries need glibc, libgcc_s and libz, all
# of which debian:bookworm-slim already has. librdkafka here is built with SSL,
# SASL, zstd and curl support disabled, and no dependency in these services
# speaks TLS, so there is no ca-certificates or libsasl2-2 to ship.
#
# curl earns its place twice over: it answers the container's own healthcheck,
# and it makes `docker compose exec order curl ...` possible when the demo
# misbehaves. To drop it, use the bash /dev/tcp healthcheck noted in
# compose.yaml.
FROM debian:bookworm-slim AS runtime

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl; \
    rm -rf /var/lib/apt/lists/*; \
    useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin kafkaman

# ---------------------------------------------------------------------------
FROM runtime AS order

# `Config::discover()` walks up from the process working directory looking for
# `kafkaman.toml`, and a registered message type with no config file is a boot
# failure rather than a silent fallback to defaults. So the config has to sit at
# or above WORKDIR. This is the same contract as the "run it from this
# directory" note in the binary's own docs, honoured rather than worked around:
# the container exercises discovery exactly as `cargo run` does.
WORKDIR /srv/order
COPY examples/order/kafkaman.toml ./kafkaman.toml
COPY --from=builder /out/order /usr/local/bin/order
USER kafkaman
EXPOSE 3001
CMD ["/usr/local/bin/order"]

# ---------------------------------------------------------------------------
FROM runtime AS product

WORKDIR /srv/product
COPY examples/product/kafkaman.toml ./kafkaman.toml
COPY --from=builder /out/product /usr/local/bin/product
USER kafkaman
EXPOSE 3002
CMD ["/usr/local/bin/product"]

# ---------------------------------------------------------------------------
# The provisioner: creates the databases and the entity topics, then exits.
#
# No WORKDIR and no `kafkaman.toml`, unlike the two services. This binary
# registers no message types and runs no loops, so it has no kafkaman config to
# discover — its whole input is four environment variables. Giving it a config
# file would imply it reads one.
#
# It is also the reason the two services can be strict: they verify their topics
# at boot and refuse to start on a missing one, which is only a workable posture
# because something else creates them first. See examples/provision/src/lib.rs.
FROM runtime AS provision

COPY --from=builder /out/provision /usr/local/bin/provision
USER kafkaman
CMD ["/usr/local/bin/provision"]
