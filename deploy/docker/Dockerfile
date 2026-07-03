# syntax=docker/dockerfile:1.7
###############################################################################
# Reusable, parameterized multi-stage Dockerfile for ALL five services.
#
# One image per service, selected at build time with:  --build-arg SERVICE=<crate>
# where <crate> ∈ { gateway, identity, ledger, notification, worker }.
# The binary name always equals the crate name (see docs/CONVENTIONS.md).
#
# Build strategy (docs/CONVENTIONS.md §"Container / image conventions"):
#   * cargo-chef for dependency-layer caching  → fast rebuilds when only src changes
#   * builder on the official rust image        → compiles the *whole* workspace once
#   * runtime on debian:bookworm-slim, non-root → small, least-privilege image
#
# Because the workspace builds all binaries in one `cargo build`, the dependency
# cache (the expensive layer) is shared across every service image. Only the tiny
# final COPY differs per service.
###############################################################################

# --- Pinnable versions -------------------------------------------------------
ARG RUST_VERSION=1.83
ARG DEBIAN_RELEASE=bookworm

###############################################################################
# Stage 0: chef — base image with cargo-chef installed.
###############################################################################
FROM rust:${RUST_VERSION}-slim-${DEBIAN_RELEASE} AS chef
# System deps needed to compile common Rust crates (TLS, protobuf for tonic, pkg-config).
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        protobuf-compiler \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked
WORKDIR /app

###############################################################################
# Stage 1: planner — compute the dependency "recipe" (a fingerprint of deps).
# This layer is invalidated only when Cargo.toml / Cargo.lock change.
###############################################################################
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

###############################################################################
# Stage 2: builder — cook dependencies (cached), then build the workspace.
###############################################################################
FROM chef AS builder
# 2a. Build & cache ONLY the dependencies. As long as recipe.json is unchanged
#     this whole layer is reused, even if application source changed.
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# 2b. Now copy the real source and build all release binaries.
COPY . .
RUN cargo build --release --workspace

###############################################################################
# Stage 3: runtime — minimal, non-root image carrying a single service binary.
###############################################################################
FROM debian:${DEBIAN_RELEASE}-slim AS runtime

# SERVICE selects which binary to ship. Passed by docker-compose / CI per service.
ARG SERVICE
# Fail the build early if SERVICE was not provided.
RUN test -n "${SERVICE}" || (echo "ERROR: build-arg SERVICE is required" && exit 1)

# Runtime libraries (TLS certs + libssl) — no compiler, no build tooling.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    # Create an unprivileged user/group to run as (uid/gid 10001).
    && groupadd --gid 10001 app \
    && useradd  --uid 10001 --gid app --home-dir /app --shell /usr/sbin/nologin app

WORKDIR /app

# Copy the selected binary from the builder and give it a stable name `service`.
# (Using a fixed name keeps ENTRYPOINT identical across all images.)
COPY --from=builder /app/target/release/${SERVICE} /usr/local/bin/service

# Ship the layered config files; the concrete env is chosen by RUN_ENV at runtime.
COPY --from=builder /app/config /app/config

# Persist SERVICE into the image so scripts/labels can read it back.
ENV SERVICE=${SERVICE} \
    RUN_ENV=production \
    # Sensible default log config (overridable via APP__LOG__* env).
    APP__LOG__LEVEL=info \
    APP__LOG__FORMAT=json

# Document every port a service *might* expose. Compose/k8s map only what's used:
#   8080-8084 HTTP/WS/health · 50051-50052 gRPC · 9100-9104 Prometheus metrics
EXPOSE 8080 8081 8082 8083 8084 50051 50052 9100 9101 9102 9103 9104

# Drop privileges.
USER app

# Container-level healthcheck. Each service exposes /health/live on its HTTP port.
# The concrete port differs per service, so compose/k8s override HEALTH_PORT.
# Default 8080 matches the gateway; other services set HEALTH_PORT accordingly.
ENV HEALTH_PORT=8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=30s --retries=5 \
    CMD curl -fsS "http://127.0.0.1:${HEALTH_PORT}/health/live" || exit 1

# SIGTERM → graceful shutdown is handled inside the binary (docs §6.5).
ENTRYPOINT ["/usr/local/bin/service"]
