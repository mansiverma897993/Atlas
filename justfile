# Developer command runner — https://github.com/casey/just
# List recipes with `just` or `just --list`.
#
# Services: gateway identity ledger notification worker

set dotenv-load := true
set shell := ["bash", "-uc"]

# Default DATABASE_URL used by migrate/sqlx recipes (override via .env or env).
export DATABASE_URL := env_var_or_default("DATABASE_URL", "postgres://app:app@localhost:5432/ledger_db")

# List all available recipes.
default:
    @just --list

# Format the entire workspace.
fmt:
    cargo fmt --all

# Verify formatting without writing (CI parity).
fmt-check:
    cargo fmt --all -- --check

# Clippy across all targets/features, warnings are errors.
lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# Type-check the workspace without producing binaries.
check:
    cargo check --workspace --all-targets --all-features

# Run the full test suite (unit + integration).
test:
    cargo test --workspace --all-features

# Build the workspace (release; pass `just build ""` for debug).
build profile="--release":
    cargo build --workspace {{profile}}

# Run a single service binary, e.g. `just run gateway`.
run service:
    cargo run --bin {{service}}

# ---- Infra (docker compose) ------------------------------------------------
compose := "docker compose -f deploy/docker-compose.yml"

# Bring the local infra stack up (Postgres, Redis, Redpanda, OTel, Jaeger, ...).
up:
    {{compose}} up -d

# Tear the stack down.
down:
    {{compose}} down

# Follow logs from the stack.
logs:
    {{compose}} logs -f

# ---- Database --------------------------------------------------------------
# Run migrations for one bounded context (identity_db | ledger_db | worker_db).
migrate db="ledger_db":
    sqlx database create --database-url "$(echo $DATABASE_URL | sed 's#/[^/]*$#/{{db}}#')"
    sqlx migrate run --source migrations/{{db}} --database-url "$(echo $DATABASE_URL | sed 's#/[^/]*$#/{{db}}#')"

# Regenerate the offline query cache (.sqlx/) for CI builds.
sqlx-prepare:
    cargo sqlx prepare --workspace -- --all-targets

# ---- Quality gates ---------------------------------------------------------
# Supply-chain + advisory scan.
audit:
    cargo deny check

# Line coverage via cargo-llvm-cov; writes lcov.info.
coverage:
    cargo llvm-cov --workspace --all-features --lcov --output-path lcov.info

# Coverage as an HTML report.
coverage-html:
    cargo llvm-cov --workspace --all-features --html

# Criterion benchmarks.
bench:
    cargo bench --workspace

# Goose load test (expects a `loadtest` binary/crate; edit host as needed).
loadtest host="http://localhost:8080":
    cargo run --release --bin loadtest -- --host {{host}}

# Format + lint + test in one shot (pre-push gate).
ci: fmt-check lint test audit
