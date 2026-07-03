# Thin GNU Make equivalent of the justfile for contributors without `just`.
# Usage: `make <target>` (e.g. `make lint`). See `justfile` for the canonical set.

# Override on the command line: `make run SERVICE=gateway`, `make migrate DB=ledger_db`.
SERVICE       ?= gateway
DB            ?= ledger_db
PROFILE       ?= --release
COMPOSE       := docker compose -f deploy/docker-compose.yml
DATABASE_URL  ?= postgres://app:app@localhost:5432/ledger_db
DB_URL         = $(shell echo "$(DATABASE_URL)" | sed 's#/[^/]*$$#/$(DB)#')

.DEFAULT_GOAL := help
.PHONY: help fmt fmt-check lint check test build run up down logs migrate \
        sqlx-prepare audit coverage coverage-html bench loadtest ci

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

fmt: ## Format the workspace
	cargo fmt --all

fmt-check: ## Check formatting (CI parity)
	cargo fmt --all -- --check

lint: ## Clippy, warnings as errors
	cargo clippy --workspace --all-targets --all-features -- -D warnings

check: ## Type-check without building binaries
	cargo check --workspace --all-targets --all-features

test: ## Run unit + integration tests
	cargo test --workspace --all-features

build: ## Build the workspace ($(PROFILE))
	cargo build --workspace $(PROFILE)

run: ## Run a service: make run SERVICE=gateway
	cargo run --bin $(SERVICE)

up: ## Start local infra stack
	$(COMPOSE) up -d

down: ## Stop local infra stack
	$(COMPOSE) down

logs: ## Follow infra logs
	$(COMPOSE) logs -f

migrate: ## Run migrations: make migrate DB=ledger_db
	sqlx database create --database-url "$(DB_URL)"
	sqlx migrate run --source migrations/$(DB) --database-url "$(DB_URL)"

sqlx-prepare: ## Regenerate .sqlx offline cache
	cargo sqlx prepare --workspace -- --all-targets

audit: ## Supply-chain / advisory scan
	cargo deny check

coverage: ## Line coverage -> lcov.info
	cargo llvm-cov --workspace --all-features --lcov --output-path lcov.info

coverage-html: ## Coverage as HTML
	cargo llvm-cov --workspace --all-features --html

bench: ## Criterion benchmarks
	cargo bench --workspace

loadtest: ## Goose load test (HOST overridable)
	cargo run --release --bin loadtest -- --host $(or $(HOST),http://localhost:8080)

ci: fmt-check lint test audit ## Full local gate
