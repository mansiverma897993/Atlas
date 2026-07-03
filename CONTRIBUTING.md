# Contributing

Thanks for contributing to the Ledger Platform backend. This is a Cargo workspace of
five service binaries — `gateway`, `identity`, `ledger`, `notification`, `worker` — over
shared library crates in `crates/libs/`. Please read
[`docs/CONVENTIONS.md`](docs/CONVENTIONS.md) and [`docs/ROADMAP.md`](docs/ROADMAP.md)
before opening a PR.

## Prerequisites

- Rust toolchain — pinned via [`rust-toolchain.toml`](rust-toolchain.toml); `rustup`
  installs it automatically.
- Docker + Docker Compose (local infra: Postgres, Redis, Redpanda, OTel, Jaeger, ...).
- `protoc` (Protocol Buffers compiler) — gRPC codegen with tonic.
- Helpers: [`just`](https://github.com/casey/just), and Cargo tools
  `sqlx-cli`, `cargo-deny`, `cargo-llvm-cov`.

```bash
cargo install sqlx-cli --no-default-features --features rustls,postgres
cargo install cargo-deny cargo-llvm-cov
```

## Build, run & test locally

```bash
cp .env.example .env          # local defaults (see docs/CONVENTIONS.md)
just up                       # start the infra stack (or: make up)
just migrate identity_db      # per bounded context: identity_db | ledger_db | worker_db
just migrate ledger_db
just migrate worker_db

just run gateway              # run one service (gateway|identity|ledger|notification|worker)

just fmt                      # format
just lint                     # clippy -D warnings
just test                     # unit + integration
just audit                    # cargo-deny (advisories + licenses + bans + sources)
just coverage                 # lcov.info via cargo-llvm-cov
just down                     # stop infra
```

No `just`? Every recipe has a `make` equivalent — `make lint`, `make run SERVICE=gateway`,
`make migrate DB=ledger_db`, etc.

Ports, env vars, database names, and Kafka topics are defined once in
[`docs/CONVENTIONS.md`](docs/CONVENTIONS.md) — treat it as the single source of truth and
keep code, CI, and deploy manifests in sync with it.

## Coding standards

- **Hexagonal / clean architecture.** Keep `domain` pure — no imports from `application`,
  `adapters`, or `infra`. Dependencies point inward (adapters depend on the domain, never
  the reverse). I/O lives at the edges behind ports/traits.
- **Lints.** The workspace runs `clippy` with `pedantic` as warnings and forbids
  `unsafe_code`. CI enforces `-D warnings`; keep it clean.
- **Formatting.** `rustfmt` config is committed; run `just fmt` before pushing.
- **Errors.** `thiserror` for library/domain error types, `anyhow` at binary boundaries.
- **sqlx.** Commit the offline query cache — run `just sqlx-prepare` when queries change so
  CI builds with `SQLX_OFFLINE=true`.
- **Migrations** live under `migrations/<db>/` and run as an init container / Job, never at
  app boot (ADR-0008).
- **Tests.** Unit tests beside the code; integration tests against real Postgres/Redis/
  Redpanda (mirrors CI). Property tests for invariants (`Money`, conservation) where
  applicable.

## Architectural decisions — ADRs

Any architectural choice or non-obvious trade-off gets an ADR under `docs/adr/`
(`NNNN-title.md`). Reference the ADR number in the PR. If a change contradicts an existing
ADR, supersede it with a new one rather than editing history.

## Commits & PRs

- **Conventional Commits**: `type(scope): summary`
  (`feat(ledger): add reserve command`, `fix(gateway): cache JWKS`, `chore(deps): ...`).
  Types: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `chore`, `ci`, `build`.
  Scope is usually the service or lib crate.
- Branch off `main`; keep PRs focused. `main` is always deployable and merges only behind
  green CI.
- Fill in the PR template checklist. CI must pass: `fmt`, `clippy`, `test`, `deny`,
  `coverage`.
- Update `.env.example` **and** `docs/CONVENTIONS.md` together whenever you add a config var.

## Reporting security issues

Do **not** open a public issue for vulnerabilities — see [`SECURITY.md`](SECURITY.md).
