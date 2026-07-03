# ADR-0001 — Modular-monorepo Cargo workspace over polyrepo microservices

**Status:** Accepted

## Context
We want independently deployable services (separate scaling, isolation of failure) *and*
strong, checked contracts between them. Two extremes: (a) one fat binary — simple but can't
scale services independently and blurs boundaries; (b) polyrepo microservices — realistic at
org scale but imposes cross-repo versioning, contract drift, and CI overhead that is pure
cost for a single-author portfolio.

## Decision
One repository, one **Cargo workspace**, many crates: a binary crate per service
(`gateway`, `identity`, `ledger`, `notification`, `worker`) plus shared **library** crates
(`kernel`, `proto`, `config`, `telemetry`, `infra`, `resilience`). Each service deploys as
its own container/binary; shared contracts are compile-time-checked because they are ordinary
crate dependencies.

## Consequences
- **+** Service boundaries are enforced by the module graph and the dependency rule, not by
  convention. Refactors across a contract fail to compile instead of failing in production.
- **+** One `cargo test`, one CI, atomic cross-service changes, no version-skew dance.
- **+** Deployment is still per-service (independent scaling, blast-radius isolation).
- **−** Not a literal microservices repo topology; if this grew to many teams, we'd split.
  Recorded as an accepted trade-off, revisitable via a superseding ADR.
- **−** All services share the workspace's dependency versions (a feature here: consistency).
