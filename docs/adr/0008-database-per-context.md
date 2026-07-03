# ADR-0008 — One PostgreSQL database per bounded context

**Status:** Accepted

## Context
Bounded contexts must be able to evolve their schemas and scale their storage independently.
A shared database with cross-context tables and foreign keys creates hidden coupling: one
context's migration can break another, and the "boundaries" become fiction.

## Decision
Each context owns a **separate PostgreSQL database** (`identity_db`, `ledger_db`, …). No
cross-context foreign keys, no reaching into another context's tables. Contexts integrate
**only through events** on the backbone. If Ledger needs a fact from Identity (e.g. owner
exists), it learns it via a `UserRegistered` event and keeps its own local copy/reference.

## Consequences
- **+** Boundaries are physically enforced; a schema change is contained to its context.
- **+** Each store scales, backs up, and gets least-privilege DB credentials independently.
- **+** Prepares the ground for true service extraction if ever needed.
- **−** No cross-context joins or cross-context transactions — data is duplicated and
  consistency is eventual. This is the intended trade of microservice data ownership, handled
  by the event backbone and sagas, and it forces us to confront distributed-data reality
  honestly rather than hide it behind joins.
- **−** More databases to run locally. Cheap under Docker Compose (separate DBs / instances).
