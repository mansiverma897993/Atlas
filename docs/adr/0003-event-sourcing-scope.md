# ADR-0003 — Event Sourcing scoped to the Ledger only

**Status:** Accepted

## Context
Event Sourcing (ES) buys immutable history, temporal queries, audit-by-construction, and a
natural integration stream — at the cost of complexity: replay, snapshots, projection
rebuilds, versioning/upcasting, eventual consistency on the read side. Applying ES uniformly
across all contexts is a common over-engineering mistake; applying it nowhere wastes the
domain that most benefits.

## Decision
Use **Event Sourcing only in the Ledger** context, where immutability, auditability, and
point-in-time reconstruction are hard requirements and the events *are* the business facts.
Model **Identity** and other supporting contexts as **CRUD + transactional outbox** — normal
tables for state, events emitted only for integration.

## Consequences
- **+** ES pays for itself exactly where the requirements demand it; the audit trail and
  balance history are free byproducts of the write model.
- **+** Identity stays simple (no replay/snapshot machinery for data that never needs it).
- **+** The deliberate contrast demonstrates judgment — the *decision not to use* a pattern is
  as much a signal as using it.
- **−** Two persistence styles in one system; developers must know which context they're in.
  Mitigated by the uniform hexagonal layout and this ADR.
