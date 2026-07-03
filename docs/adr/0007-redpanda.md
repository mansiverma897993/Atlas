# ADR-0007 — Redpanda as the event backbone

**Status:** Accepted

## Context
The system needs a durable, partitioned, **replayable** log for event-driven integration,
event-sourcing projections, and saga choreography. Candidates: Apache Kafka, Redpanda,
RabbitMQ, NATS JetStream. RabbitMQ is a queue/broker whose model fits work-queues but is a
weaker fit for a replayable event log (event sourcing wants an ordered, retained,
re-consumable stream). Kafka is the reference implementation but heavy to run locally
(JVM + KRaft/Zookeeper), which hurts a laptop-reproducible portfolio and CI.

## Decision
Use **Redpanda** — Kafka-API-compatible, single native binary, no JVM/Zookeeper. All code
uses the Kafka protocol (`rdkafka`), so the choice is transparent to application code and
swappable for Kafka in production.

## Consequences
- **+** Same Kafka skills and client code; ordered, retained, partitioned, replayable log —
  exactly what ES/CQRS/sagas need (ordering per account via partition-by-`stream_id`).
- **+** Dramatically lighter local `docker compose` and CI footprint; faster feedback.
- **+** Compaction and DLQ topics supported like Kafka.
- **−** Not literally Kafka in the demo. Mitigated: protocol-compatible, and production can run
  managed Kafka/Redpanda unchanged. Recorded as an accepted, reversible trade-off.
- **Rejected:** RabbitMQ (poor replay/event-log fit), NATS (smaller ecosystem for this use).
