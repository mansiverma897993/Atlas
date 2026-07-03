# ADR-0012 — OpenTelemetry OTLP → Collector → Jaeger/Prometheus/Grafana

**Status:** Accepted

## Context
A distributed, event-driven system is undebuggable without correlated observability: a single
transfer crosses gateway → ledger → worker → notification, synchronously *and* through the
async backbone. Per-service logs alone can't answer "where did this transfer spend its time?"
or "why did this one fail?" We also want to avoid coupling application code to any one vendor
or backend.

## Decision
Instrument everything with **OpenTelemetry**. Application code emits traces/metrics/logs via
`tracing` + `tracing-opentelemetry` and exports **OTLP** to a central **OTel Collector**, which
fans out to **Jaeger** (traces), **Prometheus** (metrics), and **Grafana** (dashboards).
- **Trace context is propagated across both transports:** gRPC metadata (tonic interceptors)
  **and** Redpanda message headers (`traceparent`), so one transfer is one distributed trace
  even across the async hop.
- **Correlation/request IDs** are minted at the gateway and carried in gRPC metadata, Kafka
  headers, and every structured (JSON) log line, alongside `trace_id`/`span_id`.
- **Metrics:** RED per endpoint + domain metrics (transfers/sec, saga latency, consumer lag,
  DLQ depth). **Audit log** is a separate append-only concern, not operational logging.

## Consequences
- **+** One trace spans the whole causal chain, sync and async — real distributed debugging.
- **+** Vendor-neutral: swap Jaeger/Prometheus/Grafana for any OTLP-compatible backend
  (Datadog, Honeycomb, Tempo) with zero application changes — only the Collector config moves.
- **+** Correlated logs/metrics/traces via shared ids; dashboards and alerts come for free.
- **−** Running a Collector + Jaeger + Prometheus + Grafana adds moving parts to the local
  stack. Encapsulated in `deploy/observability/` and one `telemetry` bootstrap call per service.
- **−** Context propagation across Kafka headers is easy to forget on a new producer/consumer;
  enforced by wrapping all producers/consumers in the shared `infra` crate so it's automatic.
