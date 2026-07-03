# ADR-0011 — gRPC internally, REST + WebSocket at the edge

**Status:** Accepted

## Context
Two different audiences need two different contracts. External clients (browsers, mobile,
partners) want a familiar, documented, firewall-friendly HTTP/JSON API. Internal
service-to-service calls want typed, versioned, low-overhead, streaming-capable contracts and
compile-time schema checks. Using REST/JSON internally loses type safety and streaming; using
gRPC at the browser edge is awkward (needs grpc-web proxying) and unfamiliar to API consumers.

## Decision
- **Edge (client ↔ gateway):** **REST over HTTPS**, documented with **OpenAPI/Swagger**, plus
  **WebSocket (WSS)** for realtime. Human- and partner-friendly.
- **Internal (gateway ↔ service, service ↔ service sync):** **gRPC (tonic)** over HTTP/2 —
  typed, multiplexed, streaming, low overhead.
- The **`proto` crate** is the single source of truth for internal contracts (compiled by
  `build.rs`); the gateway maps REST ⇄ gRPC and generates OpenAPI from its own handler types.
- Asynchronous integration remains on the event backbone ([ADR-0007](./0007-redpanda.md)), not
  gRPC — gRPC is for synchronous request/response only.

## Consequences
- **+** External API is standard, documented, and easy to consume; internal contracts are
  type-checked and evolve safely via protobuf's additive rules.
- **+** HTTP/2 multiplexing and gRPC streaming benefit chatty internal paths.
- **+** Trace context propagates cleanly through gRPC metadata (tonic interceptors).
- **−** Two contract representations (OpenAPI + protobuf) and a mapping layer at the gateway.
  The mapping is localized to gateway adapters and is a deliberate translation seam.
- **−** gRPC isn't directly callable from a browser; intentional — browsers talk REST/WS to the
  gateway, never gRPC.
