//! Inbound adapters — the framework-facing edge of the service.
//!
//! Only the WebSocket surface here (there is no gRPC/REST on notification). The composition
//! root injects the [`ws::WsState`] holding the hub, presence tracker, and token verifier.

pub mod ws;
