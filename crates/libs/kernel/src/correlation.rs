//! Correlation, causation, and request identifiers for distributed tracing.
//!
//! * [`RequestId`] — unique per inbound edge request. Minted by the gateway.
//! * [`CorrelationId`] — follows an entire causal chain across services and the event bus,
//!   so every log line and span for one logical operation can be joined.
//! * [`CausationId`] — the id of the message/command that directly caused this one, enabling
//!   reconstruction of the causal graph within a correlation.
//!
//! These are propagated through gRPC metadata and Kafka headers (see the `infra` crate) and
//! attached to every structured log line and tracing span (see ADR-0012).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! trace_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Mint a fresh identifier.
            #[must_use]
            pub fn generate() -> Self {
                Self(Uuid::new_v4().to_string())
            }

            /// Adopt an existing identifier string (e.g. propagated from an upstream hop).
            #[must_use]
            pub fn from_string(s: impl Into<String>) -> Self {
                Self(s.into())
            }

            /// The string form (for headers / metadata / logs).
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

trace_id!(
    /// Unique per inbound request at the edge.
    RequestId
);
trace_id!(
    /// Follows an entire causal chain across services and the event bus.
    CorrelationId
);
trace_id!(
    /// The id of the immediate cause of the current message.
    CausationId
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_is_unique_and_propagatable() {
        let a = CorrelationId::generate();
        let b = CorrelationId::generate();
        assert_ne!(a, b);
        let propagated = CorrelationId::from_string(a.as_str());
        assert_eq!(a, propagated);
    }
}
