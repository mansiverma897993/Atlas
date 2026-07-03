//! Typed identifiers.
//!
//! Every id is a distinct newtype over [`uuid::Uuid`] so the compiler prevents passing an
//! `AccountId` where an `OwnerId` is expected. The [`typed_id!`] macro generates the boiler-
//! plate (construction, parsing, display, serde) uniformly.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{KernelError, Result};

/// Declare a UUID-backed newtype identifier with a consistent API.
macro_rules! typed_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generate a fresh random (v4) identifier.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Generate a time-ordered (v7) identifier — useful where lexical ordering by
            /// creation time helps index locality (e.g. event ids).
            #[must_use]
            pub fn new_ordered() -> Self {
                Self(Uuid::now_v7())
            }

            /// Wrap an existing [`Uuid`].
            #[must_use]
            pub const fn from_uuid(id: Uuid) -> Self {
                Self(id)
            }

            /// The underlying [`Uuid`].
            #[must_use]
            pub const fn as_uuid(&self) -> Uuid {
                self.0
            }

            /// Parse from its string form.
            pub fn parse(s: &str) -> Result<Self> {
                Uuid::parse_str(s)
                    .map(Self)
                    .map_err(|e| KernelError::InvalidId(e.to_string()))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl From<Uuid> for $name {
            fn from(id: Uuid) -> Self {
                Self(id)
            }
        }

        impl From<$name> for Uuid {
            fn from(id: $name) -> Self {
                id.0
            }
        }
    };
}

typed_id!(
    /// Identifies an [`Account`](crate) aggregate (an event stream in the ledger).
    AccountId
);
typed_id!(
    /// Identifies the owner (customer) of an account, as known to the ledger.
    OwnerId
);
typed_id!(
    /// Identifies a transfer saga instance. Doubles as the client-facing idempotency surface.
    TransferId
);
typed_id!(
    /// Identifies a persisted domain event (idempotency key for consumers).
    EventId
);
typed_id!(
    /// Identifies a user in the identity context.
    UserId
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_types_do_not_mix() {
        // This is a compile-time guarantee; here we just exercise the runtime API.
        let a = AccountId::new();
        let parsed = AccountId::parse(&a.to_string()).unwrap();
        assert_eq!(a, parsed);
    }

    #[test]
    fn ordered_ids_are_parseable() {
        let e = EventId::new_ordered();
        assert_eq!(EventId::parse(&e.to_string()).unwrap(), e);
    }

    #[test]
    fn invalid_id_is_rejected() {
        assert!(matches!(
            AccountId::parse("not-a-uuid"),
            Err(KernelError::InvalidId(_))
        ));
    }

    #[test]
    fn uuid_conversions() {
        let raw = Uuid::new_v4();
        let id = TransferId::from_uuid(raw);
        assert_eq!(id.as_uuid(), raw);
        assert_eq!(Uuid::from(id), raw);
    }
}
