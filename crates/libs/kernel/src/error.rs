//! Kernel-level errors: violations detectable purely from value-object rules.
//!
//! These are *not* business-rule errors (those live in each service's domain layer). They
//! are the low-level invariants of the primitives themselves — overflow, currency mismatch,
//! malformed identifiers.

use thiserror::Error;

/// Errors produced by kernel value objects.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum KernelError {
    /// Two [`crate::Money`] values of different currencies were combined.
    #[error("currency mismatch: {left} vs {right}")]
    CurrencyMismatch {
        /// Currency of the left-hand operand.
        left: crate::Currency,
        /// Currency of the right-hand operand.
        right: crate::Currency,
    },

    /// A monetary arithmetic operation overflowed the underlying integer.
    #[error("monetary overflow during {operation}")]
    MonetaryOverflow {
        /// The operation that overflowed (e.g. `add`, `sub`, `mul`).
        operation: &'static str,
    },

    /// A value that must be non-negative was negative.
    #[error("value must be non-negative, got {0}")]
    NegativeAmount(i128),

    /// A string identifier could not be parsed into its typed form.
    #[error("invalid identifier: {0}")]
    InvalidId(String),
}

/// Convenience alias for kernel results.
pub type Result<T> = std::result::Result<T, KernelError>;
