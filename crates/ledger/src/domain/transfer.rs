//! The **transfer saga** as a pure state machine (DOMAIN §5).
//!
//! The saga coordinates a money movement across two Account aggregates using reserve → credit
//! → capture with compensation. This module holds only the *decision logic* — which step to
//! run next, and how to advance on success/failure. The I/O (issuing account commands,
//! persisting state, publishing events) lives in the `worker`'s orchestrator, which calls
//! these pure methods. Keeping the state machine pure makes every path unit-testable and the
//! failure/compensation logic explicit.

use kernel::{AccountId, Money, TransferId};
use serde::{Deserialize, Serialize};

/// The saga's state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransferState {
    /// Accepted, not yet started.
    Requested,
    /// Reserving funds on the source account (phase 1).
    Reserving,
    /// Crediting the destination account (phase 2).
    Crediting,
    /// Capturing the source reservation (phase 3).
    Capturing,
    /// Compensating a partial failure (releasing the reservation).
    Compensating {
        /// Why compensation was triggered.
        reason: String,
    },
    /// Terminal success.
    Completed,
    /// Terminal failure.
    Failed {
        /// Failure reason.
        reason: String,
    },
}

impl TransferState {
    /// String form for the `transfer_status_view` read model.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            TransferState::Requested => "REQUESTED",
            TransferState::Reserving => "RESERVING",
            TransferState::Crediting => "CREDITING",
            TransferState::Capturing => "CAPTURING",
            TransferState::Compensating { .. } => "COMPENSATING",
            TransferState::Completed => "COMPLETED",
            TransferState::Failed { .. } => "FAILED",
        }
    }

    /// Whether this is a terminal state (no further steps).
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TransferState::Completed | TransferState::Failed { .. }
        )
    }
}

/// The next action the orchestrator should perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferStep {
    /// Issue `Reserve` on the source account.
    ReserveSource,
    /// Issue `Credit` on the destination account.
    CreditDestination,
    /// Issue `Capture` on the source account.
    CaptureSource,
    /// Issue `Release` (compensation) on the source account.
    ReleaseSource {
        /// Reason forwarded onto the release event.
        reason: String,
    },
    /// Nothing to do — the saga has reached a terminal state.
    Done,
}

/// The saga instance: immutable identity + amounts, plus mutable [`TransferState`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferSaga {
    /// Transfer id (idempotency key across all account commands).
    pub transfer_id: TransferId,
    /// Debited account.
    pub source: AccountId,
    /// Credited account.
    pub destination: AccountId,
    /// Amount to move.
    pub amount: Money,
    /// Current state.
    pub state: TransferState,
}

impl TransferSaga {
    /// Create a freshly requested saga.
    #[must_use]
    pub fn new(
        transfer_id: TransferId,
        source: AccountId,
        destination: AccountId,
        amount: Money,
    ) -> Self {
        Self {
            transfer_id,
            source,
            destination,
            amount,
            state: TransferState::Requested,
        }
    }

    /// The next action to perform for the current state.
    #[must_use]
    pub fn next_step(&self) -> TransferStep {
        match &self.state {
            TransferState::Requested => TransferStep::ReserveSource,
            TransferState::Reserving => TransferStep::ReserveSource,
            TransferState::Crediting => TransferStep::CreditDestination,
            TransferState::Capturing => TransferStep::CaptureSource,
            TransferState::Compensating { reason } => TransferStep::ReleaseSource {
                reason: reason.clone(),
            },
            TransferState::Completed | TransferState::Failed { .. } => TransferStep::Done,
        }
    }

    /// Advance after the current step succeeded.
    pub fn on_success(&mut self) {
        self.state = match &self.state {
            TransferState::Requested | TransferState::Reserving => TransferState::Crediting,
            TransferState::Crediting => TransferState::Capturing,
            TransferState::Capturing => TransferState::Completed,
            // A successful compensation ends in Failed with the original reason.
            TransferState::Compensating { reason } => TransferState::Failed {
                reason: reason.clone(),
            },
            terminal => terminal.clone(),
        };
    }

    /// Advance after the current step failed with `reason`.
    ///
    /// * A failure during **reserve** (phase 1) needs no compensation — nothing was applied.
    /// * A failure during **credit** or **capture** requires releasing the source reservation.
    pub fn on_failure(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        self.state = match &self.state {
            TransferState::Requested | TransferState::Reserving => TransferState::Failed { reason },
            TransferState::Crediting | TransferState::Capturing => {
                TransferState::Compensating { reason }
            }
            // Compensation itself failing: remain compensating for retry (idempotent release).
            TransferState::Compensating { reason } => TransferState::Compensating {
                reason: reason.clone(),
            },
            terminal => terminal.clone(),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernel::Currency;

    fn saga() -> TransferSaga {
        TransferSaga::new(
            TransferId::new(),
            AccountId::new(),
            AccountId::new(),
            Money::from_minor(500, Currency::Usd),
        )
    }

    #[test]
    fn happy_path_reaches_completed() {
        let mut s = saga();
        assert_eq!(s.next_step(), TransferStep::ReserveSource);
        s.on_success(); // -> Crediting
        assert_eq!(s.next_step(), TransferStep::CreditDestination);
        s.on_success(); // -> Capturing
        assert_eq!(s.next_step(), TransferStep::CaptureSource);
        s.on_success(); // -> Completed
        assert_eq!(s.state, TransferState::Completed);
        assert_eq!(s.next_step(), TransferStep::Done);
    }

    #[test]
    fn reserve_failure_fails_without_compensation() {
        let mut s = saga();
        s.on_failure("insufficient funds");
        assert!(matches!(s.state, TransferState::Failed { .. }));
        assert_eq!(s.next_step(), TransferStep::Done);
    }

    #[test]
    fn credit_failure_triggers_compensation_then_failed() {
        let mut s = saga();
        s.on_success(); // Reserving -> Crediting
        s.on_failure("destination closed");
        assert!(matches!(s.state, TransferState::Compensating { .. }));
        assert!(matches!(s.next_step(), TransferStep::ReleaseSource { .. }));
        s.on_success(); // compensation done -> Failed
        assert!(matches!(s.state, TransferState::Failed { .. }));
    }

    #[test]
    fn capture_failure_compensates() {
        let mut s = saga();
        s.on_success(); // Crediting
        s.on_success(); // Capturing
        s.on_failure("infra");
        assert!(matches!(s.state, TransferState::Compensating { .. }));
    }
}
