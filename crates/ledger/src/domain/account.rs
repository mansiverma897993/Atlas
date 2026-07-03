//! The **Account aggregate** — the ledger's consistency boundary (DOMAIN §2.1).
//!
//! Follows the classic event-sourced shape:
//! * [`Account::apply`] folds a single event into state (used for rehydration and after a
//!   successful decision).
//! * [`Account::decide`] is a **pure function** `command → Result<Vec<AccountEvent>>` that
//!   enforces every invariant. It never performs I/O and is exhaustively unit-tested.
//!
//! Invariants enforced here:
//! * `available = posted_balance − reserved ≥ 0` for customer accounts (no double-spend).
//! * No reservations/credits/captures on a non-open account.
//! * Commands are **idempotent** by `transfer_id`: replaying a reserve/capture/credit for a
//!   transfer already applied is a no-op (empty event list), which makes at-least-once bus
//!   delivery safe (exactly-once *effects*).

use std::collections::{HashMap, HashSet};

use kernel::{AccountId, Currency, Money, OwnerId, TransferId};

use super::error::DomainError;
use super::events::AccountEvent;

/// Lifecycle status of an account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountStatus {
    /// Fully operational.
    Open,
    /// No new reservations/credits accepted.
    Frozen,
    /// Terminal; no operations accepted.
    Closed,
}

impl AccountStatus {
    /// String form used in views and errors.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            AccountStatus::Open => "OPEN",
            AccountStatus::Frozen => "FROZEN",
            AccountStatus::Closed => "CLOSED",
        }
    }
}

/// Commands accepted by the Account aggregate.
#[derive(Debug, Clone)]
pub enum AccountCommand {
    /// Open a new account.
    Open {
        /// Owner id.
        owner: OwnerId,
        /// Account currency.
        currency: Currency,
    },
    /// Hold funds for a transfer (source leg, phase 1).
    Reserve {
        /// Transfer id (idempotency key).
        transfer_id: TransferId,
        /// Amount to hold.
        amount: Money,
    },
    /// Settle a held reservation into a posted debit (source leg, phase 3).
    Capture {
        /// Transfer id.
        transfer_id: TransferId,
    },
    /// Release a hold without capture (compensation).
    Release {
        /// Transfer id.
        transfer_id: TransferId,
        /// Reason.
        reason: String,
    },
    /// Credit funds (destination leg, phase 2).
    Credit {
        /// Transfer id.
        transfer_id: TransferId,
        /// Amount to credit.
        amount: Money,
    },
    /// Freeze the account.
    Freeze {
        /// Reason.
        reason: String,
    },
    /// Close the account.
    Close,
}

/// The rehydrated state of an Account (a fold over its event stream).
#[derive(Debug, Clone)]
pub struct Account {
    id: AccountId,
    owner: Option<OwnerId>,
    currency: Option<Currency>,
    status: Option<AccountStatus>,
    posted_balance: i128,
    reserved: i128,
    /// Active holds by transfer (amount in minor units).
    reservations: HashMap<TransferId, i128>,
    /// Transfers already credited (dedup for the credit leg).
    credited: HashSet<TransferId>,
    /// Transfers already captured (dedup for the capture leg).
    captured: HashSet<TransferId>,
    version: u64,
}

impl Account {
    /// A fresh, unopened aggregate for `id`. Rehydrate by applying its events.
    #[must_use]
    pub fn empty(id: AccountId) -> Self {
        Self {
            id,
            owner: None,
            currency: None,
            status: None,
            posted_balance: 0,
            reserved: 0,
            reservations: HashMap::new(),
            credited: HashSet::new(),
            captured: HashSet::new(),
            version: 0,
        }
    }

    /// Rehydrate from an ordered event history.
    #[must_use]
    pub fn rehydrate(id: AccountId, history: &[AccountEvent]) -> Self {
        let mut acc = Self::empty(id);
        for e in history {
            acc.apply(e);
        }
        acc
    }

    // ---- accessors (used by projections/queries) ----
    #[must_use]
    pub fn id(&self) -> AccountId {
        self.id
    }
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }
    #[must_use]
    pub fn exists(&self) -> bool {
        self.status.is_some()
    }
    #[must_use]
    pub fn status(&self) -> Option<AccountStatus> {
        self.status
    }
    #[must_use]
    pub fn currency(&self) -> Option<Currency> {
        self.currency
    }
    #[must_use]
    pub fn posted_balance(&self) -> i128 {
        self.posted_balance
    }
    #[must_use]
    pub fn reserved(&self) -> i128 {
        self.reserved
    }
    /// `available = posted − reserved`.
    #[must_use]
    pub fn available(&self) -> i128 {
        self.posted_balance - self.reserved
    }

    /// Fold one event into state. **Total** and side-effect-free. Bumps the version.
    pub fn apply(&mut self, event: &AccountEvent) {
        match event {
            AccountEvent::AccountOpened { owner, currency } => {
                self.owner = Some(*owner);
                self.currency = Some(*currency);
                self.status = Some(AccountStatus::Open);
            }
            AccountEvent::FundsReserved {
                transfer_id,
                amount,
            } => {
                self.reserved += amount.minor_units();
                self.reservations.insert(*transfer_id, amount.minor_units());
            }
            AccountEvent::ReservationReleased { transfer_id, .. } => {
                if let Some(amt) = self.reservations.remove(transfer_id) {
                    self.reserved -= amt;
                }
            }
            AccountEvent::FundsCaptured { transfer_id, .. } => {
                if let Some(amt) = self.reservations.remove(transfer_id) {
                    self.reserved -= amt;
                    self.posted_balance -= amt;
                }
                self.captured.insert(*transfer_id);
            }
            AccountEvent::FundsCredited {
                transfer_id,
                amount,
            } => {
                self.posted_balance += amount.minor_units();
                self.credited.insert(*transfer_id);
            }
            AccountEvent::AccountFrozen { .. } => self.status = Some(AccountStatus::Frozen),
            AccountEvent::AccountClosed => self.status = Some(AccountStatus::Closed),
        }
        self.version += 1;
    }

    /// Decide the events a command produces, or reject it. **Pure** — no I/O.
    ///
    /// Returns an empty vec for idempotent no-ops (command already applied), which the caller
    /// treats as success without appending anything.
    pub fn decide(&self, command: AccountCommand) -> Result<Vec<AccountEvent>, DomainError> {
        match command {
            AccountCommand::Open { owner, currency } => {
                if self.exists() {
                    return Err(DomainError::AlreadyOpened);
                }
                Ok(vec![AccountEvent::AccountOpened { owner, currency }])
            }

            AccountCommand::Reserve {
                transfer_id,
                amount,
            } => {
                // idempotent replay
                if self.reservations.contains_key(&transfer_id)
                    || self.captured.contains(&transfer_id)
                {
                    return Ok(vec![]);
                }
                self.ensure_open()?;
                self.ensure_currency(amount)?;
                self.ensure_positive(amount)?;
                if self.available() < amount.minor_units() {
                    return Err(DomainError::InsufficientFunds {
                        available: self.available().to_string(),
                        requested: amount.minor_units().to_string(),
                    });
                }
                Ok(vec![AccountEvent::FundsReserved {
                    transfer_id,
                    amount,
                }])
            }

            AccountCommand::Capture { transfer_id } => {
                // already captured (or reservation gone) => idempotent no-op
                let Some(amt) = self.reservations.get(&transfer_id).copied() else {
                    return Ok(vec![]);
                };
                let currency = self.currency.ok_or(DomainError::AccountNotFound)?;
                Ok(vec![AccountEvent::FundsCaptured {
                    transfer_id,
                    amount: Money::from_minor(amt, currency),
                }])
            }

            AccountCommand::Release {
                transfer_id,
                reason,
            } => {
                let Some(amt) = self.reservations.get(&transfer_id).copied() else {
                    return Ok(vec![]); // nothing to release => no-op
                };
                let currency = self.currency.ok_or(DomainError::AccountNotFound)?;
                Ok(vec![AccountEvent::ReservationReleased {
                    transfer_id,
                    amount: Money::from_minor(amt, currency),
                    reason,
                }])
            }

            AccountCommand::Credit {
                transfer_id,
                amount,
            } => {
                if self.credited.contains(&transfer_id) {
                    return Ok(vec![]); // idempotent replay
                }
                self.ensure_open()?;
                self.ensure_currency(amount)?;
                self.ensure_positive(amount)?;
                Ok(vec![AccountEvent::FundsCredited {
                    transfer_id,
                    amount,
                }])
            }

            AccountCommand::Freeze { reason } => {
                self.ensure_exists()?;
                if self.status == Some(AccountStatus::Frozen) {
                    return Ok(vec![]);
                }
                Ok(vec![AccountEvent::AccountFrozen { reason }])
            }

            AccountCommand::Close => {
                self.ensure_exists()?;
                if self.status == Some(AccountStatus::Closed) {
                    return Ok(vec![]);
                }
                if self.reserved != 0 {
                    return Err(DomainError::ReservationsOutstanding);
                }
                Ok(vec![AccountEvent::AccountClosed])
            }
        }
    }

    // ---- invariant helpers ----
    fn ensure_exists(&self) -> Result<(), DomainError> {
        if self.exists() {
            Ok(())
        } else {
            Err(DomainError::AccountNotFound)
        }
    }
    fn ensure_open(&self) -> Result<(), DomainError> {
        match self.status {
            Some(AccountStatus::Open) => Ok(()),
            Some(s) => Err(DomainError::AccountNotOpen(s.as_str().to_string())),
            None => Err(DomainError::AccountNotFound),
        }
    }
    fn ensure_currency(&self, amount: Money) -> Result<(), DomainError> {
        match self.currency {
            Some(c) if c == amount.currency() => Ok(()),
            Some(_) => Err(DomainError::CurrencyMismatch),
            None => Err(DomainError::AccountNotFound),
        }
    }
    fn ensure_positive(&self, amount: Money) -> Result<(), DomainError> {
        if amount.is_positive() {
            Ok(())
        } else {
            Err(DomainError::NonPositiveAmount)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usd(m: i128) -> Money {
        Money::from_minor(m, Currency::Usd)
    }

    /// Helper: apply a decided command and return the new state.
    fn exec(acc: &mut Account, cmd: AccountCommand) -> Result<Vec<AccountEvent>, DomainError> {
        let events = acc.decide(cmd)?;
        for e in &events {
            acc.apply(e);
        }
        Ok(events)
    }

    fn opened() -> Account {
        let mut acc = Account::empty(AccountId::new());
        exec(
            &mut acc,
            AccountCommand::Open {
                owner: OwnerId::new(),
                currency: Currency::Usd,
            },
        )
        .unwrap();
        acc
    }

    #[test]
    fn open_then_credit_increases_balance() {
        let mut acc = opened();
        let t = TransferId::new();
        exec(
            &mut acc,
            AccountCommand::Credit {
                transfer_id: t,
                amount: usd(1000),
            },
        )
        .unwrap();
        assert_eq!(acc.posted_balance(), 1000);
        assert_eq!(acc.available(), 1000);
    }

    #[test]
    fn cannot_open_twice() {
        let mut acc = opened();
        let err = acc
            .decide(AccountCommand::Open {
                owner: OwnerId::new(),
                currency: Currency::Usd,
            })
            .unwrap_err();
        assert_eq!(err, DomainError::AlreadyOpened);
    }

    #[test]
    fn reserve_then_capture_debits() {
        let mut acc = opened();
        let credit = TransferId::new();
        exec(
            &mut acc,
            AccountCommand::Credit {
                transfer_id: credit,
                amount: usd(500),
            },
        )
        .unwrap();
        let t = TransferId::new();
        exec(
            &mut acc,
            AccountCommand::Reserve {
                transfer_id: t,
                amount: usd(300),
            },
        )
        .unwrap();
        assert_eq!(acc.reserved(), 300);
        assert_eq!(acc.available(), 200); // 500 - 300 held
        exec(&mut acc, AccountCommand::Capture { transfer_id: t }).unwrap();
        assert_eq!(acc.reserved(), 0);
        assert_eq!(acc.posted_balance(), 200); // 500 - 300 captured
    }

    #[test]
    fn reserve_release_restores_available() {
        let mut acc = opened();
        exec(
            &mut acc,
            AccountCommand::Credit {
                transfer_id: TransferId::new(),
                amount: usd(500),
            },
        )
        .unwrap();
        let t = TransferId::new();
        exec(
            &mut acc,
            AccountCommand::Reserve {
                transfer_id: t,
                amount: usd(300),
            },
        )
        .unwrap();
        exec(
            &mut acc,
            AccountCommand::Release {
                transfer_id: t,
                reason: "compensate".into(),
            },
        )
        .unwrap();
        assert_eq!(acc.reserved(), 0);
        assert_eq!(acc.available(), 500);
    }

    #[test]
    fn overdraft_is_rejected() {
        let mut acc = opened();
        exec(
            &mut acc,
            AccountCommand::Credit {
                transfer_id: TransferId::new(),
                amount: usd(100),
            },
        )
        .unwrap();
        let err = acc
            .decide(AccountCommand::Reserve {
                transfer_id: TransferId::new(),
                amount: usd(101),
            })
            .unwrap_err();
        assert!(matches!(err, DomainError::InsufficientFunds { .. }));
    }

    #[test]
    fn reserve_is_idempotent_for_same_transfer() {
        let mut acc = opened();
        exec(
            &mut acc,
            AccountCommand::Credit {
                transfer_id: TransferId::new(),
                amount: usd(500),
            },
        )
        .unwrap();
        let t = TransferId::new();
        let first = exec(
            &mut acc,
            AccountCommand::Reserve {
                transfer_id: t,
                amount: usd(200),
            },
        )
        .unwrap();
        let second = exec(
            &mut acc,
            AccountCommand::Reserve {
                transfer_id: t,
                amount: usd(200),
            },
        )
        .unwrap();
        assert_eq!(first.len(), 1);
        assert!(second.is_empty()); // replay => no new event
        assert_eq!(acc.reserved(), 200); // not doubled
    }

    #[test]
    fn credit_is_idempotent_for_same_transfer() {
        let mut acc = opened();
        let t = TransferId::new();
        exec(
            &mut acc,
            AccountCommand::Credit {
                transfer_id: t,
                amount: usd(100),
            },
        )
        .unwrap();
        let replay = exec(
            &mut acc,
            AccountCommand::Credit {
                transfer_id: t,
                amount: usd(100),
            },
        )
        .unwrap();
        assert!(replay.is_empty());
        assert_eq!(acc.posted_balance(), 100);
    }

    #[test]
    fn frozen_account_rejects_reserve() {
        let mut acc = opened();
        exec(
            &mut acc,
            AccountCommand::Credit {
                transfer_id: TransferId::new(),
                amount: usd(100),
            },
        )
        .unwrap();
        exec(
            &mut acc,
            AccountCommand::Freeze {
                reason: "kyc".into(),
            },
        )
        .unwrap();
        let err = acc
            .decide(AccountCommand::Reserve {
                transfer_id: TransferId::new(),
                amount: usd(10),
            })
            .unwrap_err();
        assert!(matches!(err, DomainError::AccountNotOpen(_)));
    }

    #[test]
    fn cannot_close_with_reservations() {
        let mut acc = opened();
        exec(
            &mut acc,
            AccountCommand::Credit {
                transfer_id: TransferId::new(),
                amount: usd(100),
            },
        )
        .unwrap();
        exec(
            &mut acc,
            AccountCommand::Reserve {
                transfer_id: TransferId::new(),
                amount: usd(50),
            },
        )
        .unwrap();
        assert_eq!(
            acc.decide(AccountCommand::Close).unwrap_err(),
            DomainError::ReservationsOutstanding
        );
    }

    #[test]
    fn currency_mismatch_rejected() {
        let mut acc = opened();
        let err = acc
            .decide(AccountCommand::Credit {
                transfer_id: TransferId::new(),
                amount: Money::from_minor(100, Currency::Eur),
            })
            .unwrap_err();
        assert_eq!(err, DomainError::CurrencyMismatch);
    }

    #[test]
    fn rehydrate_equals_incremental() {
        let mut acc = opened();
        let mut events = vec![];
        let t = TransferId::new();
        for cmd in [
            AccountCommand::Credit {
                transfer_id: TransferId::new(),
                amount: usd(1000),
            },
            AccountCommand::Reserve {
                transfer_id: t,
                amount: usd(400),
            },
            AccountCommand::Capture { transfer_id: t },
        ] {
            events.extend(exec(&mut acc, cmd).unwrap());
        }
        // rebuild from the AccountOpened + subsequent events
        let full: Vec<AccountEvent> = {
            let mut all = vec![AccountEvent::AccountOpened {
                owner: acc.owner.unwrap(),
                currency: Currency::Usd,
            }];
            all.extend(events);
            all
        };
        let rebuilt = Account::rehydrate(acc.id(), &full);
        assert_eq!(rebuilt.posted_balance(), acc.posted_balance());
        assert_eq!(rebuilt.reserved(), acc.reserved());
        assert_eq!(rebuilt.version(), acc.version());
    }
}
