//! `Money` — an exact monetary amount as an integer count of minor units.
//!
//! **Never floating point** (ADR-0010). Binary floats cannot represent decimal fractions
//! exactly, so a ledger built on `f64` will fail to balance to the cent. Here money is an
//! `i128` count of minor units (e.g. cents) tagged with its [`Currency`]. All arithmetic is
//! checked (returns [`Result`] on overflow) and refuses to mix currencies.

use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::currency::Currency;
use crate::error::{KernelError, Result};

/// An exact amount of money in a single currency.
///
/// The amount is stored as a count of the currency's *minor units* (cents for USD). Positive,
/// negative, and zero are all valid at this level — sign conventions (e.g. "reserved is
/// non-negative") are enforced by the domain aggregates, not the value object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Money {
    minor_units: i128,
    currency: Currency,
}

impl Money {
    /// Construct from a raw minor-unit count.
    #[must_use]
    pub const fn from_minor(minor_units: i128, currency: Currency) -> Self {
        Self {
            minor_units,
            currency,
        }
    }

    /// A zero amount in the given currency.
    #[must_use]
    pub const fn zero(currency: Currency) -> Self {
        Self::from_minor(0, currency)
    }

    /// The raw minor-unit count.
    #[must_use]
    pub const fn minor_units(self) -> i128 {
        self.minor_units
    }

    /// The currency of this amount.
    #[must_use]
    pub const fn currency(self) -> Currency {
        self.currency
    }

    /// `true` if the amount is exactly zero.
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.minor_units == 0
    }

    /// `true` if the amount is strictly negative.
    #[must_use]
    pub const fn is_negative(self) -> bool {
        self.minor_units < 0
    }

    /// `true` if the amount is strictly positive.
    #[must_use]
    pub const fn is_positive(self) -> bool {
        self.minor_units > 0
    }

    /// Checked addition. Fails on currency mismatch or integer overflow.
    pub fn add(self, other: Self) -> Result<Self> {
        self.ensure_same_currency(other)?;
        let sum = self
            .minor_units
            .checked_add(other.minor_units)
            .ok_or(KernelError::MonetaryOverflow { operation: "add" })?;
        Ok(Self::from_minor(sum, self.currency))
    }

    /// Checked subtraction. Fails on currency mismatch or integer overflow.
    pub fn sub(self, other: Self) -> Result<Self> {
        self.ensure_same_currency(other)?;
        let diff = self
            .minor_units
            .checked_sub(other.minor_units)
            .ok_or(KernelError::MonetaryOverflow { operation: "sub" })?;
        Ok(Self::from_minor(diff, self.currency))
    }

    /// Checked multiplication by an integer scalar (e.g. quantity). Fails on overflow.
    pub fn mul_scalar(self, factor: i128) -> Result<Self> {
        let product = self
            .minor_units
            .checked_mul(factor)
            .ok_or(KernelError::MonetaryOverflow { operation: "mul" })?;
        Ok(Self::from_minor(product, self.currency))
    }

    /// Compare two amounts of the same currency. Fails on currency mismatch.
    pub fn compare(self, other: Self) -> Result<Ordering> {
        self.ensure_same_currency(other)?;
        Ok(self.minor_units.cmp(&other.minor_units))
    }

    /// `true` if `self >= other` (same currency required).
    pub fn at_least(self, other: Self) -> Result<bool> {
        Ok(self.compare(other)? != Ordering::Less)
    }

    fn ensure_same_currency(self, other: Self) -> Result<()> {
        if self.currency == other.currency {
            Ok(())
        } else {
            Err(KernelError::CurrencyMismatch {
                left: self.currency,
                right: other.currency,
            })
        }
    }
}

impl fmt::Display for Money {
    /// Formats with the currency's decimal places, e.g. `123.45 USD`, `1000 JPY`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let per_major = self.currency.minor_units_per_major();
        let sign = if self.minor_units < 0 { "-" } else { "" };
        let abs = self.minor_units.unsigned_abs();
        let major = abs / per_major.unsigned_abs();
        if self.currency.exponent() == 0 {
            write!(f, "{sign}{major} {}", self.currency)
        } else {
            let minor = abs % per_major.unsigned_abs();
            let width = self.currency.exponent() as usize;
            write!(f, "{sign}{major}.{minor:0width$} {}", self.currency)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn usd(m: i128) -> Money {
        Money::from_minor(m, Currency::Usd)
    }

    #[test]
    fn add_and_sub_same_currency() {
        assert_eq!(usd(100).add(usd(50)).unwrap(), usd(150));
        assert_eq!(usd(100).sub(usd(150)).unwrap(), usd(-50));
    }

    #[test]
    fn mixing_currencies_is_rejected() {
        let a = usd(100);
        let b = Money::from_minor(100, Currency::Eur);
        assert_eq!(
            a.add(b),
            Err(KernelError::CurrencyMismatch {
                left: Currency::Usd,
                right: Currency::Eur
            })
        );
        assert!(a.compare(b).is_err());
    }

    #[test]
    fn overflow_is_checked_not_wrapping() {
        let big = usd(i128::MAX);
        assert_eq!(
            big.add(usd(1)),
            Err(KernelError::MonetaryOverflow { operation: "add" })
        );
    }

    #[test]
    fn comparisons() {
        assert!(usd(100).at_least(usd(100)).unwrap());
        assert!(usd(101).at_least(usd(100)).unwrap());
        assert!(!usd(99).at_least(usd(100)).unwrap());
        assert_eq!(usd(1).compare(usd(2)).unwrap(), Ordering::Less);
    }

    #[test]
    fn display_formats_decimals() {
        assert_eq!(usd(12_345).to_string(), "123.45 USD");
        assert_eq!(usd(5).to_string(), "0.05 USD");
        assert_eq!(usd(-12_345).to_string(), "-123.45 USD");
        assert_eq!(
            Money::from_minor(1000, Currency::Jpy).to_string(),
            "1000 JPY"
        );
    }

    #[test]
    fn serde_round_trip() {
        let m = usd(9_999);
        let json = serde_json::to_string(&m).unwrap();
        let back: Money = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn zero_helpers() {
        assert!(usd(0).is_zero());
        assert!(usd(-1).is_negative());
        assert!(usd(1).is_positive());
    }
}
