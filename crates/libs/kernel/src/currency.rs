//! ISO-4217 currency with its minor-unit exponent.
//!
//! A single account holds exactly one currency (v1 — no in-ledger FX; see ARCHITECTURE §10).
//! The `exponent` is the number of decimal places in the currency's minor unit and is used
//! only for *display* — all arithmetic happens in integer minor units (see [`crate::Money`]).

use std::fmt;

use serde::{Deserialize, Serialize};

/// A supported ISO-4217 currency.
///
/// Kept as a closed enum (rather than a free-form code) so that an unknown currency is
/// unrepresentable and currency handling is exhaustively checked by the compiler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Currency {
    /// United States dollar.
    Usd,
    /// Euro.
    Eur,
    /// Pound sterling.
    Gbp,
    /// Japanese yen (zero minor units).
    Jpy,
    /// Indian rupee.
    Inr,
}

impl Currency {
    /// Number of decimal places in this currency's minor unit.
    ///
    /// e.g. USD has 2 (cents), JPY has 0 (no sub-unit).
    #[must_use]
    pub const fn exponent(self) -> u32 {
        match self {
            Currency::Usd | Currency::Eur | Currency::Gbp | Currency::Inr => 2,
            Currency::Jpy => 0,
        }
    }

    /// The ISO-4217 alphabetic code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Currency::Usd => "USD",
            Currency::Eur => "EUR",
            Currency::Gbp => "GBP",
            Currency::Jpy => "JPY",
            Currency::Inr => "INR",
        }
    }

    /// Parse from an ISO-4217 alphabetic code (case-insensitive).
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        match code.to_ascii_uppercase().as_str() {
            "USD" => Some(Currency::Usd),
            "EUR" => Some(Currency::Eur),
            "GBP" => Some(Currency::Gbp),
            "JPY" => Some(Currency::Jpy),
            "INR" => Some(Currency::Inr),
            _ => None,
        }
    }

    /// `10^exponent` — the number of minor units in one major unit.
    #[must_use]
    pub const fn minor_units_per_major(self) -> i128 {
        10_i128.pow(self.exponent())
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_via_code() {
        for c in [
            Currency::Usd,
            Currency::Eur,
            Currency::Gbp,
            Currency::Jpy,
            Currency::Inr,
        ] {
            assert_eq!(Currency::from_code(c.code()), Some(c));
        }
    }

    #[test]
    fn code_parsing_is_case_insensitive() {
        assert_eq!(Currency::from_code("usd"), Some(Currency::Usd));
        assert_eq!(Currency::from_code("uSd"), Some(Currency::Usd));
        assert_eq!(Currency::from_code("xxx"), None);
    }

    #[test]
    fn exponents_are_correct() {
        assert_eq!(Currency::Usd.exponent(), 2);
        assert_eq!(Currency::Jpy.exponent(), 0);
        assert_eq!(Currency::Usd.minor_units_per_major(), 100);
        assert_eq!(Currency::Jpy.minor_units_per_major(), 1);
    }
}
