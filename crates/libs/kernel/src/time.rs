//! Time abstractions.
//!
//! A [`Clock`] port lets the domain read "now" without depending on the system clock, so
//! time-dependent logic (reservation expiry, token TTLs) is deterministic under test. The
//! real adapter is [`SystemClock`]; tests use a fixed clock.

use chrono::{DateTime, Utc};

/// A source of the current instant. Injected wherever the domain needs wall-clock time.
pub trait Clock: Send + Sync {
    /// The current UTC instant.
    fn now(&self) -> DateTime<Utc>;
}

/// Production clock backed by the operating system.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A clock frozen at a fixed instant, for deterministic tests.
#[derive(Debug, Clone)]
pub struct FixedClock(pub DateTime<Utc>);

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_clock_is_deterministic() {
        let t = Utc::now();
        let clock = FixedClock(t);
        assert_eq!(clock.now(), t);
        assert_eq!(clock.now(), clock.now());
    }
}
