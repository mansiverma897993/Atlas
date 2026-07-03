//! Exponential backoff with full jitter.
//!
//! Delay for attempt `n` (0-indexed) is `min(cap, base * multiplier^n)`, then randomized with
//! *full jitter* — a uniform sample in `[0, delay]` — which is the AWS-recommended strategy
//! for avoiding synchronized retry storms (the "thundering herd").

use std::time::Duration;

use rand::Rng;

/// A backoff schedule. Cheap to clone; stateless — the caller tracks the attempt number.
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    /// Delay for the first retry.
    pub base: Duration,
    /// Upper bound on any single delay.
    pub cap: Duration,
    /// Growth factor between attempts.
    pub multiplier: f64,
    /// Apply full jitter to each delay.
    pub jitter: bool,
    /// Maximum number of retries (not counting the initial attempt).
    pub max_retries: u32,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(50),
            cap: Duration::from_secs(10),
            multiplier: 2.0,
            jitter: true,
            max_retries: 5,
        }
    }
}

impl ExponentialBackoff {
    /// The (un-jittered) exponential delay for a given 0-indexed attempt, clamped to `cap`.
    #[must_use]
    pub fn raw_delay(&self, attempt: u32) -> Duration {
        let factor = self.multiplier.powi(attempt as i32);
        let millis = (self.base.as_millis() as f64 * factor).min(self.cap.as_millis() as f64);
        Duration::from_millis(millis as u64)
    }

    /// The delay to sleep before `attempt` (0-indexed), with full jitter if enabled.
    #[must_use]
    pub fn delay_for(&self, attempt: u32) -> Duration {
        let raw = self.raw_delay(attempt);
        if self.jitter && !raw.is_zero() {
            let jittered = rand::thread_rng().gen_range(0..=raw.as_millis() as u64);
            Duration::from_millis(jittered)
        } else {
            raw
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_grows_and_is_capped() {
        let b = ExponentialBackoff {
            base: Duration::from_millis(100),
            cap: Duration::from_secs(1),
            multiplier: 2.0,
            jitter: false,
            max_retries: 10,
        };
        assert_eq!(b.raw_delay(0), Duration::from_millis(100));
        assert_eq!(b.raw_delay(1), Duration::from_millis(200));
        assert_eq!(b.raw_delay(2), Duration::from_millis(400));
        // capped
        assert_eq!(b.raw_delay(20), Duration::from_secs(1));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let b = ExponentialBackoff {
            base: Duration::from_millis(100),
            cap: Duration::from_secs(10),
            multiplier: 2.0,
            jitter: true,
            max_retries: 10,
        };
        for attempt in 0..5 {
            let raw = b.raw_delay(attempt);
            for _ in 0..50 {
                assert!(b.delay_for(attempt) <= raw);
            }
        }
    }
}
