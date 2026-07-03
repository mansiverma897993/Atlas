//! A circuit breaker: `Closed → Open → HalfOpen`.
//!
//! Protects a caller from hammering a failing dependency. While **Closed**, calls flow and
//! outcomes are recorded in a rolling window; if the failure ratio crosses the threshold
//! (with a minimum sample size), the breaker trips **Open** and rejects calls immediately for
//! a cool-down. After the cool-down it goes **HalfOpen** and admits a limited number of trial
//! calls: enough consecutive successes close it; any failure re-opens it.
//!
//! State transitions are time-driven via an injected [`Clock`], so behavior is fully
//! deterministic under test. The breaker is `Send + Sync` and cheap to share (`Arc`).

use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};
use kernel::time::Clock;
use thiserror::Error;

/// Observable breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Calls flow normally; outcomes are sampled.
    Closed,
    /// Calls are rejected immediately until the cool-down elapses.
    Open,
    /// A limited number of trial calls are admitted to test recovery.
    HalfOpen,
}

/// Error returned when the breaker is open (the guarded call was not attempted).
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("circuit breaker is open; call rejected")]
pub struct CircuitError;

/// Tuning for a [`CircuitBreaker`].
#[derive(Debug, Clone)]
pub struct CircuitConfig {
    /// Failure ratio in `(0.0, 1.0]` that trips the breaker.
    pub failure_ratio: f64,
    /// Minimum sampled calls before the ratio is evaluated (avoids tripping on 1/1).
    pub minimum_calls: u32,
    /// How long to stay open before probing (HalfOpen).
    pub open_cooldown: Duration,
    /// Consecutive successes in HalfOpen required to close.
    pub half_open_successes: u32,
    /// Size of the rolling outcome window used in Closed.
    pub window_size: u32,
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            failure_ratio: 0.5,
            minimum_calls: 10,
            open_cooldown: Duration::from_secs(5),
            half_open_successes: 3,
            window_size: 50,
        }
    }
}

#[derive(Debug)]
struct Inner {
    state: CircuitState,
    /// Rolling window of recent outcomes (`true` = success). Used in Closed.
    window: std::collections::VecDeque<bool>,
    /// When the breaker last tripped Open (drives the cool-down).
    opened_at: Option<DateTime<Utc>>,
    /// Consecutive successes observed in HalfOpen.
    half_open_successes: u32,
    /// Trial calls admitted in the current HalfOpen episode.
    half_open_calls: u32,
}

/// A thread-safe circuit breaker.
pub struct CircuitBreaker<C: Clock> {
    config: CircuitConfig,
    clock: C,
    inner: Mutex<Inner>,
}

impl<C: Clock> CircuitBreaker<C> {
    /// Create a breaker in the `Closed` state.
    pub fn new(config: CircuitConfig, clock: C) -> Self {
        Self {
            config,
            clock,
            inner: Mutex::new(Inner {
                state: CircuitState::Closed,
                window: std::collections::VecDeque::new(),
                opened_at: None,
                half_open_successes: 0,
                half_open_calls: 0,
            }),
        }
    }

    /// Current state (advancing Open → HalfOpen if the cool-down has elapsed).
    pub fn state(&self) -> CircuitState {
        let mut inner = self.inner.lock().expect("breaker mutex poisoned");
        self.refresh(&mut inner);
        inner.state
    }

    /// Guard an async operation. Returns [`CircuitError`] without calling `op` if open.
    ///
    /// `op` is expected to be **idempotent** — the breaker may be combined with retries.
    pub async fn call<T, E, F, Fut>(&self, op: F) -> Result<Result<T, E>, CircuitError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        self.acquire()?;
        let outcome = op().await;
        self.record(outcome.is_ok());
        Ok(outcome)
    }

    /// Check admission and reserve a HalfOpen trial slot if applicable.
    fn acquire(&self) -> Result<(), CircuitError> {
        let mut inner = self.inner.lock().expect("breaker mutex poisoned");
        self.refresh(&mut inner);
        match inner.state {
            CircuitState::Open => Err(CircuitError),
            CircuitState::HalfOpen => {
                // Admit only up to `half_open_successes` concurrent trials.
                if inner.half_open_calls >= self.config.half_open_successes {
                    return Err(CircuitError);
                }
                inner.half_open_calls += 1;
                Ok(())
            }
            CircuitState::Closed => Ok(()),
        }
    }

    /// Record the outcome of a guarded call and advance the state machine.
    fn record(&self, success: bool) {
        let mut inner = self.inner.lock().expect("breaker mutex poisoned");
        match inner.state {
            CircuitState::Closed => {
                if inner.window.len() as u32 >= self.config.window_size {
                    inner.window.pop_front();
                }
                inner.window.push_back(success);
                self.maybe_trip(&mut inner);
            }
            CircuitState::HalfOpen => {
                if success {
                    inner.half_open_successes += 1;
                    if inner.half_open_successes >= self.config.half_open_successes {
                        self.close(&mut inner);
                    }
                } else {
                    self.trip(&mut inner);
                }
            }
            CircuitState::Open => { /* shouldn't happen: acquire() rejects first */ }
        }
    }

    /// Move Open → HalfOpen once the cool-down has elapsed.
    fn refresh(&self, inner: &mut Inner) {
        if inner.state == CircuitState::Open {
            if let Some(opened) = inner.opened_at {
                let elapsed = self.clock.now().signed_duration_since(opened);
                if elapsed.to_std().unwrap_or_default() >= self.config.open_cooldown {
                    inner.state = CircuitState::HalfOpen;
                    inner.half_open_successes = 0;
                    inner.half_open_calls = 0;
                }
            }
        }
    }

    fn maybe_trip(&self, inner: &mut Inner) {
        let total = inner.window.len() as u32;
        if total < self.config.minimum_calls {
            return;
        }
        let failures = inner.window.iter().filter(|ok| !**ok).count() as f64;
        if failures / f64::from(total) >= self.config.failure_ratio {
            self.trip(inner);
        }
    }

    fn trip(&self, inner: &mut Inner) {
        inner.state = CircuitState::Open;
        inner.opened_at = Some(self.clock.now());
        inner.window.clear();
        tracing::warn!("circuit breaker tripped OPEN");
    }

    fn close(&self, inner: &mut Inner) {
        inner.state = CircuitState::Closed;
        inner.window.clear();
        inner.opened_at = None;
        inner.half_open_successes = 0;
        inner.half_open_calls = 0;
        tracing::info!("circuit breaker CLOSED (recovered)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernel::time::FixedClock;
    use std::sync::Arc;

    fn breaker(clock: Arc<Mutex<DateTime<Utc>>>) -> CircuitBreaker<TestClock> {
        CircuitBreaker::new(
            CircuitConfig {
                failure_ratio: 0.5,
                minimum_calls: 4,
                open_cooldown: Duration::from_secs(5),
                half_open_successes: 2,
                window_size: 10,
            },
            TestClock(clock),
        )
    }

    #[derive(Clone)]
    struct TestClock(Arc<Mutex<DateTime<Utc>>>);
    impl Clock for TestClock {
        fn now(&self) -> DateTime<Utc> {
            *self.0.lock().unwrap()
        }
    }

    #[tokio::test]
    async fn trips_open_after_failure_ratio_exceeded() {
        let now = Arc::new(Mutex::new(Utc::now()));
        let cb = breaker(now.clone());
        // 4 calls, 2 failures => ratio 0.5 >= threshold => trips.
        for i in 0..4 {
            let _ = cb
                .call(|| async move {
                    if i % 2 == 0 {
                        Ok::<_, ()>(())
                    } else {
                        Err(())
                    }
                })
                .await;
        }
        assert_eq!(cb.state(), CircuitState::Open);
        // While open, calls are rejected without executing.
        let rejected = cb.call(|| async { Ok::<_, ()>(()) }).await;
        assert_eq!(rejected, Err(CircuitError));
    }

    #[tokio::test]
    async fn recovers_through_half_open() {
        let now = Arc::new(Mutex::new(Utc::now()));
        let cb = breaker(now.clone());
        for _ in 0..4 {
            let _ = cb.call(|| async { Err::<(), ()>(()) }).await;
        }
        assert_eq!(cb.state(), CircuitState::Open);
        // advance past cool-down
        *now.lock().unwrap() = Utc::now() + chrono::Duration::seconds(6);
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        // two successful trials close it
        let _ = cb.call(|| async { Ok::<_, ()>(()) }).await;
        let _ = cb.call(|| async { Ok::<_, ()>(()) }).await;
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn half_open_failure_reopens() {
        let now = Arc::new(Mutex::new(Utc::now()));
        let cb = breaker(now.clone());
        for _ in 0..4 {
            let _ = cb.call(|| async { Err::<(), ()>(()) }).await;
        }
        *now.lock().unwrap() = Utc::now() + chrono::Duration::seconds(6);
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        let _ = cb.call(|| async { Err::<(), ()>(()) }).await;
        assert_eq!(cb.state(), CircuitState::Open);
    }

    // Ensure FixedClock (from kernel) satisfies the same bound used in production.
    #[tokio::test]
    async fn works_with_fixed_clock() {
        let cb = CircuitBreaker::new(CircuitConfig::default(), FixedClock(Utc::now()));
        assert_eq!(cb.state(), CircuitState::Closed);
    }
}
