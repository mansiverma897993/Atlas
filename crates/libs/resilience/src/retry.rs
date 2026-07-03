//! Retry an idempotent async operation under an [`ExponentialBackoff`] schedule.
//!
//! Only **idempotent** operations should be retried (ADR-0005/0006): a retried command must
//! not double-apply. The classifier decides which errors are transient (retry) vs terminal
//! (give up immediately, e.g. a validation error).

use std::future::Future;

use crate::backoff::ExponentialBackoff;

/// A retry policy: a backoff schedule plus a predicate deciding whether an error is transient.
pub struct RetryPolicy<E> {
    /// The delay schedule between attempts.
    pub backoff: ExponentialBackoff,
    /// Returns `true` if the error is transient and the operation should be retried.
    pub is_transient: fn(&E) -> bool,
}

impl<E> RetryPolicy<E> {
    /// A policy that retries every error under the default backoff.
    #[must_use]
    pub fn all_errors() -> Self {
        Self {
            backoff: ExponentialBackoff::default(),
            is_transient: |_| true,
        }
    }

    /// Build a policy with a custom classifier.
    #[must_use]
    pub fn new(backoff: ExponentialBackoff, is_transient: fn(&E) -> bool) -> Self {
        Self {
            backoff,
            is_transient,
        }
    }
}

/// Run `op`, retrying transient failures with jittered exponential backoff.
///
/// Returns the last error if the retry budget is exhausted or the error is terminal.
/// `op` is invoked fresh on each attempt (so it must be a factory closure).
pub async fn retry<T, E, F, Fut>(policy: &RetryPolicy<E>, mut op: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let mut attempt: u32 = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                let exhausted = attempt >= policy.backoff.max_retries;
                if exhausted || !(policy.is_transient)(&err) {
                    return Err(err);
                }
                let delay = policy.backoff.delay_for(attempt);
                tracing::debug!(
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    ?err,
                    "retrying after transient error"
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn fast_policy() -> RetryPolicy<&'static str> {
        RetryPolicy {
            backoff: ExponentialBackoff {
                base: Duration::from_millis(1),
                cap: Duration::from_millis(1),
                multiplier: 1.0,
                jitter: false,
                max_retries: 3,
            },
            is_transient: |_| true,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn succeeds_after_transient_failures() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let result: Result<u32, &str> = retry(&fast_policy(), || {
            let c = c.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err("transient")
                } else {
                    Ok(n)
                }
            }
        })
        .await;
        assert_eq!(result, Ok(2));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_budget() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let result: Result<u32, &str> = retry(&fast_policy(), || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err("always")
            }
        })
        .await;
        assert_eq!(result, Err("always"));
        // initial attempt + 3 retries = 4 invocations
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_error_is_not_retried() {
        let policy = RetryPolicy {
            backoff: fast_policy().backoff,
            is_transient: |e: &&str| *e != "terminal",
        };
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let _: Result<(), &str> = retry(&policy, || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err("terminal")
            }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
