//! Cron scheduler for periodic background jobs (ARCHITECTURE §3).
//!
//! Built on `tokio-cron-scheduler`. Jobs are **pluggable**: anything implementing the
//! [`ScheduledJob`] trait (a cron `schedule()` + an async `run()`) can be registered. The
//! [`Scheduler`] wires each into the underlying `JobScheduler`, wrapping `run()` so every tick
//! increments the `scheduler_runs_total` metric.
//!
//! Two concrete jobs ship by default: a periodic **reservation-expiry sweep** trigger and a
//! **daily statement generation** job. For this build they log and emit metrics (and could
//! call the Ledger gRPC), which is enough to exercise the wiring end-to-end.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_cron_scheduler::{Job, JobScheduler};

/// A pluggable scheduled job: a cron expression plus the work to run on each tick.
#[async_trait]
pub trait ScheduledJob: Send + Sync {
    /// Stable job name (used as a metric label and in logs).
    fn name(&self) -> &'static str;

    /// Cron expression in `tokio-cron-scheduler` form: `sec min hour dom mon dow` (7th `year`
    /// field optional). E.g. `"0 0 0 * * *"` = daily at midnight UTC.
    fn schedule(&self) -> &str;

    /// The work to perform on each firing. Must be self-contained and idempotent.
    async fn run(&self);
}

/// Owns the underlying cron scheduler and the set of registered jobs.
pub struct Scheduler {
    inner: JobScheduler,
    jobs: Vec<Arc<dyn ScheduledJob>>,
}

impl Scheduler {
    /// Create an empty scheduler.
    pub async fn new() -> anyhow::Result<Self> {
        let inner = JobScheduler::new().await?;
        Ok(Self {
            inner,
            jobs: Vec::new(),
        })
    }

    /// Register a job. The closure handed to the cron library clones the `Arc` per tick and
    /// records the run before delegating to [`ScheduledJob::run`].
    pub async fn register(&mut self, job: Arc<dyn ScheduledJob>) -> anyhow::Result<()> {
        let schedule = job.schedule().to_string();
        let task = Arc::clone(&job);
        let cron_job = Job::new_async(schedule.as_str(), move |_uuid, _lock| {
            let task = Arc::clone(&task);
            Box::pin(async move {
                metrics::counter!("scheduler_runs_total", "job" => task.name()).increment(1);
                tracing::info!(job = task.name(), "scheduled job firing");
                task.run().await;
            })
        })?;
        self.inner.add(cron_job).await?;
        self.jobs.push(job);
        Ok(())
    }

    /// Start ticking. Jobs fire on their schedules until [`Scheduler::shutdown`].
    pub async fn start(&self) -> anyhow::Result<()> {
        self.inner.start().await?;
        tracing::info!(jobs = self.jobs.len(), "scheduler started");
        Ok(())
    }

    /// Stop the scheduler, cancelling pending ticks.
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.inner.shutdown().await?;
        Ok(())
    }
}

// ---- Default jobs -------------------------------------------------------------------------

/// Periodically triggers a sweep of expired reservations. In the full system this would ask
/// the ledger to release reservations whose hold has elapsed (DOMAIN §2.1); here it emits a
/// metric/log so the schedule is observable.
pub struct ReservationExpirySweep {
    /// Cron schedule (default: every 30 seconds).
    pub schedule: String,
}

impl Default for ReservationExpirySweep {
    fn default() -> Self {
        // Every 30 seconds: seconds field `0/30`.
        Self {
            schedule: "0/30 * * * * *".to_string(),
        }
    }
}

#[async_trait]
impl ScheduledJob for ReservationExpirySweep {
    fn name(&self) -> &'static str {
        "reservation_expiry_sweep"
    }
    fn schedule(&self) -> &str {
        &self.schedule
    }
    async fn run(&self) {
        // Placeholder for the ledger gRPC sweep call; emit a domain metric for now.
        metrics::counter!("reservation_sweeps_total").increment(1);
        tracing::debug!("reservation-expiry sweep triggered");
    }
}

/// Generates account statements once per day. Emits a metric/log for the demo.
pub struct DailyStatementGeneration {
    /// Cron schedule (default: daily at 00:00 UTC).
    pub schedule: String,
}

impl Default for DailyStatementGeneration {
    fn default() -> Self {
        Self {
            schedule: "0 0 0 * * *".to_string(),
        }
    }
}

#[async_trait]
impl ScheduledJob for DailyStatementGeneration {
    fn name(&self) -> &'static str {
        "daily_statement_generation"
    }
    fn schedule(&self) -> &str {
        &self.schedule
    }
    async fn run(&self) {
        metrics::counter!("statements_generated_total").increment(1);
        tracing::info!("daily statement generation triggered");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Probe {
        ran: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl ScheduledJob for Probe {
        fn name(&self) -> &'static str {
            "probe"
        }
        fn schedule(&self) -> &str {
            // Every second, so the test observes a tick quickly.
            "* * * * * *"
        }
        async fn run(&self) {
            self.ran.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn default_schedules_parse_as_valid_cron() {
        // Constructing the Job validates the cron expression; a parse error would be an Err.
        for schedule in [
            ReservationExpirySweep::default().schedule().to_string(),
            DailyStatementGeneration::default().schedule().to_string(),
        ] {
            let job = Job::new_async(schedule.as_str(), |_u, _l| Box::pin(async {}));
            assert!(job.is_ok(), "schedule {schedule:?} should be valid cron");
        }
    }

    #[tokio::test]
    async fn registers_and_fires_a_job() {
        let ran = Arc::new(AtomicUsize::new(0));
        let mut sched = Scheduler::new().await.expect("scheduler");
        sched
            .register(Arc::new(Probe { ran: ran.clone() }))
            .await
            .expect("register");
        sched.start().await.expect("start");
        // Wait long enough for at least one 1-second tick.
        tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
        sched.shutdown().await.expect("shutdown");
        assert!(
            ran.load(Ordering::SeqCst) >= 1,
            "job should have fired at least once"
        );
    }
}
