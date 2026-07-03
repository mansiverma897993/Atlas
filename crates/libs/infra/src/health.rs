//! Health aggregation for liveness/readiness/startup probes (ARCHITECTURE §6.5).
//!
//! * **Liveness** — the process is running (always OK if we can answer).
//! * **Readiness** — all registered dependency checks pass *and* startup has completed;
//!   controls whether k8s routes traffic and whether a rollout proceeds.
//! * **Startup** — flips to ready once one-time boot work (migrations applied, caches warm)
//!   is done, guarding slow first boots.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

/// A single dependency probe (e.g. "postgres", "redis").
#[async_trait]
pub trait HealthCheck: Send + Sync {
    /// Human-readable dependency name (appears in the readiness report).
    fn name(&self) -> &'static str;
    /// Returns `Ok(())` if the dependency is reachable/usable.
    async fn check(&self) -> Result<(), String>;
}

/// The outcome of one dependency check.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CheckResult {
    /// Dependency name.
    pub name: String,
    /// Whether it passed.
    pub healthy: bool,
    /// Error detail when unhealthy.
    pub detail: Option<String>,
}

/// Aggregate readiness report.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Readiness {
    /// Overall status.
    pub ready: bool,
    /// Per-dependency results.
    pub checks: Vec<CheckResult>,
}

/// Registry of dependency checks plus the startup gate.
#[derive(Clone)]
pub struct HealthRegistry {
    checks: Arc<Vec<Arc<dyn HealthCheck>>>,
    started: Arc<AtomicBool>,
}

impl HealthRegistry {
    /// Create an empty registry (not yet started).
    #[must_use]
    pub fn new(checks: Vec<Arc<dyn HealthCheck>>) -> Self {
        Self {
            checks: Arc::new(checks),
            started: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Mark one-time startup work complete (opens the startup/readiness gate).
    pub fn mark_started(&self) {
        self.started.store(true, Ordering::SeqCst);
    }

    /// Liveness: the process can answer.
    #[must_use]
    pub fn live(&self) -> bool {
        true
    }

    /// Startup: has boot work finished?
    #[must_use]
    pub fn started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Readiness: startup done AND all dependencies healthy.
    pub async fn readiness(&self) -> Readiness {
        let mut results = Vec::with_capacity(self.checks.len());
        let mut all_ok = self.started();
        for check in self.checks.iter() {
            let (healthy, detail) = match check.check().await {
                Ok(()) => (true, None),
                Err(e) => (false, Some(e)),
            };
            all_ok &= healthy;
            results.push(CheckResult {
                name: check.name().to_string(),
                healthy,
                detail,
            });
        }
        Readiness {
            ready: all_ok,
            checks: results,
        }
    }
}
