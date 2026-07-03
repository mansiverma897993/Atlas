//! The **transfer saga orchestrator** (DOMAIN §5). Drives each pending [`TransferSaga`] through
//! reserve → credit → capture with compensation, issuing idempotent Account commands through
//! [`CommandHandlers`] and persisting saga state after every transition.
//!
//! Durability & recovery: sagas live in the transfer store, so the orchestrator is a simple,
//! crash-safe loop — on restart it lists non-terminal sagas and resumes them from their last
//! recorded step. Because every Account command is idempotent by `transfer_id`, re-driving a
//! partially-applied saga is safe (exactly-once *effects*).

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::commands::{CommandError, CommandHandlers};
use super::ports::TransferStore;
use crate::domain::account::AccountCommand;
use crate::domain::transfer::{TransferSaga, TransferState, TransferStep};

/// Drives transfer sagas to completion.
#[derive(Clone)]
pub struct SagaOrchestrator {
    handlers: CommandHandlers,
    transfers: Arc<dyn TransferStore>,
    poll_interval: Duration,
    batch: u32,
}

impl SagaOrchestrator {
    /// Construct the orchestrator.
    pub fn new(handlers: CommandHandlers, transfers: Arc<dyn TransferStore>) -> Self {
        Self {
            handlers,
            transfers,
            poll_interval: Duration::from_millis(100),
            batch: 64,
        }
    }

    /// Run the orchestration loop until `cancel` fires (graceful shutdown).
    pub async fn run(&self, cancel: CancellationToken) {
        tracing::info!("saga orchestrator started");
        loop {
            if cancel.is_cancelled() {
                tracing::info!("saga orchestrator stopping");
                return;
            }
            match self.transfers.list_pending(self.batch).await {
                Ok(pending) if pending.is_empty() => {
                    tokio::select! {
                        () = tokio::time::sleep(self.poll_interval) => {}
                        () = cancel.cancelled() => return,
                    }
                }
                Ok(pending) => {
                    for saga in pending {
                        if let Err(e) = self.drive(saga).await {
                            tracing::warn!(error = %e, "saga step deferred (transient)");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to list pending sagas");
                    tokio::time::sleep(self.poll_interval).await;
                }
            }
        }
    }

    /// Drive a single saga forward until it reaches a terminal state or hits a transient error
    /// (which is returned so the loop retries later). Persists after every transition.
    pub async fn drive(&self, mut saga: TransferSaga) -> Result<(), CommandError> {
        let correlation = saga.transfer_id.to_string();
        loop {
            let step = saga.next_step();
            let outcome = match step {
                TransferStep::Done => {
                    self.record_terminal(&saga);
                    return Ok(());
                }
                TransferStep::ReserveSource => {
                    self.handlers
                        .execute(
                            saga.source,
                            AccountCommand::Reserve {
                                transfer_id: saga.transfer_id,
                                amount: saga.amount,
                            },
                            &correlation,
                        )
                        .await
                }
                TransferStep::CreditDestination => {
                    self.handlers
                        .execute(
                            saga.destination,
                            AccountCommand::Credit {
                                transfer_id: saga.transfer_id,
                                amount: saga.amount,
                            },
                            &correlation,
                        )
                        .await
                }
                TransferStep::CaptureSource => {
                    self.handlers
                        .execute(
                            saga.source,
                            AccountCommand::Capture {
                                transfer_id: saga.transfer_id,
                            },
                            &correlation,
                        )
                        .await
                }
                TransferStep::ReleaseSource { reason } => {
                    self.handlers
                        .execute(
                            saga.source,
                            AccountCommand::Release {
                                transfer_id: saga.transfer_id,
                                reason,
                            },
                            &correlation,
                        )
                        .await
                }
            };

            match outcome {
                Ok(_) => saga.on_success(),
                // A business-rule rejection is a *decision*, not a fault: advance the saga
                // toward compensation/failure with the reason.
                Err(CommandError::Domain(d)) => saga.on_failure(d.to_string()),
                // An infra/store fault is transient: persist nothing new, surface it so the
                // loop retries the same step later (the account command is idempotent).
                Err(e @ CommandError::Port(_)) => return Err(e),
            }
            self.transfers.save(&saga, &correlation).await?;
        }
    }

    fn record_terminal(&self, saga: &TransferSaga) {
        match &saga.state {
            TransferState::Completed => {
                metrics::counter!("ledger_transfers_total", "status" => "completed").increment(1);
            }
            TransferState::Failed { .. } => {
                metrics::counter!("ledger_transfers_total", "status" => "failed").increment(1);
            }
            _ => {}
        }
    }
}
