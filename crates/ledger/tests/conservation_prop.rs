//! Property test (ROADMAP Phase 7): across *random* schedules of credits and transfers, the
//! ledger's core invariants must always hold:
//!   1. **Conservation** — total posted balance across all accounts equals the total credited
//!      in (transfers move money, never create or destroy it), regardless of which transfers
//!      succeed or fail.
//!   2. **No negative available** — no account ends with `available < 0` or a dangling hold.
//!
//! `proptest` explores hundreds of randomized interleavings; a single counterexample would be
//! shrunk to a minimal failing case.

use std::sync::Arc;

use kernel::{AccountId, Currency, Money, OwnerId, TransferId};
use ledger::adapters::outbound::memory::{
    InMemoryEventStore, InMemoryIdempotency, InMemoryTransfers,
};
use ledger::application::commands::CommandHandlers;
use ledger::application::ports::TransferStore;
use ledger::application::queries::QueryHandlers;
use ledger::application::saga::SagaOrchestrator;
use ledger::domain::account::AccountCommand;
use proptest::prelude::*;

const N_ACCOUNTS: usize = 4;

fn usd(m: i128) -> Money {
    Money::from_minor(m, Currency::Usd)
}

/// Run one scenario and return (accounts, total_credited, harness) for assertions.
async fn run_scenario(
    credits: Vec<i128>,
    transfers: Vec<(usize, usize, i128)>,
) -> (Vec<AccountId>, i128, QueryHandlers) {
    let events = Arc::new(InMemoryEventStore::default());
    let transfer_store = Arc::new(InMemoryTransfers::default());
    let idem = Arc::new(InMemoryIdempotency::default());
    let read = Arc::new(ledger::adapters::outbound::memory::InMemoryReadModel::new(
        (*events).clone(),
        (*transfer_store).clone(),
    ));
    let commands = CommandHandlers::new(events, transfer_store.clone(), idem);
    let queries = QueryHandlers::new(read);
    let orchestrator = SagaOrchestrator::new(commands.clone(), transfer_store.clone());

    // Open and fund accounts.
    let mut accounts = Vec::with_capacity(N_ACCOUNTS);
    let mut total_credited = 0i128;
    for &amount in credits.iter().take(N_ACCOUNTS) {
        let acc = commands
            .open_account(OwnerId::new(), Currency::Usd, "p")
            .await
            .unwrap();
        if amount > 0 {
            commands
                .execute(
                    acc,
                    AccountCommand::Credit {
                        transfer_id: TransferId::new(),
                        amount: usd(amount),
                    },
                    "p",
                )
                .await
                .unwrap();
            total_credited += amount;
        }
        accounts.push(acc);
    }

    // Execute transfers, driving each saga to a terminal state.
    for (i, (from, to, amount)) in transfers.into_iter().enumerate() {
        if from == to || amount <= 0 {
            continue;
        }
        let key = format!("t-{i}");
        let tid = commands
            .initiate_transfer(&key, accounts[from], accounts[to], usd(amount), "p")
            .await
            .unwrap();
        let saga = transfer_store.load(tid).await.unwrap().unwrap();
        orchestrator.drive(saga).await.unwrap();
    }

    (accounts, total_credited, queries)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(80))]

    #[test]
    fn conservation_and_no_overdraft(
        credits in prop::collection::vec(0i128..10_000, N_ACCOUNTS),
        transfers in prop::collection::vec(
            (0usize..N_ACCOUNTS, 0usize..N_ACCOUNTS, 1i128..5_000),
            0..25,
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (accounts, total_credited, queries) = run_scenario(credits, transfers).await;

            let mut total_posted = 0i128;
            for acc in accounts {
                let view = queries.account(acc).await.unwrap().unwrap();
                // invariant 2: no negative available, no dangling reservation at rest
                prop_assert!(view.available.minor_units() >= 0, "negative available balance");
                prop_assert_eq!(view.reserved.minor_units(), 0, "dangling reservation");
                total_posted += view.posted.minor_units();
            }
            // invariant 1: conservation
            prop_assert_eq!(total_posted, total_credited, "money was created or destroyed");
            Ok(())
        })?;
    }
}
