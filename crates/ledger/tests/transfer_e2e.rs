//! In-process end-to-end test of the full write path: open accounts → credit → initiate a
//! transfer → drive the saga to completion → assert balances and conservation. Uses the
//! in-memory adapters so it runs in CI with no database or broker, yet exercises the real
//! command handlers, aggregate, and saga orchestrator together.

use std::sync::Arc;

use kernel::{Currency, Money, OwnerId, TransferId};
use ledger::adapters::outbound::memory::{
    InMemoryEventStore, InMemoryIdempotency, InMemoryReadModel, InMemoryTransfers,
};
use ledger::application::commands::CommandHandlers;
use ledger::application::ports::TransferStore;
use ledger::application::queries::{QueryHandlers, ReadModel};
use ledger::application::saga::SagaOrchestrator;
use ledger::domain::account::AccountCommand;
use ledger::domain::transfer::TransferState;

fn usd(m: i128) -> Money {
    Money::from_minor(m, Currency::Usd)
}

struct Harness {
    commands: CommandHandlers,
    queries: QueryHandlers,
    orchestrator: SagaOrchestrator,
    transfers: Arc<InMemoryTransfers>,
}

fn harness() -> Harness {
    let events = Arc::new(InMemoryEventStore::default());
    let transfers = Arc::new(InMemoryTransfers::default());
    let idem = Arc::new(InMemoryIdempotency::default());
    let read = Arc::new(InMemoryReadModel::new(
        (*events).clone(),
        (*transfers).clone(),
    ));

    let commands = CommandHandlers::new(events, transfers.clone(), idem);
    let queries = QueryHandlers::new(read);
    let orchestrator = SagaOrchestrator::new(commands.clone(), transfers.clone());
    Harness {
        commands,
        queries,
        orchestrator,
        transfers,
    }
}

#[tokio::test]
async fn happy_path_transfer_moves_money_and_conserves_total() {
    let h = harness();

    let alice = h
        .commands
        .open_account(OwnerId::new(), Currency::Usd, "c")
        .await
        .unwrap();
    let bob = h
        .commands
        .open_account(OwnerId::new(), Currency::Usd, "c")
        .await
        .unwrap();

    // Fund Alice with 1_000.
    h.commands
        .execute(
            alice,
            AccountCommand::Credit {
                transfer_id: TransferId::new(),
                amount: usd(1_000),
            },
            "c",
        )
        .await
        .unwrap();

    // Move 400 Alice -> Bob.
    let transfer_id = h
        .commands
        .initiate_transfer("idem-1", alice, bob, usd(400), "c")
        .await
        .unwrap();

    // Drive the saga to a terminal state.
    let saga = h.transfers.load(transfer_id).await.unwrap().unwrap();
    h.orchestrator.drive(saga).await.unwrap();

    // Assert final balances.
    let alice_view = h.queries.account(alice).await.unwrap().unwrap();
    let bob_view = h.queries.account(bob).await.unwrap().unwrap();
    assert_eq!(alice_view.posted.minor_units(), 600);
    assert_eq!(bob_view.posted.minor_units(), 400);
    assert_eq!(alice_view.reserved.minor_units(), 0);

    // Transfer reached COMPLETED.
    let tview = h.queries.transfer(transfer_id).await.unwrap().unwrap();
    assert_eq!(tview.status, "COMPLETED");

    // Conservation: total money unchanged by the transfer.
    assert_eq!(
        alice_view.posted.minor_units() + bob_view.posted.minor_units(),
        1_000
    );
}

#[tokio::test]
async fn insufficient_funds_transfer_fails_and_conserves() {
    let h = harness();
    let alice = h
        .commands
        .open_account(OwnerId::new(), Currency::Usd, "c")
        .await
        .unwrap();
    let bob = h
        .commands
        .open_account(OwnerId::new(), Currency::Usd, "c")
        .await
        .unwrap();
    h.commands
        .execute(
            alice,
            AccountCommand::Credit {
                transfer_id: TransferId::new(),
                amount: usd(100),
            },
            "c",
        )
        .await
        .unwrap();

    // Attempt to move more than Alice has.
    let transfer_id = h
        .commands
        .initiate_transfer("idem-2", alice, bob, usd(500), "c")
        .await
        .unwrap();
    let saga = h.transfers.load(transfer_id).await.unwrap().unwrap();
    h.orchestrator.drive(saga).await.unwrap();

    let final_saga = h.transfers.load(transfer_id).await.unwrap().unwrap();
    assert!(matches!(final_saga.state, TransferState::Failed { .. }));

    // No money moved; total conserved.
    let alice_view = h.queries.account(alice).await.unwrap().unwrap();
    let bob_view = h.queries.account(bob).await.unwrap().unwrap();
    assert_eq!(alice_view.posted.minor_units(), 100);
    assert_eq!(bob_view.posted.minor_units(), 0);
    assert_eq!(alice_view.available.minor_units(), 100); // reservation released
}

#[tokio::test]
async fn transfer_is_idempotent_under_replayed_drive() {
    let h = harness();
    let alice = h
        .commands
        .open_account(OwnerId::new(), Currency::Usd, "c")
        .await
        .unwrap();
    let bob = h
        .commands
        .open_account(OwnerId::new(), Currency::Usd, "c")
        .await
        .unwrap();
    h.commands
        .execute(
            alice,
            AccountCommand::Credit {
                transfer_id: TransferId::new(),
                amount: usd(1_000),
            },
            "c",
        )
        .await
        .unwrap();
    let transfer_id = h
        .commands
        .initiate_transfer("idem-3", alice, bob, usd(300), "c")
        .await
        .unwrap();

    // Drive twice — the second drive must not double-apply (idempotent effects).
    let saga = h.transfers.load(transfer_id).await.unwrap().unwrap();
    h.orchestrator.drive(saga).await.unwrap();
    let saga2 = h.transfers.load(transfer_id).await.unwrap().unwrap();
    h.orchestrator.drive(saga2).await.unwrap();

    let alice_view = h.queries.account(alice).await.unwrap().unwrap();
    let bob_view = h.queries.account(bob).await.unwrap().unwrap();
    assert_eq!(alice_view.posted.minor_units(), 700);
    assert_eq!(bob_view.posted.minor_units(), 300); // not 600
}
