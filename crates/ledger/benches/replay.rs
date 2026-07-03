//! Benchmark (ROADMAP Phase 7): the hot path of an event-sourced aggregate is **replay** —
//! folding a stream of events back into state on load. This measures `Account::rehydrate`
//! across stream lengths, which is what snapshotting (DOMAIN §4.1) exists to bound. Run with
//! `cargo bench -p ledger`.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use kernel::{AccountId, Currency, Money, OwnerId, TransferId};
use ledger::domain::account::Account;
use ledger::domain::events::AccountEvent;

/// Build a synthetic event stream: one open, then `n` credit/reserve/capture cycles.
fn make_stream(n: usize) -> Vec<AccountEvent> {
    let mut events = Vec::with_capacity(n * 3 + 1);
    events.push(AccountEvent::AccountOpened {
        owner: OwnerId::new(),
        currency: Currency::Usd,
    });
    for _ in 0..n {
        let t = TransferId::new();
        events.push(AccountEvent::FundsCredited {
            transfer_id: t,
            amount: Money::from_minor(100, Currency::Usd),
        });
        let t2 = TransferId::new();
        events.push(AccountEvent::FundsReserved {
            transfer_id: t2,
            amount: Money::from_minor(10, Currency::Usd),
        });
        events.push(AccountEvent::FundsCaptured {
            transfer_id: t2,
            amount: Money::from_minor(10, Currency::Usd),
        });
    }
    events
}

fn bench_replay(c: &mut Criterion) {
    let id = AccountId::new();
    let mut group = c.benchmark_group("account_replay");
    for &len in &[10usize, 100, 1_000, 10_000] {
        let stream = make_stream(len);
        group.bench_with_input(
            BenchmarkId::from_parameter(stream.len()),
            &stream,
            |b, s| {
                b.iter(|| {
                    let acc = Account::rehydrate(black_box(id), black_box(s));
                    black_box(acc.version());
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_replay);
criterion_main!(benches);
