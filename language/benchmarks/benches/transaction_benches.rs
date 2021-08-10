// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use criterion::{criterion_group, criterion_main, measurement::Measurement, Criterion};
use language_benchmarks::{measurement::wall_time_measurement, transactions::TransactionBencher};
use language_e2e_tests::account_universe::P2PTransferGen;
use proptest::prelude::*;

//
// Transaction benchmarks
//

fn peer_to_peer<M: Measurement + 'static>(c: &mut Criterion<M>) {
    c.bench_function("peer_to_peer", |b| {
        let bencher = TransactionBencher::new(any_with::<P2PTransferGen>((1_000, 1_000_000)));
        bencher.bench(b)
    });
}

fn parallel_execution_correctness_test<M: Measurement + 'static>(c: &mut Criterion<M>) {
    c.bench_function("parallel_execution_correctness_test", |b| {
        let bencher = TransactionBencher::new(any_with::<P2PTransferGen>((1_000, 1_000_000)));
        bencher.bench_correctness_test(b)
    });
}

criterion_group!(
    name = txn_benches;
    config = wall_time_measurement().sample_size(10);
    targets = peer_to_peer,
    parallel_execution_correctness_test
);

criterion_main!(txn_benches);
