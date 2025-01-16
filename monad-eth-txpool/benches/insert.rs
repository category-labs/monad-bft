use bytes::Bytes;
use common::SignatureType;
use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use monad_consensus_types::{metrics::TxPoolEvents, txpool::TxPool};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::EthTxPool;
use monad_types::GENESIS_SEQ_NUM;

use self::common::{run_txpool_benches, BenchController, SignatureCollectionType, EXECUTION_DELAY};

mod common;

fn criterion_benchmark(c: &mut Criterion) {
    // TODO: change this to something more meaningful, i.e. what's is the block
    // policy state we want to benchmark
    let block_policy: EthBlockPolicy<SignatureType, SignatureCollectionType> =
        EthBlockPolicy::new(GENESIS_SEQ_NUM, EXECUTION_DELAY, 1337);

    run_txpool_benches(
        c,
        "insert",
        |controller_config| {
            let pool = EthTxPool::default_testing();

            let (pending_txs, txs) = BenchController::generate_txs(
                controller_config.accounts,
                controller_config.txs,
                controller_config.nonce_var,
                0,
            );

            assert!(pending_txs.is_empty());

            let state_backend = BenchController::generate_state_backend_for_txs(&txs);

            (
                pool,
                txs.iter()
                    .map(|t| Bytes::from(alloy_rlp::encode(t)))
                    .collect_vec(),
                state_backend,
            )
        },
        |state| {
            assert!(!TxPool::insert_tx(
                &mut state.0,
                state.1.to_owned(),
                &block_policy,
                &state.2,
                &mut TxPoolEvents::default()
            )
            .is_empty());
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
