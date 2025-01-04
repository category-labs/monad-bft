use alloy_rlp::Encodable;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::EthTxPool;
use monad_types::GENESIS_SEQ_NUM;

use self::common::{
    run_txpool_benches, BenchController, Pool, SignatureCollectionType, SignatureType,
    StateBackendType, EXECUTION_DELAY,
};

mod common;

fn criterion_benchmark(c: &mut Criterion) {
    // TODO: change this to something more meaningful, i.e. what's is the block
    // policy state we want to benchmark
    let block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, EXECUTION_DELAY, 1337);

    run_txpool_benches(
        c,
        "insert",
        |controller_config| {
            let pool =
                EthTxPool::<SignatureType, SignatureCollectionType, StateBackendType>::new(true);

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
                    .map(|tx| {
                        let mut rlp_tx = Vec::new();
                        tx.encode(&mut rlp_tx);
                        Bytes::from(rlp_tx)
                    })
                    .collect_vec(),
                state_backend,
            )
        },
        |state| {
            assert!(
                !Pool::insert_tx(&mut state.0, state.1.to_owned(), &block_policy, &state.2,)
                    .is_empty()
            );
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
