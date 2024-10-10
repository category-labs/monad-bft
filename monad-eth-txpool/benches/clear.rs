use criterion::{criterion_group, criterion_main, Criterion};
use monad_consensus_types::txpool::TxPool;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_types::Balance;
use monad_state_backend::InMemoryState;
use monad_types::{SeqNum, GENESIS_SEQ_NUM};

use self::common::{run_txpool_benches, BenchController, SignatureCollectionType, EXECUTION_DELAY};

mod common;

fn criterion_benchmark(c: &mut Criterion) {
    // TODO: change this to something more meaningful, i.e. what's is the block
    // policy state we want to benchmark
    let block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, Balance::MAX, EXECUTION_DELAY, 1337);

    run_txpool_benches(
        c,
        "clear",
        |controller_config| {
            let mut controller = BenchController::setup(&block_policy, controller_config.clone());

            TxPool::<SignatureCollectionType, EthBlockPolicy, InMemoryState>::create_proposal(
                &mut controller.pool,
                controller.block_policy.get_last_commit() + SeqNum(1),
                controller.proposal_tx_limit,
                controller.gas_limit,
                controller.block_policy,
                Default::default(),
                &controller.state_backend,
            )
            .unwrap();

            controller.pool
        },
        |pool| {
            TxPool::<SignatureCollectionType, EthBlockPolicy, InMemoryState>::clear(pool);
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
