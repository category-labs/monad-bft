use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
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
        "create_proposal",
        |controller_config| BenchController::setup(&block_policy, controller_config.clone()),
        |BenchController {
             state_backend,
             block_policy,
             pool,
             pending_blocks,
             proposal_tx_limit,
             gas_limit,
         }| {
            TxPool::<SignatureCollectionType, EthBlockPolicy, InMemoryState>::create_proposal(
                pool,
                block_policy.get_last_commit() + SeqNum(1),
                *proposal_tx_limit,
                *gas_limit,
                block_policy,
                pending_blocks.iter().collect_vec(),
                state_backend,
            )
            .unwrap();
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
