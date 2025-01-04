use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use monad_eth_block_policy::EthBlockPolicy;
use monad_types::{SeqNum, GENESIS_SEQ_NUM};

use self::common::{
    make_test_round_signature, run_txpool_benches, BenchController, Pool, EXECUTION_DELAY,
    MOCK_BENEFICIARY, MOCK_TIMESTAMP,
};

mod common;

fn criterion_benchmark(c: &mut Criterion) {
    // TODO: change this to something more meaningful, i.e. what's is the block
    // policy state we want to benchmark
    let block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, EXECUTION_DELAY, 1337);

    run_txpool_benches(
        c,
        "clear",
        |controller_config| {
            let mut controller = BenchController::setup(&block_policy, controller_config.clone());

            Pool::create_proposal(
                &mut controller.pool,
                controller.block_policy.get_last_commit()
                    + SeqNum(controller.pending_blocks.len() as u64),
                controller.proposal_tx_limit,
                &MOCK_BENEFICIARY,
                MOCK_TIMESTAMP,
                &make_test_round_signature(),
                controller.block_policy,
                controller.pending_blocks.iter().collect_vec(),
                &controller.state_backend,
            )
            .unwrap();

            controller.pool
        },
        |pool| {
            Pool::clear(pool);
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
