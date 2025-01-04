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
            Pool::create_proposal(
                pool,
                block_policy.get_last_commit() + SeqNum(pending_blocks.len() as u64),
                *proposal_tx_limit,
                &MOCK_BENEFICIARY,
                MOCK_TIMESTAMP,
                &make_test_round_signature(),
                block_policy,
                pending_blocks.iter().collect_vec(),
                state_backend,
            )
            .unwrap();
        },
    );

    run_txpool_benches(
        c,
        "create_proposal_after_promoting",
        |controller_config| {
            let mut controller = BenchController::setup(&block_policy, controller_config.clone());

            controller
                .pool
                .promote_pending(
                    controller.block_policy,
                    &controller.state_backend,
                    usize::MAX,
                )
                .expect("promote pending succeeds");

            controller
        },
        |BenchController {
             state_backend,
             block_policy,
             pool,
             pending_blocks,
             proposal_tx_limit,
             gas_limit,
         }| {
            Pool::create_proposal(
                pool,
                block_policy.get_last_commit() + SeqNum(pending_blocks.len() as u64),
                *proposal_tx_limit,
                &MOCK_BENEFICIARY,
                MOCK_TIMESTAMP,
                &make_test_round_signature(),
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
