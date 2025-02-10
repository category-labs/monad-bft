use criterion::{criterion_group, criterion_main, Criterion};
use monad_consensus_types::{block::GENESIS_TIMESTAMP, payload::RoundSignature};
use monad_crypto::{certificate_signature::CertificateKeyPair, NopKeyPair};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::EthTxPoolEventTracker;
use monad_types::{Round, SeqNum, GENESIS_SEQ_NUM};

use self::common::{run_txpool_benches, BenchController, EXECUTION_DELAY};

mod common;

fn criterion_benchmark(c: &mut Criterion) {
    // TODO: change this to something more meaningful, i.e. what's is the block
    // policy state we want to benchmark
    let block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, EXECUTION_DELAY, 1337);

    let mock_keypair = NopKeyPair::from_bytes(&mut [5_u8; 32]).unwrap();
    run_txpool_benches(
        c,
        "create_proposal",
        |controller_config| BenchController::setup(&block_policy, controller_config.clone()),
        |BenchController {
             state_backend,
             block_policy,
             pool,
             pending_blocks,
             metrics,
             snapshot_manager,
             proposal_tx_limit,
             proposal_gas_limit,
             proposal_byte_limit,
         }| {
            pool.create_proposal(
                &mut EthTxPoolEventTracker::new(metrics, snapshot_manager, &mut Vec::default()),
                block_policy.get_last_commit() + SeqNum(pending_blocks.len() as u64),
                *proposal_tx_limit,
                *proposal_gas_limit,
                *proposal_byte_limit,
                [0_u8; 20],
                GENESIS_TIMESTAMP
                    + block_policy.get_last_commit().0 as u128
                    + pending_blocks.len() as u128,
                RoundSignature::new(Round(0), &mock_keypair),
                pending_blocks.to_owned(),
                block_policy,
                state_backend,
            )
            .unwrap();
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
