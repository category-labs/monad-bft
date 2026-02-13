// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::BTreeMap;

use criterion::{criterion_group, criterion_main, Criterion};
use monad_chain_config::MockChainConfig;
use monad_consensus_types::{block::GENESIS_TIMESTAMP, payload::RoundSignature};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, PubKey},
    NopKeyPair, NopPubKey,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::EthTxPoolEventTracker;
use monad_types::{Epoch, NodeId, Round, SeqNum, GENESIS_SEQ_NUM};

use self::common::{run_txpool_benches, BenchController, EXECUTION_DELAY};

mod common;

const BASE_FEE: u64 = 100_000_000_000;

fn criterion_benchmark(c: &mut Criterion) {
    // TODO: change this to something more meaningful, i.e. what's is the block
    // policy state we want to benchmark
    let block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, EXECUTION_DELAY);

    let mock_keypair = NopKeyPair::from_bytes(&mut [5_u8; 32]).unwrap();
    run_txpool_benches(
        c,
        "create_proposal",
        |controller_config| BenchController::setup(&block_policy, controller_config.clone()),
        |BenchController {
             chain_config: _,
             state_backend,
             block_policy,
             pool,
             pending_blocks,
             metrics,
             proposal_tx_limit,
             proposal_gas_limit,
             proposal_byte_limit,
         }| {
            pool.fetch_proposal(
                &mut EthTxPoolEventTracker::new(metrics, &mut BTreeMap::default()),
                Epoch(1),
                Round(1),
                block_policy.get_last_commit() + SeqNum(pending_blocks.len() as u64),
                BASE_FEE,
                *proposal_tx_limit,
                *proposal_gas_limit,
                *proposal_byte_limit,
                [0_u8; 20],
                GENESIS_TIMESTAMP
                    + block_policy.get_last_commit().0 as u128
                    + pending_blocks.len() as u128,
                NodeId::new(NopPubKey::from_bytes(&[0_u8; 32]).unwrap()),
                RoundSignature::new(Round(0), &mock_keypair),
                pending_blocks.to_owned(),
                block_policy,
                state_backend,
                &MockChainConfig::DEFAULT,
            )
            .unwrap();
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
