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

use alloy_primitives::{hex, B256};
use monad_chain_config::MockChainConfig;
use monad_consensus_types::{block::GENESIS_TIMESTAMP, payload::RoundSignature};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, PubKey},
    NopKeyPair, NopPubKey, NopSignature,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::{
    generate_block_with_txs, make_legacy_tx, recover_tx, secret_to_eth_address,
};
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker, EthTxPoolMetrics};
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner, StateBackend};
use monad_testutil::signing::MockSignatures;
use monad_tfm::base_fee::MIN_BASE_FEE;
use monad_types::{Balance, Epoch, NodeId, Round, SeqNum, GENESIS_SEQ_NUM};

type SignatureType = NopSignature;
type SignatureCollectionType = MockSignatures<SignatureType>;
type StateBackendType = InMemoryState<SignatureType, SignatureCollectionType>;

// pubkey starts with AAA
const S1: B256 = B256::new(hex!(
    "0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad"
));

// pubkey starts with BBB
const S2: B256 = B256::new(hex!(
    "009ac901cf45a2e92e7e7bdf167dc52e3a6232be3c56cc3b05622b247c2c716a"
));

#[test]
fn txpool_create_proposal_lookups_bound_by_tx_limit() {
    for (tx_limit, expected_lookups) in [(usize::MAX, 7), (1, 6)] {
        let mut pool = EthTxPool::default_testing();

        let metrics = EthTxPoolMetrics::default();
        let mut ipc_events = BTreeMap::default();
        let mut event_tracker = EthTxPoolEventTracker::new(&metrics, &mut ipc_events);

        pool.update_committed_block(
            &mut event_tracker,
            &MockChainConfig::DEFAULT,
            generate_block_with_txs(
                Round(0),
                SeqNum(0),
                MIN_BASE_FEE,
                &MockChainConfig::DEFAULT,
                Vec::default(),
            ),
        );

        let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, u64::MAX);

        let state_backend: StateBackendType = InMemoryStateInner::new(
            Balance::MAX,
            SeqNum::MAX,
            InMemoryBlockState::genesis(BTreeMap::from_iter([
                (secret_to_eth_address(S1), 0),
                (secret_to_eth_address(S2), 0),
            ])),
        );

        pool.insert_txs(
            &mut event_tracker,
            &eth_block_policy,
            &state_backend,
            &MockChainConfig::DEFAULT,
            vec![
                recover_tx(make_legacy_tx(S1, MIN_BASE_FEE.into(), 100_000, 0, 0)),
                recover_tx(make_legacy_tx(S2, MIN_BASE_FEE.into(), 100_000, 0, 0)),
            ],
            true,
            |_| {},
        );

        assert_eq!(pool.num_txs(), 2);

        let mock_keypair = NopKeyPair::from_bytes(&mut [5_u8; 32]).unwrap();

        let _ = pool
            .create_proposal(
                &mut event_tracker,
                Epoch(1),
                Round(1),
                SeqNum(1),
                MIN_BASE_FEE,
                tx_limit,
                1_000_000,
                1024 * 1024,
                [0_u8; 20],
                GENESIS_TIMESTAMP,
                NodeId::new(NopPubKey::from_bytes(&[0_u8; 32]).unwrap()),
                RoundSignature::new(Round(0), &mock_keypair),
                vec![],
                &eth_block_policy,
                &state_backend,
                &MockChainConfig::DEFAULT,
            )
            .unwrap();

        assert_eq!(state_backend.total_db_lookups(), expected_lookups);
    }
}
