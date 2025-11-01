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

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use itertools::Itertools;
use monad_chain_config::{revision::MockChainRevision, MockChainConfig};
use monad_crypto::NopSignature;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::{generate_block_with_txs, make_legacy_tx, recover_tx, S1};
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker, EthTxPoolMetrics};
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_testutil::signing::MockSignatures;
use monad_types::{Balance, Round, SeqNum, GENESIS_SEQ_NUM};

type SignatureType = NopSignature;
type SignatureCollectionType = MockSignatures<SignatureType>;

const FORWARD_MIN_SEQ_NUM_DIFF: u64 = 3;
const FORWARD_MAX_RETRIES: usize = 2;
const BASE_FEE: u64 = 100_000_000_000;

fn harness_with_txs(
    txs: Vec<Recovered<TxEnvelope>>,
    insert_tx_owned: bool,
    f: impl FnOnce(
        EthTxPool<
            SignatureType,
            SignatureCollectionType,
            InMemoryState<SignatureType, SignatureCollectionType>,
            MockChainConfig,
            MockChainRevision,
        >,
        &mut EthTxPoolEventTracker,
    ),
) {
    let eth_block_policy = EthBlockPolicy::<
        SignatureType,
        SignatureCollectionType,
        MockChainConfig,
        MockChainRevision,
    >::new(GENESIS_SEQ_NUM, 4);
    let state_backend = InMemoryStateInner::new(
        Balance::MAX,
        SeqNum(4),
        InMemoryBlockState::genesis(BTreeMap::from_iter(
            txs.iter()
                .map(|tx| (tx.signer(), 0u64))
                .unique_by(|(signer, _)| *signer),
        )),
    );
    let mut pool = EthTxPool::default_testing();

    let metrics = EthTxPoolMetrics::default();
    let mut ipc_events = BTreeMap::default();
    let mut event_tracker = EthTxPoolEventTracker::new(&metrics, &mut ipc_events);

    assert!(pool
        .get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
        .is_none());

    pool.update_committed_block(
        &mut event_tracker,
        &MockChainConfig::DEFAULT,
        generate_block_with_txs(
            Round(0),
            SeqNum(0),
            BASE_FEE,
            &MockChainConfig::DEFAULT,
            Vec::default(),
        ),
    );

    assert_eq!(
        pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
            .unwrap()
            .count(),
        0
    );

    let metrics = EthTxPoolMetrics::default();
    let mut ipc_events = BTreeMap::default();
    let mut event_tracker = EthTxPoolEventTracker::new(&metrics, &mut ipc_events);

    let num_txs = txs.len();

    pool.insert_txs(
        &mut event_tracker,
        &eth_block_policy,
        &state_backend,
        &MockChainConfig::DEFAULT,
        txs,
        insert_tx_owned,
        |_| {},
    );

    assert_eq!(pool.num_txs(), num_txs);
    assert_eq!(
        pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
            .unwrap()
            .count(),
        0
    );

    f(pool, &mut event_tracker)
}

fn harness(
    insert_tx_owned: bool,
    f: impl FnOnce(
        EthTxPool<
            SignatureType,
            SignatureCollectionType,
            InMemoryState<SignatureType, SignatureCollectionType>,
            MockChainConfig,
            MockChainRevision,
        >,
        &mut EthTxPoolEventTracker,
    ),
) {
    let tx = recover_tx(make_legacy_tx(S1, BASE_FEE.into(), 100_000, 0, 0));

    harness_with_txs(vec![tx], insert_tx_owned, f)
}

#[test]
fn test_simple() {
    harness(true, |mut pool, event_tracker| {
        for (idx, forwardable) in [0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0].into_iter().enumerate() {
            pool.update_committed_block(
                event_tracker,
                &MockChainConfig::DEFAULT,
                generate_block_with_txs(
                    Round(idx as u64 + 1),
                    SeqNum(idx as u64 + 1),
                    BASE_FEE,
                    &MockChainConfig::DEFAULT,
                    Vec::default(),
                ),
            );

            assert_eq!(
                pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                    .unwrap()
                    .count(),
                forwardable
            );

            // Subsequent calls do not produce the tx
            //  -> Validates that tx is not reproduced in same block
            for _ in 0..128 {
                assert_eq!(
                    pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                        .unwrap()
                        .count(),
                    0
                );
            }
        }
    });
}

#[test]
fn test_forwarded() {
    harness(false, |mut pool, event_tracker| {
        for idx in 0..128 {
            pool.update_committed_block(
                event_tracker,
                &MockChainConfig::DEFAULT,
                generate_block_with_txs(
                    Round(idx as u64 + 1),
                    SeqNum(idx as u64 + 1),
                    BASE_FEE,
                    &MockChainConfig::DEFAULT,
                    Vec::default(),
                ),
            );

            for _ in 0..128 {
                assert_eq!(
                    pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                        .unwrap()
                        .count(),
                    // Forwarded txs are never forwarded
                    0
                );
            }
        }
    });
}

#[test]
fn test_multiple_sequential_commits() {
    harness(true, |mut pool, event_tracker| {
        let mut round_seqnum = 1;

        for forwardable in [1, 1, 0, 0, 0, 0, 0, 0] {
            for _ in 0..128 {
                pool.update_committed_block(
                    event_tracker,
                    &MockChainConfig::DEFAULT,
                    generate_block_with_txs(
                        Round(round_seqnum),
                        SeqNum(round_seqnum),
                        BASE_FEE,
                        &MockChainConfig::DEFAULT,
                        Vec::default(),
                    ),
                );
                round_seqnum += 1;
            }

            assert_eq!(
                pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                    .unwrap()
                    .count(),
                forwardable
            );

            // Subsequent calls do not produce the tx
            //  -> Validates that forwarding is non-bursty
            for _ in 0..128 {
                assert_eq!(
                    pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                        .unwrap()
                        .count(),
                    0
                );
            }
        }
    });
}

#[test]
fn test_base_fee() {
    harness(true, |mut pool, event_tracker| {
        let mut round = 1;

        for _ in 0..FORWARD_MAX_RETRIES {
            for _ in 0..128 {
                pool.update_committed_block(
                    event_tracker,
                    &MockChainConfig::DEFAULT,
                    generate_block_with_txs(
                        Round(round),
                        SeqNum(round),
                        BASE_FEE + 1,
                        &MockChainConfig::DEFAULT,
                        Vec::default(),
                    ),
                );
                round += 1;

                assert_eq!(
                    pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                        .unwrap()
                        .count(),
                    0
                );
            }

            pool.update_committed_block(
                event_tracker,
                &MockChainConfig::DEFAULT,
                generate_block_with_txs(
                    Round(round),
                    SeqNum(round),
                    BASE_FEE,
                    &MockChainConfig::DEFAULT,
                    Vec::default(),
                ),
            );
            round += 1;

            assert_eq!(
                pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                    .unwrap()
                    .count(),
                1
            );
        }

        for _ in 0..128 {
            pool.update_committed_block(
                event_tracker,
                &MockChainConfig::DEFAULT,
                generate_block_with_txs(
                    Round(round),
                    SeqNum(round),
                    BASE_FEE,
                    &MockChainConfig::DEFAULT,
                    Vec::default(),
                ),
            );
            round += 1;

            // Subsequent calls do not produce the tx
            //  -> Validates that forwarding is non-bursty
            assert_eq!(
                pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                    .unwrap()
                    .count(),
                0
            );
        }
    });
}

#[test]
fn test_short_nonce_gap() {
    harness_with_txs(
        vec![recover_tx(make_legacy_tx(
            S1,
            BASE_FEE.into(),
            100_000,
            1,
            0,
        ))],
        true,
        |mut pool, event_tracker| {
            pool.update_committed_block(
                event_tracker,
                &MockChainConfig::DEFAULT,
                generate_block_with_txs(
                    Round(1),
                    SeqNum(1),
                    BASE_FEE,
                    &MockChainConfig::DEFAULT,
                    vec![recover_tx(make_legacy_tx(
                        S1,
                        BASE_FEE.into(),
                        100_000,
                        0,
                        0,
                    ))],
                ),
            );

            // Because nonce gap closed within a block, tx forwarded two more times after another two blocks
            for (idx, forwardable) in [0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                .into_iter()
                .enumerate()
            {
                assert_eq!(
                    pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                        .unwrap()
                        .count(),
                    forwardable,
                );

                pool.update_committed_block(
                    event_tracker,
                    &MockChainConfig::DEFAULT,
                    generate_block_with_txs(
                        Round(idx as u64 + 2),
                        SeqNum(idx as u64 + 2),
                        BASE_FEE,
                        &MockChainConfig::DEFAULT,
                        Vec::default(),
                    ),
                );
            }
        },
    );
}

#[test]
fn test_long_nonce_gap() {
    harness_with_txs(
        vec![
            recover_tx(make_legacy_tx(S1, BASE_FEE.into(), 100_000, 1, 0)),
            recover_tx(make_legacy_tx(S1, BASE_FEE.into(), 100_000, 2, 0)),
        ],
        true,
        |mut pool, event_tracker| {
            // Txs are immediately forwarded but not retried because of nonce gap
            for idx in 0..128 {
                pool.update_committed_block(
                    event_tracker,
                    &MockChainConfig::DEFAULT,
                    generate_block_with_txs(
                        Round(idx as u64 + 1),
                        SeqNum(idx as u64 + 1),
                        BASE_FEE,
                        &MockChainConfig::DEFAULT,
                        Vec::default(),
                    ),
                );

                assert_eq!(
                    pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                        .unwrap()
                        .count(),
                    0
                );
            }

            pool.update_committed_block(
                event_tracker,
                &MockChainConfig::DEFAULT,
                generate_block_with_txs(
                    Round(129),
                    SeqNum(129),
                    BASE_FEE,
                    &MockChainConfig::DEFAULT,
                    vec![recover_tx(make_legacy_tx(
                        S1,
                        BASE_FEE.into(),
                        100_000,
                        0,
                        0,
                    ))],
                ),
            );

            // Immediately after nonce gap is closed, txs forwarded two more times
            for (idx, forwardable) in [2, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                .into_iter()
                .enumerate()
            {
                pool.update_committed_block(
                    event_tracker,
                    &MockChainConfig::DEFAULT,
                    generate_block_with_txs(
                        Round(idx as u64 + 130),
                        SeqNum(idx as u64 + 130),
                        BASE_FEE,
                        &MockChainConfig::DEFAULT,
                        Vec::default(),
                    ),
                );

                assert_eq!(
                    pool.get_forwardable_txs::<FORWARD_MIN_SEQ_NUM_DIFF, FORWARD_MAX_RETRIES>()
                        .unwrap()
                        .count(),
                    forwardable,
                );
            }
        },
    );
}
