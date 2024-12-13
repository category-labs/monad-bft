use std::collections::{BTreeMap, VecDeque};

use alloy_primitives::{hex, B256};
use alloy_rlp::Decodable;
use bytes::Bytes;
use itertools::Itertools;
use monad_consensus_types::{
    block::BlockPolicy, quorum_certificate::QuorumCertificate, txpool::TxPool,
};
use monad_crypto::NopSignature;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::generate_block_with_txs;
use monad_eth_tx::EthSignedTransaction;
use monad_eth_types::{Balance, EthAddress};
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_testutil::signing::MockSignatures;
use monad_types::{Round, SeqNum, GENESIS_SEQ_NUM};

use crate::EthTxPool;

const EXECUTION_DELAY: u64 = 4;

pub type SignatureType = NopSignature;
pub type StateBackendType = InMemoryState;
type QC = QuorumCertificate<MockSignatures<SignatureType>>;

pub type Pool = dyn TxPool<MockSignatures<SignatureType>, EthBlockPolicy, StateBackendType>;

pub fn make_test_block_policy() -> EthBlockPolicy {
    EthBlockPolicy::new(GENESIS_SEQ_NUM, EXECUTION_DELAY, 1337)
}

pub enum TxPoolTestEvent<'a> {
    InsertTxs {
        txs: Vec<(&'a EthSignedTransaction, bool)>,
        expected_pool_size_change: usize,
    },
    CreateProposal {
        tx_limit: usize,
        gas_limit: u64,
        expected_txs: Vec<&'a EthSignedTransaction>,
        add_to_blocktree: bool,
    },
    CommitPendingBlocks {
        num_blocks: usize,
        expected_committed_seq_num: u64,
    },
    Clear,
    Block(Box<dyn FnOnce(&mut EthTxPool)>),
}

pub fn run_custom_eth_txpool_test<const N: usize>(
    mut eth_block_policy: EthBlockPolicy,
    nonces_override: Option<BTreeMap<EthAddress, u64>>,
    events: [TxPoolTestEvent<'_>; N],
) {
    let state_backend = {
        let nonces = if let Some(nonces) = nonces_override {
            nonces
        } else {
            events
                .iter()
                .flat_map(|event| match event {
                    TxPoolTestEvent::InsertTxs {
                        txs,
                        expected_pool_size_change: _,
                    } => txs
                        .iter()
                        .map(|(tx, _)| tx.recover_signer().expect("signer is recoverable"))
                        .collect::<Vec<_>>(),
                    _ => vec![],
                })
                .map(|address| (EthAddress(address), 0))
                .collect()
        };

        InMemoryStateInner::new(Balance::MAX, SeqNum(4), InMemoryBlockState::genesis(nonces))
    };

    let mut pool = EthTxPool::default();
    let mut current_round = 1u64;
    let mut current_seq_num = 1u64;
    let mut pending_blocks = VecDeque::default();

    for event in events {
        match event {
            TxPoolTestEvent::InsertTxs {
                txs,
                expected_pool_size_change,
            } => {
                let pool_previous_num_txs = pool.num_txs();

                for (tx, inserted) in txs {
                    let inserted_txs = Pool::insert_tx(
                        &mut pool,
                        vec![Bytes::from(tx.envelope_encoded())],
                        &eth_block_policy,
                        &state_backend,
                    );

                    if inserted {
                        assert_eq!(inserted_txs, vec![Bytes::from(tx.envelope_encoded())]);
                    } else {
                        assert!(inserted_txs.is_empty());
                    }
                }

                assert_eq!(
                    pool.num_txs(),
                    pool_previous_num_txs
                        .checked_add(expected_pool_size_change)
                        .expect("pool size change does not overflow")
                );
            }
            TxPoolTestEvent::CreateProposal {
                tx_limit,
                gas_limit,
                expected_txs,
                add_to_blocktree,
            } => {
                let encoded_txns = Pool::create_proposal(
                    &mut pool,
                    SeqNum(0),
                    tx_limit,
                    gas_limit,
                    &eth_block_policy,
                    pending_blocks.iter().collect_vec(),
                    &state_backend,
                )
                .expect("create proposal succeeds");

                let decoded_txns =
                    Vec::<EthSignedTransaction>::decode(&mut encoded_txns.as_ref()).unwrap();

                let expected_txs = expected_txs.into_iter().cloned().collect_vec();

                assert_eq!(
                    decoded_txns,
                    expected_txs,
                    "create_proposal decodede txns do not match expected txs!\n{:#?}",
                    decoded_txns
                        .iter()
                        .zip_longest(expected_txs.iter())
                        .collect_vec()
                );

                if add_to_blocktree {
                    let block = generate_block_with_txs(
                        Round(current_round),
                        SeqNum(current_seq_num),
                        decoded_txns,
                    );

                    current_seq_num += 1;

                    pending_blocks.push_back(block);
                }

                current_round += 1;
            }
            TxPoolTestEvent::CommitPendingBlocks {
                num_blocks,
                expected_committed_seq_num,
            } => {
                for _ in 0..num_blocks {
                    let block = pending_blocks
                        .pop_front()
                        .expect("missing block in blocktree");

                    assert_eq!(
                        current_seq_num
                            .checked_sub(pending_blocks.len() as u64 + 2)
                            .expect("seq_num does not underflow"),
                        block.block.qc.get_seq_num().0
                    );

                    BlockPolicy::<MockSignatures<SignatureType>, StateBackendType>::update_committed_block(
                        &mut eth_block_policy,
                        &block,
                    );

                    TxPool::<
                        MockSignatures<SignatureType>,
                        EthBlockPolicy,
                        StateBackendType,
                    >::update_committed_block(&mut pool, &block);
                }

                assert_eq!(
                    expected_committed_seq_num,
                    eth_block_policy.get_last_commit().0
                );
            }
            TxPoolTestEvent::Clear => {
                TxPool::<MockSignatures<SignatureType>, EthBlockPolicy, StateBackendType>::clear(
                    &mut pool,
                );

                assert!(pool.is_empty());
                assert_eq!(pool.num_txs(), 0);
            }
            TxPoolTestEvent::Block(f) => f(&mut pool),
        }
    }
}

pub fn run_eth_txpool_test<const N: usize>(events: [TxPoolTestEvent<'_>; N]) {
    run_custom_eth_txpool_test(make_test_block_policy(), None, events);
}
