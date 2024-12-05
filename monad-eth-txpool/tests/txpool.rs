use std::collections::{BTreeMap, VecDeque};

use alloy_primitives::{hex, B256};
use alloy_rlp::Decodable;
use bytes::Bytes;
use itertools::Itertools;
use monad_consensus_types::{block::BlockPolicy, txpool::TxPool};
use monad_crypto::NopSignature;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::{generate_block_with_txs, make_tx};
use monad_eth_tx::EthSignedTransaction;
use monad_eth_txpool::EthTxPool;
use monad_eth_types::{Balance, EthAddress};
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_testutil::signing::MockSignatures;
use monad_types::{Round, SeqNum, GENESIS_SEQ_NUM};
use tracing_test::traced_test;

const EXECUTION_DELAY: u64 = 4;
const BASE_FEE: u128 = 1000;
const GAS_LIMIT: u64 = 30000;

// pubkey starts with AAA
const S1: B256 = B256::new(hex!(
    "0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad"
));

// pubkey starts with BBB
const S2: B256 = B256::new(hex!(
    "009ac901cf45a2e92e7e7bdf167dc52e3a6232be3c56cc3b05622b247c2c716a"
));

// pubkey starts with CCC
const S3: B256 = B256::new(hex!(
    "0d756f31a3e98f1ae46475687cbfe3085ec74b3abdd712decff3e1e5e4c697a2"
));

// pubkey starts with DDD
const S4: B256 = B256::new(hex!(
    "871683e86bef90f2e790e60e4245916c731f540eec4a998697c2cbab4e156868"
));

// pubkey starts with EEE
const S5: B256 = B256::new(hex!(
    "9c82e5ab4dda8da5391393c5eb7cb8b79ca8e03b3028be9ba1e31f2480e17dc8"
));

type SignatureType = NopSignature;
type SignatureCollectionType = MockSignatures<SignatureType>;
type StateBackendType = InMemoryState;

type Pool = dyn TxPool<SignatureCollectionType, EthBlockPolicy, StateBackendType>;

fn make_test_block_policy() -> EthBlockPolicy {
    EthBlockPolicy::new(GENESIS_SEQ_NUM, EXECUTION_DELAY, 1337)
}

enum TxPoolTestEvent<'a> {
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
    Block(Box<dyn FnOnce(&mut EthTxPool<SignatureCollectionType, StateBackendType>)>),
}

fn run_custom_eth_txpool_test<const N: usize>(
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

    pool.update_committed_block(&generate_block_with_txs(
        Round(0),
        SeqNum(0),
        Vec::default(),
    ));

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
                        .expect("pool size change does not overflow"),
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
                    SeqNum(current_seq_num),
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
                    "create_proposal decoded txns do not match expected txs!\n{:#?}",
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
            }
            TxPoolTestEvent::Block(f) => f(&mut pool),
        }
    }
}

fn run_eth_txpool_test<const N: usize>(events: [TxPoolTestEvent<'_>; N]) {
    run_custom_eth_txpool_test(make_test_block_policy(), None, events);
}

#[test]
#[traced_test]
fn test_create_proposal_with_insufficient_tx_limit() {
    let tx = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 0,
            gas_limit: GAS_LIMIT,
            expected_txs: vec![],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::Block(Box::new(|pool| {
            assert_eq!(pool.num_txs(), 1);
        })),
    ]);
}

#[test]
#[traced_test]
fn test_create_proposal_with_insufficient_gas_limit() {
    let tx = make_tx(S1, BASE_FEE, GAS_LIMIT + 1, 0, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 1,
            gas_limit: GAS_LIMIT,
            expected_txs: vec![],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::Block(Box::new(|pool| {
            assert_eq!(pool.num_txs(), 1);
        })),
    ]);
}

#[test]
#[traced_test]
fn test_create_partial_proposal_with_insufficient_gas_limit() {
    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_tx(S1, BASE_FEE, GAS_LIMIT, 2, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true), (&tx3, true)],
            expected_pool_size_change: 3,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 3,
            gas_limit: GAS_LIMIT * 2,
            expected_txs: vec![&tx1, &tx2],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::Block(Box::new(|pool| {
            assert_eq!(pool.num_txs(), 3);
        })),
    ]);
}

#[test]
#[traced_test]
fn test_basic_price_priority() {
    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S2, 2 * BASE_FEE, 2 * GAS_LIMIT, 0, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true)],
            expected_pool_size_change: 2,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 2,
            gas_limit: GAS_LIMIT * 3,
            expected_txs: vec![&tx2, &tx1],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::Block(Box::new(|pool| {
            assert_eq!(pool.num_txs(), 2);
        })),
    ]);
}

#[test]
#[traced_test]
fn test_resubmit_with_better_price() {
    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S1, 2 * BASE_FEE, 2 * GAS_LIMIT, 0, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 2,
            gas_limit: GAS_LIMIT * 3,
            expected_txs: vec![&tx2],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn nontrivial_example() {
    let tx1 = make_tx(S1, 10 * BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S1, 5 * BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_tx(S1, 3 * BASE_FEE, GAS_LIMIT, 2, 10);
    let tx4 = make_tx(S2, 5 * BASE_FEE, GAS_LIMIT, 0, 10);
    let tx5 = make_tx(S2, 3 * BASE_FEE, GAS_LIMIT, 1, 10);
    let tx6 = make_tx(S2, BASE_FEE, GAS_LIMIT, 2, 10);
    let tx7 = make_tx(S3, 8 * BASE_FEE, GAS_LIMIT, 0, 10);
    let tx8 = make_tx(S3, 9 * BASE_FEE, GAS_LIMIT, 1, 10);
    let tx9 = make_tx(S3, 10 * BASE_FEE, GAS_LIMIT, 2, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![&tx1, &tx2, &tx3, &tx4, &tx5, &tx6, &tx7, &tx8, &tx9]
                .into_iter()
                .map(|tx| (tx, true))
                .collect_vec(),
            expected_pool_size_change: 9,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 1024 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx7, &tx8, &tx9, &tx2, &tx4, &tx5, &tx3, &tx6],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn another_non_trivial_example() {
    let tx1 = make_tx(S1, 10 * BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S1, 5 * BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_tx(S2, 5 * BASE_FEE, GAS_LIMIT, 0, 10);
    let tx4 = make_tx(S2, 3 * BASE_FEE, GAS_LIMIT, 1, 10);
    let tx5 = make_tx(S3, 8 * BASE_FEE, GAS_LIMIT, 0, 10);
    let tx6 = make_tx(S3, 9 * BASE_FEE, GAS_LIMIT, 1, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![&tx1, &tx2, &tx3, &tx4, &tx5, &tx6]
                .into_iter()
                .map(|tx| (tx, true))
                .collect_vec(),
            expected_pool_size_change: 6,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 1024 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx5, &tx6, &tx2, &tx3, &tx4],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn attacker_tries_to_include_transaction_with_large_gas_limit_to_exit_proposal_creation_early() {
    let tx1 = make_tx(S1, 10 * BASE_FEE, 100 * GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S2, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx3 = make_tx(S2, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx4 = make_tx(S2, BASE_FEE, GAS_LIMIT, 2, 10);
    let tx5 = make_tx(S2, BASE_FEE, GAS_LIMIT, 3, 10);
    let tx6 = make_tx(S2, BASE_FEE, GAS_LIMIT, 4, 10);
    let tx7 = make_tx(S2, BASE_FEE, GAS_LIMIT, 5, 10);
    let tx8 = make_tx(S2, BASE_FEE, GAS_LIMIT, 6, 10);
    let tx9 = make_tx(S2, BASE_FEE, GAS_LIMIT, 7, 10);
    let tx10 = make_tx(S2, BASE_FEE, GAS_LIMIT, 8, 10);
    let tx11 = make_tx(S2, BASE_FEE, GAS_LIMIT, 9, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![
                &tx1, &tx2, &tx3, &tx4, &tx5, &tx6, &tx7, &tx8, &tx9, &tx10, &tx11,
            ]
            .into_iter()
            .map(|tx| (tx, true))
            .collect_vec(),
            expected_pool_size_change: 11,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx2, &tx3, &tx4, &tx5, &tx6, &tx7, &tx8, &tx9, &tx10, &tx11],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn suboptimal_block() {
    let tx1 = make_tx(S2, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S2, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_tx(S2, BASE_FEE, GAS_LIMIT, 2, 10);
    let tx4 = make_tx(S2, BASE_FEE, GAS_LIMIT, 3, 10);
    let tx5 = make_tx(S2, BASE_FEE, GAS_LIMIT, 4, 10);
    let tx6 = make_tx(S2, BASE_FEE, GAS_LIMIT, 5, 10);
    let tx7 = make_tx(S2, BASE_FEE, GAS_LIMIT, 6, 10);
    let tx8 = make_tx(S2, BASE_FEE, GAS_LIMIT, 7, 10);
    let tx9 = make_tx(S2, BASE_FEE, GAS_LIMIT, 8, 10);
    let tx10 = make_tx(S2, BASE_FEE, GAS_LIMIT, 9, 10);
    let tx11 = make_tx(S1, 2 * BASE_FEE, 10 * GAS_LIMIT, 0, 10);

    for reverse in [false, true] {
        let mut txs = vec![
            &tx1, &tx2, &tx3, &tx4, &tx5, &tx6, &tx7, &tx8, &tx9, &tx10, &tx11,
        ];

        if reverse {
            txs.reverse();
        }

        run_eth_txpool_test([
            TxPoolTestEvent::InsertTxs {
                txs: txs.into_iter().map(|tx| (tx, true)).collect_vec(),
                expected_pool_size_change: 11,
            },
            TxPoolTestEvent::CreateProposal {
                tx_limit: 11,
                gas_limit: 10 * GAS_LIMIT,
                expected_txs: vec![&tx11],
                add_to_blocktree: true,
            },
        ]);
    }
}

#[test]
#[traced_test]
fn zero_gas_limit() {
    let tx1 = make_tx(S1, BASE_FEE, 0, 0, 10);

    run_eth_txpool_test([TxPoolTestEvent::InsertTxs {
        txs: vec![(&tx1, false)],
        expected_pool_size_change: 0,
    }]);
}

#[test]
#[traced_test]
fn nondeterminism() {
    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_tx(S2, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx4 = make_tx(S2, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx5 = make_tx(S3, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx6 = make_tx(S3, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx7 = make_tx(S4, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx8 = make_tx(S4, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx9 = make_tx(S5, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx10 = make_tx(S5, BASE_FEE, GAS_LIMIT, 1, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![&tx1, &tx2, &tx3, &tx4, &tx5, &tx6, &tx7, &tx8, &tx9, &tx10]
                .into_iter()
                .map(|tx| (tx, true))
                .collect_vec(),
            expected_pool_size_change: 10,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 10,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx9, &tx10, &tx7, &tx8, &tx5, &tx6, &tx3, &tx4, &tx1, &tx2],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_zero_nonce_included_in_block() {
    // The first transaction from an account with 0 nonce should be including in the block

    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_nonce_gap() {
    // A transaction with nonce 1 should not be included in the block if a tx with nonce 0 is missing

    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_intermediary_nonce_gap() {
    // A transaction with nonce 3 should not be included in the block if a tx with nonce 2 is missing

    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_tx(S1, BASE_FEE, GAS_LIMIT, 3, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true), (&tx3, true)],
            expected_pool_size_change: 3,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx2],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_nonce_exists_in_committed_block() {
    // A transaction with nonce 0 should not be included in the block if the latest nonce of the account is 0

    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);

    let nonces = [(
        EthAddress(tx1.recover_signer().expect("signer is recoverable")),
        1,
    )]
    .into_iter()
    .collect();

    run_custom_eth_txpool_test(
        make_test_block_policy(),
        Some(nonces),
        [
            TxPoolTestEvent::InsertTxs {
                txs: vec![(&tx1, true), (&tx2, true)],
                expected_pool_size_change: 1,
            },
            TxPoolTestEvent::CreateProposal {
                tx_limit: 128,
                gas_limit: 10 * GAS_LIMIT,
                expected_txs: vec![&tx2],
                add_to_blocktree: true,
            },
        ],
    );
}

#[test]
#[traced_test]
fn test_nonce_exists_in_pending_block() {
    // A transaction with nonce 0 should not be included in the block if the latest nonce of the account is 0

    // generate two transactions, both with nonce = 0
    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 1000);

    let tx3 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 1,
            gas_limit: GAS_LIMIT,
            expected_txs: vec![&tx1],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::Clear,
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx2, true), (&tx3, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx3],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_combine_nonces_of_blocks() {
    // TxPool should combine the nonces of commited block and pending blocks to check nonce

    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_tx(S1, BASE_FEE, GAS_LIMIT, 2, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::CommitPendingBlocks {
            num_blocks: 1,
            expected_committed_seq_num: 1,
        },
        TxPoolTestEvent::Clear,
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx2, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx2],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::Clear,
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx3, true)],
            expected_pool_size_change: 1,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx3],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_nonce_gap_maintained_across_proposals() {
    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_tx(S1, BASE_FEE, GAS_LIMIT, 2, 10);
    let tx4 = make_tx(S1, BASE_FEE, GAS_LIMIT, 3, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true), (&tx4, true)],
            expected_pool_size_change: 3,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx2],
            add_to_blocktree: false,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx2],
            add_to_blocktree: false,
        },
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx3, true)],
            expected_pool_size_change: 1,
        },
        // Even though proposals have been created, the txpool should still contain tx4!
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx2, &tx3, &tx4],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_nonce_gap_maintained_across_commit() {
    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx3 = make_tx(S1, BASE_FEE, GAS_LIMIT, 2, 10);
    let tx4 = make_tx(S1, BASE_FEE, GAS_LIMIT, 3, 10);

    run_eth_txpool_test([
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx1, true), (&tx2, true), (&tx4, true)],
            expected_pool_size_change: 3,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx2],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::CommitPendingBlocks {
            num_blocks: 1,
            expected_committed_seq_num: 1,
        },
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx3, true)],
            expected_pool_size_change: 1,
        },
        // Even though block has been committed, the txpool should still contain tx4!
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx3, &tx4],
            add_to_blocktree: false,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx3, &tx4],
            add_to_blocktree: true,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![],
            add_to_blocktree: true,
        },
    ]);
}

#[test]
#[traced_test]
fn test_invalid_chain_id() {
    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);

    run_custom_eth_txpool_test(
        EthBlockPolicy::new(GENESIS_SEQ_NUM, 0, 1),
        None,
        [
            TxPoolTestEvent::InsertTxs {
                txs: vec![(&tx1, false)],
                expected_pool_size_change: 0,
            },
            TxPoolTestEvent::CreateProposal {
                tx_limit: 128,
                gas_limit: 10 * GAS_LIMIT,
                expected_txs: vec![],
                add_to_blocktree: true,
            },
        ],
    );
}
