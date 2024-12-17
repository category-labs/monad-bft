use alloy_primitives::{hex, B256};
use itertools::Itertools;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::make_tx;
use monad_eth_txpool::test_utils::{
    make_test_block_policy, run_custom_eth_txpool_test, run_eth_txpool_test, TxPoolTestEvent,
};
use monad_eth_types::EthAddress;
use monad_types::GENESIS_SEQ_NUM;
use tracing_test::traced_test;

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
            assert!(pool.is_empty());
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
            assert!(pool.is_empty());
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
            assert!(pool.is_empty());
        })),
    ]);
}

#[test]
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
            assert!(pool.is_empty());
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
            tx_limit: 11,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx1, &tx2, &tx3, &tx4, &tx5, &tx6, &tx7, &tx8, &tx9, &tx10],
            add_to_blocktree: true,
        },
    ]);
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
fn test_nonce_gap() {
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
                expected_pool_size_change: 2,
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
        TxPoolTestEvent::InsertTxs {
            txs: vec![(&tx2, true), (&tx3, true)],
            expected_pool_size_change: 2,
        },
        TxPoolTestEvent::CreateProposal {
            tx_limit: 128,
            gas_limit: 10 * GAS_LIMIT,
            expected_txs: vec![&tx3],
            add_to_blocktree: true,
        },
    ]);
}

#[traced_test]
#[test]
fn test_combine_nonces_of_blocks() {
    // TxPool should combine the nonces of commited block and pending blocks to check nonce

    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    let tx2 = make_tx(S1, BASE_FEE, GAS_LIMIT, 2, 10);
    let tx3 = make_tx(S1, BASE_FEE, GAS_LIMIT, 3, 10);

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
        ],
    );
}

#[traced_test]
#[test]
fn test_subsequent_proposals() {
    let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 0, 10);
    let tx2 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);
    // let tx3 = make_tx(S1, BASE_FEE, GAS_LIMIT, 2, 10);
    let tx4 = make_tx(S1, BASE_FEE, GAS_LIMIT, 3, 10);

    // TODO(andr-dev): Txs should not be evicted until clear is called or a block is committed
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
        // TxPoolTestEvent::CreateProposal {
        //     tx_limit: 128,
        //     gas_limit: 10 * GAS_LIMIT,
        //     expected_txs: vec![&tx1, &tx2],
        //     add_to_blocktree: false,
        // },
        // TxPoolTestEvent::InsertTxs {
        //     txs: vec![(&tx3, true)],
        //     expected_pool_size_change: 1,
        // },
        // TxPoolTestEvent::CreateProposal {
        //     tx_limit: 128,
        //     gas_limit: 10 * GAS_LIMIT,
        //     expected_txs: vec![&tx1, &tx2, &tx3, &tx4],
        //     add_to_blocktree: true,
        // },
        // TxPoolTestEvent::CreateProposal {
        //     tx_limit: 128,
        //     gas_limit: 10 * GAS_LIMIT,
        //     expected_txs: vec![],
        //     add_to_blocktree: true,
        // },
    ]);
}

#[test]
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
