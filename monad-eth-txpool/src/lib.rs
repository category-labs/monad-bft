use std::collections::{BTreeMap, BinaryHeap};

use alloy_rlp::Decodable;
use bytes::Bytes;
use itertools::Itertools;
use monad_consensus_types::{
    payload::FullTransactionList,
    signature_collection::SignatureCollection,
    txpool::{TxPool, TxPoolInsertionError},
};
use monad_eth_block_policy::{
    compute_txn_carriage_cost, static_validate_transaction, EthBlockPolicy, EthValidatedBlock,
};
use monad_eth_tx::{EthFullTransactionList, EthTransaction};
use monad_eth_types::{Balance, EthAddress};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tracing::{info, trace, warn};

use crate::{
    pool::Pool,
    transaction::{VirtualTimestamp, WrappedTransaction},
    utils::effective_tip_per_gas,
};

mod pool;
mod transaction;
mod utils;

const MAX_TXPOOL_SIZE: usize = 1024 * 32;

#[derive(Clone, Debug, Default)]
pub struct EthTxPool {
    /// pool is transient, garbage collected after creating a proposal
    pool: Pool,
    garbage: Vec<Pool>,
}

impl EthTxPool {
    /// Removes nonces that cannot extend the current block tree branch, as the
    /// txpool is transient and garbage collected after proposal creation
    fn validate_nonces_and_carriage_fee<SCT>(
        &mut self,
        proposed_seq_num: SeqNum,
        block_policy: &EthBlockPolicy,
        state_backend: &impl StateBackend,
        extending_blocks: &Vec<&EthValidatedBlock<SCT>>,
    ) -> Result<(), StateBackendError>
    where
        SCT: SignatureCollection,
    {
        // TODO don't copy all the addresses
        let addresses = self.pool.iter_addresses().cloned().collect::<Vec<_>>();

        let account_base_nonces = block_policy.get_account_base_nonces(
            proposed_seq_num,
            state_backend,
            extending_blocks,
            addresses.iter(),
        )?;

        let account_base_reserve_balances = block_policy.compute_account_base_reserve_balances(
            proposed_seq_num,
            state_backend,
            Some(extending_blocks),
            addresses.iter(),
        )?;

        self.pool.validate_nonces_and_carriage_fee(
            proposed_seq_num,
            account_base_nonces,
            account_base_reserve_balances,
        );

        Ok(())
    }

    fn validate_and_insert_tx(
        &mut self,
        valid_chain_id: u64,
        eth_tx: EthTransaction,
        reserve_balance: &Balance,
    ) -> Result<(), TxPoolInsertionError> {
        // TODO(abenedito): Smarter tx eviction when pool is full
        if self.pool.num_txs() > MAX_TXPOOL_SIZE {
            return Err(TxPoolInsertionError::PoolFull);
        }

        let sender = EthAddress(eth_tx.signer());
        let txn_hash = eth_tx.hash();

        if static_validate_transaction(&eth_tx, valid_chain_id).is_err() {
            return Err(TxPoolInsertionError::NotWellFormed);
        }

        let ratio = (effective_tip_per_gas(&eth_tx) as f64) / (eth_tx.gas_limit() as f64);

        if ratio.is_nan() {
            return Err(TxPoolInsertionError::NotWellFormed);
        }

        // TODO nonce check

        let txn_carriage_cost = compute_txn_carriage_cost(&eth_tx);
        if &txn_carriage_cost > reserve_balance {
            trace!(
                "ReserveBalance insert_tx 2 \
                            do not add txn to the pool. insufficient balance: {:?} \
                            txn_carriage_cost: {:?} \
                            for address: {:?}",
                reserve_balance,
                txn_carriage_cost,
                sender
            );
            return Err(TxPoolInsertionError::InsufficientBalance);
        }

        // TODO: this doesn't account for txns already in the mempool,
        // an account can still send infinite transactions into the
        // mempool

        // TODO(rene): should any transaction validation occur here before inserting into mempool
        self.pool.add_tx(sender, eth_tx, ratio);
        trace!(
            "ReserveBalance insert_tx 1 \
                            reserve balance: {:?} \
                            txn carriage cost: {:?} \
                            for address: {:?}",
            reserve_balance,
            txn_carriage_cost,
            sender
        );
        trace!(txn_hash = ?txn_hash, ?sender, "txn inserted into txpool");
        Ok(())
    }

    fn validate_and_insert_txs<SCT>(
        &mut self,
        block_policy: &EthBlockPolicy,
        state_backend: &impl StateBackend,
        txs: Vec<EthTransaction>,
    ) -> Result<Vec<Result<(), TxPoolInsertionError>>, StateBackendError>
    where
        SCT: SignatureCollection,
    {
        let senders = txs.iter().map(|tx| EthAddress(tx.signer())).collect_vec();
        let block_seq_num = block_policy.get_last_commit() + SeqNum(1); // ?????
        let sender_reserve_balances = block_policy.compute_account_base_reserve_balances::<SCT>(
            block_seq_num,
            state_backend,
            None,
            senders.iter(),
        )?;

        // TODO: make this trace logging after done debugging issue
        info!(
            num_to_insert = txs.len(),
            num_txs = self.pool.num_txs(),
            "Mempool size before inserting txs",
        );
        let results = txs
            .into_iter()
            .zip(senders.iter())
            .map(|(eth_tx, sender)| {
                self.validate_and_insert_tx(
                    block_policy.get_chain_id(),
                    eth_tx,
                    sender_reserve_balances
                        .get(sender)
                        .expect("sender_reserve_balances should be populated"),
                )
            })
            .collect();
        // TODO: make this trace logging after done debugging issue
        info!(
            num_txs = self.pool.num_txs(),
            "Mempool size after inserting txs",
        );
        Ok(results)
    }
}

impl<SCT, SBT> TxPool<SCT, EthBlockPolicy, SBT> for EthTxPool
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    fn insert_tx(
        &mut self,
        txns: Vec<Bytes>,
        block_policy: &EthBlockPolicy,
        state_backend: &SBT,
    ) -> Vec<Bytes> {
        // TODO: unwrap can be removed when this is made generic over the actual
        // tx type rather than Bytes and decoding won't be necessary
        // TODO(rene): sender recovery is done inline here
        let (decoded_txs, raw_txs): (Vec<_>, Vec<_>) = txns
            .into_par_iter()
            .filter_map(|b| {
                EthTransaction::decode(&mut b.as_ref())
                    .ok()
                    .map(|valid_tx| (valid_tx, b))
            })
            .unzip();

        let Ok(insertion_results) =
            self.validate_and_insert_txs::<SCT>(block_policy, state_backend, decoded_txs)
        else {
            // can't insert, state backend is delayed
            return Vec::new();
        };

        insertion_results
            .into_iter()
            .zip(raw_txs)
            .filter_map(|(insertion_result, b)| match insertion_result {
                Ok(()) => Some(b),
                Err(_) => None,
            })
            .collect::<Vec<_>>()
    }

    fn create_proposal(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        proposal_gas_limit: u64,
        block_policy: &EthBlockPolicy,
        extending_blocks: Vec<&EthValidatedBlock<SCT>>,
        state_backend: &SBT,
    ) -> Result<FullTransactionList, StateBackendError> {
        if let Err(err) = self.validate_nonces_and_carriage_fee(
            proposed_seq_num,
            block_policy,
            state_backend,
            &extending_blocks,
        ) {
            trace!("ReserveBalance create_proposal returned error: {:?}", err);
            return Err(err);
        }

        let mut txs = Vec::new();
        let mut total_gas = 0;

        let mut transaction_iters: BTreeMap<
            EthAddress,
            sorted_vector_map::map::Iter<u64, (reth_primitives::TransactionSignedEcRecovered, f64)>,
        > = self
            .pool
            .iter()
            .map(|(address, group)| (*address, group.iter()))
            .collect::<BTreeMap<_, _>>();

        let mut virtual_time: VirtualTimestamp = 0;

        let mut max_heap = BinaryHeap::<WrappedTransaction>::new();

        // queue one eligible transaction for each account (they will be the ones with the lowest nonce)
        for (_, transaction_iter) in transaction_iters.iter_mut() {
            match transaction_iter.next() {
                None => {
                    unreachable!()
                }
                Some((_, (transaction, price_gas_limit_ratio))) => {
                    max_heap.push(WrappedTransaction {
                        inner: transaction,
                        insertion_time: virtual_time,
                        price_gas_limit_ratio: *price_gas_limit_ratio,
                    });
                    virtual_time += 1;
                }
            }
        }

        // loop invariant: `max_heap` contains one transaction per account (lowest nonce).
        // the root of the heap will be the best priced eligible transaction
        while let Some(WrappedTransaction { inner: best_tx, .. }) = max_heap.pop() {
            let address = EthAddress(best_tx.signer());

            if txs.len() == tx_limit {
                break;
            }

            // If this transaction will take us out of the block limit, we cannot include it.
            // This will create a nonce gap, so we no longer want to consider this account, but
            // we want to continue considering other accounts.
            if (total_gas + best_tx.gas_limit()) > proposal_gas_limit {
                transaction_iters.remove(&address);
                continue;
            }

            // We also want to make sure that the transaction has max_fee_per_gas larger than base fee
            // TODO(kai): currently block base fee is hardcoded to 1000 in monad-ledger
            // update this when base fee is included in consensus proposal
            if best_tx.max_fee_per_gas() < 1000 {
                transaction_iters.remove(&address);
                continue;
            }

            // maintain the loop invariant because we just removed one element from the heap
            match transaction_iters.get_mut(&address).unwrap().next() {
                None => {}
                Some((_, (transaction, price_gas_limit_ratio))) => {
                    max_heap.push(WrappedTransaction {
                        inner: transaction,
                        insertion_time: virtual_time,
                        price_gas_limit_ratio: *price_gas_limit_ratio,
                    });
                    virtual_time += 1;
                }
            }

            total_gas += best_tx.gas_limit();
            trace!(txn_hash = ?best_tx.hash(), "txn included in proposal");
            txs.push(best_tx.clone());
        }

        let proposal_num_tx = txs.len();
        let full_tx_list = EthFullTransactionList(txs).rlp_encode();

        info!(
            ?proposed_seq_num,
            ?proposal_num_tx,
            proposal_total_gas = total_gas,
            proposal_tx_bytes = full_tx_list.len(),
            mempool_size = self.pool.num_txs(),
            "created proposal"
        );

        if !self.garbage.is_empty() {
            warn!(
                garbage_len = self.garbage.len(),
                "we have received consecutive proposals without a mempool clear event in between"
            );
        }

        // let old_pool = std::mem::take(&mut self.pool);
        // self.garbage.push(old_pool);

        Ok(FullTransactionList::new(full_tx_list))
    }

    fn clear(&mut self) {
        self.garbage.clear();
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use alloy_primitives::{hex, B256};
    use alloy_rlp::Decodable;
    use bytes::Bytes;
    use itertools::Itertools;
    use monad_consensus_types::txpool::TxPool;
    use monad_crypto::NopSignature;
    use monad_eth_block_policy::EthValidatedBlock;
    use monad_eth_testutil::{generate_random_block_with_txns, make_tx};
    use monad_eth_tx::EthSignedTransaction;
    use monad_eth_types::{Balance, EthAddress};
    use monad_multi_sig::MultiSig;
    use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
    use monad_types::{SeqNum, GENESIS_SEQ_NUM};
    use tracing_test::traced_test;

    use crate::{EthBlockPolicy, EthTxPool};

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

    type Pool = dyn TxPool<MultiSig<NopSignature>, EthBlockPolicy, InMemoryState>;

    fn make_test_block_policy() -> EthBlockPolicy {
        EthBlockPolicy::new(GENESIS_SEQ_NUM, u128::MAX, EXECUTION_DELAY, 1337)
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
        },
        SetPendingBlocks {
            pending_blocks: Vec<EthValidatedBlock<MultiSig<NopSignature>>>,
        },
        Block(Box<dyn FnOnce(&mut EthTxPool)>),
    }

    fn run_custom_eth_txpool_test<const N: usize>(
        eth_block_policy: EthBlockPolicy,
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
        let mut pending_blocks = Vec::default();

        for event in events {
            match event {
                TxPoolTestEvent::InsertTxs {
                    txs,
                    expected_pool_size_change,
                } => {
                    let pool_previous_num_txs = pool.pool.num_txs();

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
                        pool.pool.num_txs(),
                        pool_previous_num_txs
                            .checked_add(expected_pool_size_change)
                            .expect("pool size change does not overflow")
                    );
                }
                TxPoolTestEvent::CreateProposal {
                    tx_limit,
                    gas_limit,
                    expected_txs,
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

                    assert_eq!(decoded_txns, expected_txs);
                }
                TxPoolTestEvent::SetPendingBlocks {
                    pending_blocks: new_pending_blocks,
                } => {
                    pending_blocks = new_pending_blocks;
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
            },
            TxPoolTestEvent::Block(Box::new(|pool| {
                assert!(pool.pool.is_empty());
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
            },
            TxPoolTestEvent::Block(Box::new(|pool| {
                assert!(pool.pool.is_empty());
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
            },
            TxPoolTestEvent::Block(Box::new(|pool| {
                assert!(pool.pool.is_empty());
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
            },
            TxPoolTestEvent::Block(Box::new(|pool| {
                assert!(pool.pool.is_empty());
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
        let tx6 = make_tx(S2, 1 * BASE_FEE, GAS_LIMIT, 2, 10);
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
            },
        ]);
    }

    #[test]
    #[traced_test]
    fn attacker_tries_to_include_transaction_with_large_gas_limit_to_exit_proposal_creation_early()
    {
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
            },
        ]);
    }

    #[test]
    #[traced_test]
    fn suboptimal_block() {
        let tx1 = make_tx(S1, 2 * BASE_FEE, 10 * GAS_LIMIT, 0, 10);
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
                tx_limit: 10,
                gas_limit: 10 * GAS_LIMIT,
                expected_txs: vec![&tx2, &tx3, &tx4, &tx5, &tx6, &tx7, &tx8, &tx9, &tx10, &tx11],
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
            },
        ]);
    }

    #[test]
    fn test_invalid_nonce_not_in_block() {
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
            TxPoolTestEvent::SetPendingBlocks {
                pending_blocks: vec![generate_random_block_with_txns(vec![tx1])],
            },
            TxPoolTestEvent::InsertTxs {
                txs: vec![(&tx2, true), (&tx3, true)],
                expected_pool_size_change: 2,
            },
            TxPoolTestEvent::CreateProposal {
                tx_limit: 128,
                gas_limit: 10 * GAS_LIMIT,
                expected_txs: vec![&tx3],
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
                    txs: vec![(&tx1, true), (&tx2, true), (&tx3, true)],
                    expected_pool_size_change: 3,
                },
                TxPoolTestEvent::SetPendingBlocks {
                    pending_blocks: vec![
                        generate_random_block_with_txns(vec![tx1.clone()]),
                        generate_random_block_with_txns(vec![tx2.clone()]),
                    ],
                },
                TxPoolTestEvent::CreateProposal {
                    tx_limit: 128,
                    gas_limit: 10 * GAS_LIMIT,
                    expected_txs: vec![&tx3],
                },
            ],
        );
    }

    #[test]
    fn test_invalid_chain_id() {
        let tx1 = make_tx(S1, BASE_FEE, GAS_LIMIT, 1, 10);

        run_custom_eth_txpool_test(
            EthBlockPolicy::new(GENESIS_SEQ_NUM, u128::MAX, 0, 1),
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
                },
            ],
        );
    }
}
