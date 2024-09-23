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

const MAX_TXPOOL_SIZE: usize = 30_000;
const MAX_PROPOSAL_CHECKED_TXS: usize = 10_000;

#[derive(Clone, Debug)]
pub struct EthTxPool {
    /// pool is transient, garbage collected after creating a proposal
    pool: Pool,
    garbage: Vec<Pool>,
    do_local_insert: bool,
}

impl Default for EthTxPool {
    fn default() -> Self {
        Self {
            pool: Default::default(),
            garbage: Default::default(),
            do_local_insert: true,
        }
    }
}

impl EthTxPool {
    pub fn new(do_local_insert: bool) -> Self {
        Self {
            do_local_insert,
            ..Default::default()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pool.is_empty()
    }

    pub fn num_txs(&self) -> usize {
        self.pool.num_txs()
    }

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
        if self.do_local_insert {
            self.pool.add_tx(sender, eth_tx, ratio);
        }
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
        for extending_block in &extending_blocks {
            // note that this is a destructive operation
            // it's a bit weird that this is done based on extending_blocks (which aren't committed)
            //
            // however, we anyway are emptying the txpool at the end of this function
            self.pool.remove_stale_txs(extending_block)
        }
        let mut pool_len = self.pool.num_txs();
        while pool_len > MAX_PROPOSAL_CHECKED_TXS {
            pool_len -= self.pool.pop_last().expect("pool is not empty");
        }

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
            "created proposal"
        );

        if !self.garbage.is_empty() {
            warn!(
                garbage_len = self.garbage.len(),
                "we have received consecutive proposals without a mempool clear event in between"
            );
        }

        let old_pool = std::mem::take(&mut self.pool);
        self.garbage.push(old_pool);

        Ok(FullTransactionList::new(full_tx_list))
    }

    fn update_committed_block(&mut self, committed_block: &EthValidatedBlock<SCT>) {
        self.pool.remove_stale_txs(committed_block);
    }

    fn clear(&mut self) {
        self.garbage.clear();
    }
}
