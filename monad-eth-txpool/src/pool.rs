use std::collections::BTreeMap;

use monad_eth_block_policy::compute_txn_carriage_cost;
use monad_eth_tx::EthTransaction;
use monad_eth_types::{EthAddress, Nonce};
use monad_types::SeqNum;
use reth_primitives::TransactionSignedEcRecovered;
use sorted_vector_map::{
    map::{Iter, Keys},
    SortedVectorMap,
};
use tracing::trace;

#[derive(Clone, Debug, Default)]
pub struct TransactionGroup {
    transactions: SortedVectorMap<Nonce, (EthTransaction, f64)>,
}

impl TransactionGroup {
    fn add(&mut self, transaction: EthTransaction, ratio: f64) {
        self.transactions
            .insert(transaction.nonce(), (transaction, ratio));
    }

    fn find_nonce_gap(&self, mut next_nonce: Nonce) -> Option<Nonce> {
        for (nonce, _) in self.transactions.iter() {
            if *nonce != next_nonce {
                return Some(*nonce);
            }
            next_nonce += 1;
        }

        None
    }

    fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    fn len(&self) -> usize {
        self.transactions.len()
    }

    pub fn iter(&self) -> Iter<'_, u64, (TransactionSignedEcRecovered, f64)> {
        self.transactions.iter()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Pool {
    txs: SortedVectorMap<EthAddress, TransactionGroup>,
}

impl Pool {
    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    pub fn num_addresses(&self) -> usize {
        self.txs.len()
    }

    pub fn num_txs(&self) -> usize {
        self.txs.values().map(TransactionGroup::len).sum()
    }

    pub fn iter_addresses(&self) -> Keys<'_, EthAddress, TransactionGroup> {
        self.txs.keys()
    }

    pub fn iter(&self) -> Iter<'_, EthAddress, TransactionGroup> {
        self.txs.iter()
    }

    pub fn add_tx(&mut self, sender: EthAddress, eth_tx: TransactionSignedEcRecovered, ratio: f64) {
        self.txs.entry(sender).or_default().add(eth_tx, ratio);
    }

    pub fn validate_nonces_and_carriage_fee(
        &mut self,
        proposed_seq_num: SeqNum,
        account_base_nonces: BTreeMap<&EthAddress, u64>,
        account_base_reserve_balances: BTreeMap<&EthAddress, u128>,
    ) {
        for (address, tx_group) in self.txs.iter_mut() {
            let &lowest_valid_nonce = account_base_nonces
                .get(address)
                .expect("account_base_nonces must be populated");

            if tracing::event_enabled!(tracing::Level::TRACE) {
                tx_group
                    .transactions
                    .iter()
                    .filter(|&(nonce, _)| *nonce < lowest_valid_nonce)
                    .for_each(|(nonce, txn)| {
                        // TODO: return to trace after debugging session
                        tracing::info!(
                            "validate_nonces_and_carriage_fee \
                        txn {:?} will be excluded \
                        nonce is : {:?} < lowest_valid_nonce {:?} \
                        ",
                            txn.0.hash(),
                            nonce,
                            lowest_valid_nonce
                        )
                    })
            }

            // Remove transactions with nonces lower than the lowest valid nonce
            tx_group
                .transactions
                .retain(|&nonce, _| nonce >= lowest_valid_nonce);

            if let Some(nonce_gap) = tx_group.find_nonce_gap(lowest_valid_nonce) {
                // TODO: garbage collect
                let _ = tx_group.transactions.split_off(&nonce_gap);
            }

            let mut reserve_balance = *account_base_reserve_balances
                .get(address)
                .expect("account_base_reserve_balances must be populated");
            trace!(
                "ReserveBalance validate_nonces_and_carriage_fee 1 \
                    balance is: {:?} \
                    at block_id: {:?} \
                    for address: {:?}",
                reserve_balance,
                proposed_seq_num,
                address
            );

            let mut nonce_to_remove: Option<u64> = None;

            for (nonce, txn) in &tx_group.transactions {
                let txn_carriage_cost = compute_txn_carriage_cost(&txn.0);

                if reserve_balance >= txn_carriage_cost {
                    reserve_balance -= txn_carriage_cost;
                    trace!(
                        "ReserveBalance validate_nonces_and_carriage_fee 2 \
                            updated balance to: {:?} \
                            at block_id: {:?} \
                            at nonce: {:?} \
                            for address: {:?}",
                        reserve_balance,
                        proposed_seq_num,
                        nonce,
                        address
                    );
                } else {
                    nonce_to_remove = Some(*nonce);
                    tracing::info!(
                        "ReserveBalance create_proposal 3 \
                            insufficient balance at nonce: {:?} \
                            for address: {:?}",
                        nonce,
                        address
                    );
                    break;
                }
            }

            if let Some(gap) = nonce_to_remove {
                // TODO: garbage collect
                let _ = tx_group.transactions.split_off(&gap);
            }
        }

        // TODO: garbage collect
        self.txs.retain(|_, tx_group| !tx_group.is_empty());
    }
}
