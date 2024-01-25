pub mod tx_states;

use std::collections::BTreeMap;

use monad_consensus_types::payload::FullTransactionList;
use monad_eth_tx::{EthFullTransactionList, EthTransaction, EthTxHash};
use monad_eth_types::EthAddress;
use tx_states::{EthTxState, EthTxStates};

#[derive(Debug, Default)]
pub struct EthAccountState {
    pub max_reserve_balance: usize,
    pub reserve_balance: usize,
    pub transaction_states: EthTxStates,
}

impl EthAccountState {
    pub fn update_max_reserve_balance(&mut self, new_max: usize) {
        // TODO: Handle case where the new max reserve balance is less than the current
        // reserve balance in the account
        self.max_reserve_balance = new_max;
    }

    pub fn update_reserve_balance(&mut self, delta: usize) {
        assert!(self.reserve_balance + delta <= self.max_reserve_balance);

        self.reserve_balance += delta;
    }
}

#[derive(Debug, Default)]
pub struct EthAccountStates(pub BTreeMap<EthAddress, EthAccountState>);

impl EthAccountStates {
    /// Transactions from IPC and leftover transactions from previous leader should be validated
    /// using this function
    pub fn validate_single_txn(&mut self, txn: EthTransaction) -> bool {
        let acc_address = EthAddress(txn.signer());

        let Some(acc_state) = self.0.get_mut(&acc_address) else {
            // Account has no state yet => No reserve balance
            return false;
        };

        let txn_hash: EthTxHash = txn.hash;

        if acc_state.transaction_states.states.contains_key(&txn_hash) {
            // Transaction already in the txpool
            return true;
        }

        // TODO: Use consensus base fee
        if acc_state.reserve_balance >= 1 {
            // Decrement the reserve balance
            acc_state.reserve_balance -= 1;

            // Store transaction as not proposed
            acc_state
                .transaction_states
                .states
                .insert(txn_hash, EthTxState::NotProposed);

            return true;
        }

        // Not enough reserve balance
        false
    }

    /// Transactions from block proposals should be validated using this function
    pub fn validate_block_txns(&mut self, txns: &FullTransactionList) -> bool {
        let Ok(eth_txns) = EthFullTransactionList::rlp_decode(txns.bytes().clone()) else {
            return false;
        };

        // Group transactions of the block by sender address
        let mut txns_from_address = BTreeMap::<EthAddress, Vec<EthTransaction>>::new();
        for txn in eth_txns.0 {
            txns_from_address
                .entry(EthAddress(txn.signer()))
                .or_default()
                .push(txn);
        }

        // Validate all the transactions in the block before (possibly) evicting transactions
        for (address, txns) in txns_from_address.iter() {
            let Some(acc_state) = self.0.get(address) else {
                // Account has no reserve balance
                return false;
            };

            // TODO: Use consensus base fee
            if acc_state.reserve_balance + acc_state.transaction_states.num_not_proposed()
                < txns.len()
            {
                return false;
            }
        }
        // At this point, the block is valid.

        let mut evicted_txns = Vec::new();

        // Add all the transactions from the block as Proposed into the account states
        for (address, txns) in txns_from_address {
            let acc_state = self
                .0
                .get_mut(&address)
                .expect("transactions were verified");

            for txn in txns {
                // If there is not enough reserve balance, evict a transaction
                if acc_state.reserve_balance == 0 {
                    let evicted_txn = acc_state
                        .transaction_states
                        .evict_not_proposed()
                        .expect("should evict one transaction");
                    evicted_txns.push(evicted_txn);
                } else {
                    // TODO: Use consensus base fee
                    acc_state.reserve_balance -= 1;
                }

                acc_state.transaction_states.add_proposal(txn.hash);
            }
        }

        for _evicted_txn in evicted_txns {
            // TODO: Remove evicted transactions from the txpool
        }

        true
    }

    /// Reverts the proposed transactions in a rejected block
    pub fn revert_block_txns(&mut self, txns: &FullTransactionList) {
        let eth_txns =
            EthFullTransactionList::rlp_decode(txns.bytes().clone()).expect("block was verified");

        for txn in eth_txns.0 {
            let acc_adress = EthAddress(txn.signer());
            let acc_state = self
                .0
                .get_mut(&acc_adress)
                .expect("transaction was verified");

            acc_state.transaction_states.revert_proposal(txn.hash);
        }
    }

    /// Removes the transactions that were committed with a block
    /// TODO: Committed transactions should also be removed from the txpool
    pub fn remove_committed_txns(&mut self, txns: &FullTransactionList) {
        let eth_txns =
            EthFullTransactionList::rlp_decode(txns.bytes().clone()).expect("block was verified");

        for txn in eth_txns.0 {
            let acc_adress = EthAddress(txn.signer());
            let acc_state = self
                .0
                .get_mut(&acc_adress)
                .expect("transaction was verified");

            acc_state.transaction_states.remove_committed(txn.hash);
        }
    }

    // Max reserve balance updates from exection
    pub fn update_max_reserve_balances(&mut self, max_reserve_balances: Vec<(EthAddress, usize)>) {
        for (acc_address, max_reserve_balance) in max_reserve_balances {
            let acc_state = self.0.entry(acc_address).or_default();

            acc_state.update_max_reserve_balance(max_reserve_balance);
        }
    }

    // Reserve balance updates from execution
    pub fn update_reserve_balances(&mut self, deltas: Vec<(EthAddress, usize)>) {
        for (acc_address, delta) in deltas {
            let acc_state = self
                .0
                .get_mut(&acc_address)
                .expect("account should exist with a max reserve balance");

            acc_state.update_reserve_balance(delta);
        }
    }
}
