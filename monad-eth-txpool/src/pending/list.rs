use std::collections::{btree_map::Entry, BTreeMap};

use monad_consensus_types::txpool::TxPoolInsertionError;
use monad_eth_types::Nonce;

use crate::transaction::ValidEthTransaction;

/// This struct ensures at the type level that nonce_map is never empty, a property used to enforce
/// that every address in the PendingTxMap has an associated pending transaction.
#[derive(Clone, Debug, Default)]
pub struct PendingTxList {
    nonce_map: BTreeMap<Nonce, ValidEthTransaction>,
}

impl PendingTxList {
    pub fn new(tx: ValidEthTransaction) -> Self {
        let mut nonce_map = BTreeMap::new();

        nonce_map.insert(tx.nonce(), tx);

        Self { nonce_map }
    }

    pub fn num_txs(&self) -> usize {
        self.nonce_map.len()
    }

    /// Returns true when the list does not contain a transaction with the same nonce and false when
    /// an existing tx was updated.
    pub fn try_add(&mut self, tx: ValidEthTransaction) -> Result<bool, TxPoolInsertionError> {
        match self.nonce_map.entry(tx.nonce()) {
            Entry::Vacant(v) => {
                v.insert(tx);
                Ok(true)
            }
            Entry::Occupied(mut existing_tx) => {
                if &tx < existing_tx.get() {
                    return Err(TxPoolInsertionError::ExistingHigherPriority);
                }

                existing_tx.insert(tx);
                Ok(false)
            }
        }
    }

    pub fn into_map(self) -> BTreeMap<Nonce, ValidEthTransaction> {
        self.nonce_map
    }
}
