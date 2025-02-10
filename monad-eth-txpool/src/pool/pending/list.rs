use std::collections::{btree_map::Entry, BTreeMap};

use alloy_primitives::Address;
use monad_eth_txpool_types::EthTxPoolDropReason;
use monad_eth_types::Nonce;

use crate::{pool::transaction::ValidEthTransaction, EthTxPoolEventTracker};

/// This struct ensures at the type level that nonce_map is never empty, a property used to enforce
/// that every address in the PendingTxMap has an associated pending transaction.
#[derive(Clone, Debug, Default)]
pub struct PendingTxList {
    nonce_map: BTreeMap<Nonce, ValidEthTransaction>,
}

impl PendingTxList {
    pub fn insert_entry<'a>(
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        entry: indexmap::map::VacantEntry<'a, Address, Self>,
        tx: ValidEthTransaction,
    ) -> &'a ValidEthTransaction {
        let mut nonce_map = BTreeMap::new();

        let nonce = tx.nonce();

        event_tracker.insert_pending(tx.hash(), tx.is_owned());
        nonce_map.insert(nonce, tx);

        let entry = entry.insert(Self { nonce_map });

        entry.nonce_map.get(&nonce).unwrap()
    }

    pub fn num_txs(&self) -> usize {
        self.nonce_map.len()
    }

    /// Produces a reference to the tx if it is present in the tx list after attempting to insert
    /// it.
    pub fn try_insert(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        tx: ValidEthTransaction,
    ) -> Option<&ValidEthTransaction> {
        match self.nonce_map.entry(tx.nonce()) {
            Entry::Vacant(v) => {
                event_tracker.insert_pending(tx.hash(), tx.is_owned());
                Some(v.insert(tx))
            }
            Entry::Occupied(mut entry) => {
                let existing_tx = entry.get();

                if &tx < existing_tx {
                    event_tracker.drop(tx.hash(), EthTxPoolDropReason::ExistingHigherPriority);
                    return None;
                }

                event_tracker.replace_pending(existing_tx.hash(), tx.hash(), tx.is_owned());
                entry.insert(tx);
                Some(entry.into_mut())
            }
        }
    }

    pub fn into_map(self) -> BTreeMap<Nonce, ValidEthTransaction> {
        self.nonce_map
    }
}
