use std::collections::{btree_map, BTreeMap};

use monad_consensus_types::txpool::TxPoolInsertionError;
use monad_eth_types::{EthAddress, Nonce};
use tracing::error;

use crate::{pending::PendingTxList, transaction::ValidEthTransaction};

/// Stores byte-validated transactions alongside the an account_nonce to enforce at the type level
/// that all the transactions in the txs map have a nonce at least account_nonce. Similar to
/// PendingTxList, this struct also enforces non-emptyness in the txs map to guarantee that
/// TrackedTxMap has a transaction for every address.
#[derive(Clone, Debug, Default)]
pub struct TrackedTxList {
    account_nonce: Nonce,
    txs: BTreeMap<Nonce, ValidEthTransaction>,
}

impl TrackedTxList {
    pub fn new_from_account_nonce_and_pending(
        account_nonce: u64,
        tx_list: PendingTxList,
    ) -> Option<Self> {
        let txs = tx_list.into_map().split_off(&account_nonce);

        if txs.is_empty() {
            return None;
        }

        Some(Self { account_nonce, txs })
    }

    pub fn num_txs(&self) -> usize {
        self.txs.len()
    }

    pub fn get_queued(
        &self,
        pending_account_nonce: Option<u64>,
    ) -> impl Iterator<Item = &ValidEthTransaction> {
        let mut account_nonce = pending_account_nonce.unwrap_or(self.account_nonce);

        self.txs
            .range(account_nonce..)
            .map_while(move |(tx_nonce, tx)| {
                debug_assert_eq!(*tx_nonce, tx.nonce());

                if *tx_nonce != account_nonce {
                    return None;
                }

                account_nonce += 1;
                Some(tx)
            })
    }

    pub(crate) fn try_add_tx(
        &mut self,
        tx: ValidEthTransaction,
    ) -> Result<(), TxPoolInsertionError> {
        if tx.nonce() < self.account_nonce {
            return Err(TxPoolInsertionError::NonceTooLow);
        }

        match self.txs.entry(tx.nonce()) {
            btree_map::Entry::Vacant(v) => {
                v.insert(tx);
            }
            btree_map::Entry::Occupied(mut existing_tx) => {
                if &tx < existing_tx.get() {
                    return Err(TxPoolInsertionError::ExistingHigherPriority);
                }

                existing_tx.insert(tx);
            }
        }

        Ok(())
    }

    pub fn update_account_nonce(
        mut this: indexmap::map::OccupiedEntry<'_, EthAddress, TrackedTxList>,
        account_nonce: u64,
    ) {
        this.get_mut().account_nonce = account_nonce;

        let Some((lowest_nonce, _)) = this.get().txs.first_key_value() else {
            error!("txpool invalid tracked tx list state");

            this.swap_remove();
            return;
        };

        if lowest_nonce >= &account_nonce {
            return;
        }

        let txs = this.get_mut().txs.split_off(&account_nonce);

        if txs.is_empty() {
            this.swap_remove();
            return;
        }

        this.get_mut().txs = txs;
    }
}
