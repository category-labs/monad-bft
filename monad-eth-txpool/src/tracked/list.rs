use std::{
    collections::{btree_map, BTreeMap},
    time::{Duration, Instant},
};

use alloy_primitives::Address;
use monad_consensus_types::txpool::TxPoolInsertionError;
use monad_eth_types::Nonce;
use tracing::error;

use crate::{pending::PendingTxList, transaction::ValidEthTransaction};

/// Stores byte-validated transactions alongside the an account_nonce to enforce at the type level
/// that all the transactions in the txs map have a nonce at least account_nonce. Similar to
/// PendingTxList, this struct also enforces non-emptyness in the txs map to guarantee that
/// TrackedTxMap has a transaction for every address.
#[derive(Clone, Debug, Default)]
pub struct TrackedTxList {
    account_nonce: Nonce,
    txs: BTreeMap<Nonce, (ValidEthTransaction, Instant)>,
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

        let now = Instant::now();

        Some(Self {
            account_nonce,
            txs: txs
                .into_iter()
                .map(|(nonce, tx)| (nonce, (tx, now)))
                .collect(),
        })
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
            .map_while(move |(tx_nonce, (tx, _))| {
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
        tx_expiry: Duration,
    ) -> Result<(), TxPoolInsertionError> {
        if tx.nonce() < self.account_nonce {
            return Err(TxPoolInsertionError::NonceTooLow);
        }

        let now = Instant::now();

        match self.txs.entry(tx.nonce()) {
            btree_map::Entry::Vacant(v) => {
                v.insert((tx, now));
            }
            btree_map::Entry::Occupied(mut entry) => {
                let (existing_tx, existing_tx_insert_time) = entry.get();

                if !tx_expired(existing_tx_insert_time, tx_expiry, &now) && &tx < existing_tx {
                    return Err(TxPoolInsertionError::ExistingHigherPriority);
                }

                entry.insert((tx, now));
            }
        }

        Ok(())
    }

    pub fn update_account_nonce(
        mut this: indexmap::map::OccupiedEntry<'_, Address, TrackedTxList>,
        account_nonce: u64,
    ) -> Option<indexmap::map::OccupiedEntry<'_, Address, TrackedTxList>> {
        this.get_mut().account_nonce = account_nonce;

        let Some((lowest_nonce, _)) = this.get().txs.first_key_value() else {
            error!("txpool invalid tracked tx list state");

            this.swap_remove();
            return None;
        };

        if lowest_nonce >= &account_nonce {
            return Some(this);
        }

        let txs = this.get_mut().txs.split_off(&account_nonce);

        if txs.is_empty() {
            this.swap_remove();
            return None;
        }

        this.get_mut().txs = txs;

        Some(this)
    }

    pub fn evict_expired_txs(
        mut this: indexmap::map::IndexedEntry<'_, Address, TrackedTxList>,
        tx_expiry: Duration,
    ) -> Option<indexmap::map::IndexedEntry<'_, Address, TrackedTxList>> {
        let now = Instant::now();

        this.get_mut()
            .txs
            .retain(|_, (_, tx_insert)| !tx_expired(tx_insert, tx_expiry, &now));

        if this.get().txs.is_empty() {
            this.swap_remove();
            return None;
        }

        Some(this)
    }
}

fn tx_expired(tx_insert: &Instant, expiry: Duration, now: &Instant) -> bool {
    &tx_insert
        .checked_add(expiry)
        .expect("time does not overflow")
        < now
}
