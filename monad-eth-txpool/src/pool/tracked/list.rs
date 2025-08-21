// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::{btree_map, BTreeMap},
    time::{Duration, Instant},
};

use alloy_primitives::Address;
use monad_eth_txpool_types::EthTxPoolDropReason;
use monad_types::Nonce;
use tracing::error;

use crate::{
    pool::{pending::PendingTxList, transaction::ValidEthTransaction},
    EthTxPoolEventTracker,
};

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
    pub fn iter(&self) -> impl Iterator<Item = &ValidEthTransaction> {
        self.txs.values().map(|(tx, _)| tx)
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut ValidEthTransaction> {
        self.txs.values_mut().map(|(tx, _)| tx)
    }

    pub fn new_from_promote_pending(
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        account_nonce: u64,
        tx_list: PendingTxList,
    ) -> Option<Self> {
        let mut tx_list = tx_list.into_map();

        let txs = tx_list.split_off(&account_nonce);

        event_tracker.pending_drop_low_nonce(
            txs.is_empty(),
            tx_list.values().map(ValidEthTransaction::hash),
        );

        if txs.is_empty() {
            return None;
        }

        event_tracker.pending_promote(txs.values().map(|tx| (tx.is_owned(), tx.raw())));

        Some(Self {
            account_nonce,
            txs: txs
                .into_iter()
                .map(|(nonce, tx)| (nonce, (tx, event_tracker.now)))
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

    pub(crate) fn try_insert_tx(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        tx: ValidEthTransaction,
        tx_expiry: Duration,
    ) -> Option<&ValidEthTransaction> {
        if tx.nonce() < self.account_nonce {
            event_tracker.drop(tx.hash(), EthTxPoolDropReason::NonceTooLow);
            return None;
        }

        match self.txs.entry(tx.nonce()) {
            btree_map::Entry::Vacant(v) => {
                event_tracker.insert_pending(tx.raw(), tx.is_owned());
                Some(&v.insert((tx, event_tracker.now)).0)
            }
            btree_map::Entry::Occupied(mut entry) => {
                let (existing_tx, existing_tx_insert_time) = entry.get();

                if !tx_expired(existing_tx_insert_time, tx_expiry, &event_tracker.now)
                    && &tx <= existing_tx
                {
                    event_tracker.drop(tx.hash(), EthTxPoolDropReason::ExistingHigherPriority);
                    return None;
                }

                event_tracker.replace_tracked(
                    tx.signer_ref(),
                    existing_tx.hash(),
                    tx.hash(),
                    tx.is_owned(),
                );
                entry.insert((tx, event_tracker.now));
                Some(&entry.into_mut().0)
            }
        }
    }

    pub fn update_committed_account_nonce(
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        mut this: indexmap::map::OccupiedEntry<'_, Address, Self>,
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

        event_tracker.tracked_commit(
            txs.is_empty(),
            this.get().txs.values().map(|(tx, _)| tx.hash()),
        );

        if txs.is_empty() {
            this.swap_remove();
            return;
        }

        this.get_mut().txs = txs;
    }

    // Produces true when the entry was removed and false otherwise
    pub fn evict_expired_txs(
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        mut this: indexmap::map::IndexedEntry<'_, Address, Self>,
        tx_expiry: Duration,
    ) -> bool {
        let now = Instant::now();

        let txs = &mut this.get_mut().txs;

        let mut removed_hashes = Vec::default();

        txs.retain(|_, (tx, tx_insert)| {
            if !tx_expired(tx_insert, tx_expiry, &now) {
                return true;
            }

            removed_hashes.push(tx.hash());
            false
        });

        event_tracker.tracked_evict_expired(txs.is_empty(), removed_hashes.into_iter());

        if txs.is_empty() {
            this.swap_remove();
            true
        } else {
            false
        }
    }
}

fn tx_expired(tx_insert: &Instant, expiry: Duration, now: &Instant) -> bool {
    &tx_insert
        .checked_add(expiry)
        .expect("time does not overflow")
        < now
}
