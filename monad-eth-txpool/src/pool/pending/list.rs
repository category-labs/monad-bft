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
    pub fn iter(&self) -> impl Iterator<Item = &ValidEthTransaction> {
        self.nonce_map.values()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut ValidEthTransaction> {
        self.nonce_map.values_mut()
    }

    pub fn insert_entry<'a>(
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        entry: indexmap::map::VacantEntry<'a, Address, Self>,
        tx: ValidEthTransaction,
    ) -> &'a ValidEthTransaction {
        let mut nonce_map = BTreeMap::new();

        let nonce = tx.nonce();

        event_tracker.insert_pending(tx.raw(), tx.is_owned());
        nonce_map.insert(nonce, tx);

        let entry = entry.insert(Self { nonce_map });

        entry.nonce_map.get(&nonce).unwrap()
    }

    pub fn num_txs(&self) -> usize {
        self.nonce_map.len()
    }

    /// Produces a reference to the tx if it is present in the tx list after attempting to insert
    /// it along with a boolean indicating if it was newly added, ie. does not replace an existing
    /// tx.
    pub fn try_insert_tx(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        tx: ValidEthTransaction,
        last_commit_base_fee: u64,
    ) -> Option<(&ValidEthTransaction, bool)> {
        match self.nonce_map.entry(tx.nonce()) {
            Entry::Vacant(v) => {
                event_tracker.insert_pending(tx.raw(), tx.is_owned());
                Some((v.insert(tx), true))
            }
            Entry::Occupied(mut entry) => {
                let existing_tx = entry.get();

                if tx.has_higher_priority(existing_tx, last_commit_base_fee) {
                    event_tracker.replace_pending(
                        tx.signer_ref(),
                        existing_tx.hash(),
                        tx.hash(),
                        tx.is_owned(),
                    );
                    entry.insert(tx);

                    Some((entry.into_mut(), false))
                } else {
                    event_tracker.drop(tx.hash(), EthTxPoolDropReason::ExistingHigherPriority);

                    None
                }
            }
        }
    }

    pub fn into_map(self) -> BTreeMap<Nonce, ValidEthTransaction> {
        self.nonce_map
    }
}
