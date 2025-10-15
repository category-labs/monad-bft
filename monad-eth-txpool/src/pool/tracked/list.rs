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
use indexmap::map::VacantEntry;
use monad_chain_config::{execution_revision::MonadExecutionRevision, revision::ChainRevision};
use monad_eth_block_policy::nonce_usage::NonceUsage;
use monad_eth_txpool_types::EthTxPoolDropReason;
use monad_types::Nonce;
use tracing::error;

use super::limits::{TrackedTxLimitAddToken, TrackedTxLimits};
use crate::{pool::transaction::ValidEthTransaction, EthTxPoolEventTracker};

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
    pub fn try_new(
        this_entry: VacantEntry<'_, Address, Self>,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        limits: &mut TrackedTxLimits,
        txs: Vec<ValidEthTransaction>,
        account_nonce: u64,
        on_insert: &mut impl FnMut(&ValidEthTransaction),
        last_commit_base_fee: u64,
    ) {
        let mut this = TrackedTxList {
            account_nonce,
            txs: BTreeMap::default(),
        };

        for tx in txs {
            let tx_token = if this.txs.is_empty() {
                limits.prepare_add_tx_to_new_address(event_tracker, tx)
            } else {
                limits.prepare_add_tx_to_existing(event_tracker, tx)
            };

            let Some(tx_token) = tx_token else {
                continue;
            };

            let Some(tx) = this.try_insert_tx(tx_token, last_commit_base_fee) else {
                continue;
            };

            on_insert(tx);
        }

        if this.txs.is_empty() {
            return;
        }

        this_entry.insert(this);
    }

    pub fn iter(&self) -> impl Iterator<Item = &ValidEthTransaction> {
        self.txs.values().map(|(tx, _)| tx)
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut ValidEthTransaction> {
        self.txs.values_mut().map(|(tx, _)| tx)
    }

    pub fn into_txs(self) -> impl Iterator<Item = ValidEthTransaction> {
        self.txs.into_values().map(|(tx, _)| tx)
    }

    pub fn num_txs(&self) -> usize {
        self.txs.len()
    }

    pub fn get_queued(
        &self,
        pending_nonce_usage: Option<NonceUsage>,
    ) -> impl Iterator<Item = &ValidEthTransaction> {
        let mut account_nonce = pending_nonce_usage
            .map_or(self.account_nonce, |pending_nonce_usage| {
                pending_nonce_usage.apply_to_account_nonce(self.account_nonce)
            });

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
        tx_token: TrackedTxLimitAddToken,
        last_commit_base_fee: u64,
    ) -> Option<&ValidEthTransaction> {
        if tx_token.nonce() < self.account_nonce {
            tx_token.cancel(EthTxPoolDropReason::NonceTooLow);
            return None;
        }

        match self.txs.entry(tx_token.nonce()) {
            btree_map::Entry::Vacant(v) => Some(tx_token.insert_vacant(v)),
            btree_map::Entry::Occupied(o) => {
                let (existing_tx, existing_tx_insert_time) = o.get();

                if tx_expired(
                    existing_tx_insert_time,
                    tx_token.limits().tx_expiry_during_insert(),
                    tx_token.now(),
                ) || tx_token.has_higher_priority(existing_tx, last_commit_base_fee)
                {
                    Some(tx_token.replace_existing(o))
                } else {
                    tx_token.cancel(EthTxPoolDropReason::ExistingHigherPriority);
                    None
                }
            }
        }
    }

    pub fn update_committed_nonce_usage(
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        limits: &mut TrackedTxLimits,
        mut this: indexmap::map::OccupiedEntry<'_, Address, Self>,
        nonce_usage: NonceUsage,
    ) {
        let account_nonce = nonce_usage.apply_to_account_nonce(this.get().account_nonce);
        this.get_mut().account_nonce = account_nonce;

        let Some((lowest_nonce, _)) = this.get().txs.first_key_value() else {
            error!("txpool invalid tracked tx list state");

            this.swap_remove();
            return;
        };

        if lowest_nonce >= &account_nonce {
            return;
        }

        let removed_txs = {
            let remaining_txs = this.get_mut().txs.split_off(&account_nonce);

            std::mem::replace(&mut this.get_mut().txs, remaining_txs)
        };

        let removed_address = this.get().txs.is_empty();

        limits.remove_committed_txs(
            event_tracker,
            removed_txs.into_values().map(|(tx, _)| tx),
            removed_address,
        );

        if removed_address {
            this.swap_remove();
        }
    }

    // Produces true when the entry was removed and false otherwise
    pub fn evict_expired_txs(
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        limits: &mut TrackedTxLimits,
        mut this: indexmap::map::IndexedEntry<'_, Address, Self>,
    ) -> bool {
        let now = Instant::now();

        let txs = &mut this.get_mut().txs;

        let mut removed_txs = Vec::default();

        txs.retain(|_, (tx, tx_insert)| {
            if !tx_expired(tx_insert, limits.tx_expiry_during_evict(), &now) {
                return true;
            }

            removed_txs.push(tx.clone());

            false
        });

        let removed_address = txs.is_empty();

        limits.remove_expired_txs(event_tracker, removed_txs.into_iter(), removed_address);

        if removed_address {
            this.swap_remove();
        }

        removed_address
    }

    // Produces true when the entry was removed and false otherwise
    pub fn static_validate_all_txs<CRT>(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        limits: &mut TrackedTxLimits,
        chain_id: u64,
        chain_revision: &CRT,
        execution_revision: &MonadExecutionRevision,
    ) -> bool
    where
        CRT: ChainRevision,
    {
        let mut removed_txs = Vec::default();

        self.txs.retain(|_, (tx, _)| {
            let Err(error) = tx.static_validate(
                chain_id,
                chain_revision.chain_params(),
                execution_revision.execution_chain_params(),
            ) else {
                return true;
            };

            removed_txs.push(tx.clone());

            false
        });

        let removed_address = self.txs.is_empty();

        limits.remove_expired_txs(event_tracker, removed_txs.into_iter(), removed_address);

        removed_address
    }
}

fn tx_expired(tx_insert: &Instant, expiry: Duration, now: &Instant) -> bool {
    &tx_insert
        .checked_add(expiry)
        .expect("time does not overflow")
        < now
}
