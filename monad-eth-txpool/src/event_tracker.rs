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

use std::{collections::BTreeMap, sync::atomic::Ordering, time::Instant};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_primitives::TxHash;
use monad_eth_txpool_types::{
    EthTxPoolDropReason, EthTxPoolEventType, EthTxPoolEvictReason, EthTxPoolInternalDropReason,
};

use crate::EthTxPoolMetrics;

pub struct EthTxPoolEventTracker<'a> {
    pub now: Instant,

    metrics: &'a EthTxPoolMetrics,
    events: &'a mut BTreeMap<TxHash, EthTxPoolEventType>,
}

impl<'a> EthTxPoolEventTracker<'a> {
    pub fn new(
        metrics: &'a EthTxPoolMetrics,
        events: &'a mut BTreeMap<TxHash, EthTxPoolEventType>,
    ) -> Self {
        Self {
            now: Instant::now(),

            metrics,
            events,
        }
    }

    pub fn insert(&mut self, tx: &Recovered<TxEnvelope>, owned: bool) {
        if owned {
            self.metrics.insert_owned_txs.fetch_add(1, Ordering::SeqCst);
        } else {
            self.metrics
                .insert_forwarded_txs
                .fetch_add(1, Ordering::SeqCst);
        }

        self.events.insert(
            *tx.tx_hash(),
            EthTxPoolEventType::Insert {
                address: tx.signer(),
                owned,
                tx: tx.clone_inner(),
            },
        );
    }

    pub fn replace(
        &mut self,
        old_tx_hash: TxHash,
        new_tx: &Recovered<TxEnvelope>,
        new_owned: bool,
    ) {
        self.drop(
            old_tx_hash,
            EthTxPoolDropReason::ReplacedByHigherPriority {
                replacement: *new_tx.tx_hash(),
            },
        );

        self.insert(new_tx, new_owned);
    }

    pub fn drop(&mut self, tx_hash: TxHash, reason: EthTxPoolDropReason) {
        match reason {
            EthTxPoolDropReason::NotWellFormed(_) => {
                self.metrics
                    .drop_not_well_formed
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::InvalidSignature => {
                self.metrics
                    .drop_invalid_signature
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::NonceTooLow => {
                self.metrics
                    .drop_nonce_too_low
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::FeeTooLow => {
                self.metrics.drop_fee_too_low.fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::InsufficientBalance => {
                self.metrics
                    .drop_insufficient_balance
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::ExistingHigherPriority => {
                self.metrics
                    .drop_existing_higher_priority
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::ReplacedByHigherPriority { .. } => {
                self.metrics
                    .drop_replaced_by_higher_priority
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::PoolFull => {
                self.metrics.drop_pool_full.fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::PoolNotReady => {
                self.metrics
                    .drop_pool_not_ready
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::StateBackendError) => {
                self.metrics
                    .drop_internal_state_backend_error
                    .fetch_add(1, Ordering::SeqCst);
            }
            EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::NotReady) => {
                self.metrics
                    .drop_internal_not_ready
                    .fetch_add(1, Ordering::SeqCst);
            }
        }

        self.events
            .insert(tx_hash, EthTxPoolEventType::Drop { reason });
    }

    pub fn drop_all(
        &mut self,
        txs: impl Iterator<Item = Recovered<TxEnvelope>>,
        reason: EthTxPoolDropReason,
    ) {
        for tx in txs {
            self.drop(tx.tx_hash().to_owned(), reason);
        }
    }

    pub fn tracked_commit(&mut self, address: bool, tx_hashes: impl Iterator<Item = TxHash>) {
        if address {
            self.metrics
                .tracked
                .remove_committed_addresses
                .fetch_add(1, Ordering::SeqCst);
        }

        for tx_hash in tx_hashes {
            self.metrics
                .tracked
                .remove_committed_txs
                .fetch_add(1, Ordering::SeqCst);

            self.events.insert(tx_hash, EthTxPoolEventType::Commit);
        }
    }

    pub fn tracked_evict_expired(
        &mut self,
        address: bool,
        tx_hashes: impl Iterator<Item = TxHash>,
    ) {
        if address {
            self.metrics
                .tracked
                .evict_expired_addresses
                .fetch_add(1, Ordering::SeqCst);
        }

        for tx_hash in tx_hashes {
            self.metrics
                .tracked
                .evict_expired_txs
                .fetch_add(1, Ordering::SeqCst);

            self.events.insert(
                tx_hash,
                EthTxPoolEventType::Evict {
                    reason: EthTxPoolEvictReason::Expired,
                },
            );
        }
    }

    pub fn update_aggregate_metrics(&mut self, tracked_addresses: u64, tracked_txs: u64) {
        self.metrics
            .tracked
            .addresses
            .store(tracked_addresses, Ordering::SeqCst);
        self.metrics
            .tracked
            .txs
            .store(tracked_txs, Ordering::SeqCst);
    }

    pub fn record_create_proposal(
        &mut self,
        tracked_addresses: usize,
        available_addresses: usize,
        backend_lookups: u64,
        proposal_txs: usize,
    ) {
        self.metrics.create_proposal.fetch_add(1, Ordering::SeqCst);
        self.metrics
            .create_proposal_txs
            .fetch_add(proposal_txs as u64, Ordering::SeqCst);
        self.metrics
            .create_proposal_tracked_addresses
            .fetch_add(tracked_addresses as u64, Ordering::SeqCst);
        self.metrics
            .create_proposal_available_addresses
            .fetch_add(available_addresses as u64, Ordering::SeqCst);
        self.metrics
            .create_proposal_backend_lookups
            .fetch_add(backend_lookups, Ordering::SeqCst);
    }
}
