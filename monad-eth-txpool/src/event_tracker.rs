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
    collections::{btree_map::Entry as BTreeMapEntry, BTreeMap},
    time::Instant,
};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_primitives::TxHash;
use monad_eth_txpool_types::{
    EthTxPoolDropReason, EthTxPoolEventType, EthTxPoolEvictReason, EthTxPoolInternalDropReason,
};
use tracing::error;

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
            self.metrics.insert_owned_txs.inc();
        } else {
            self.metrics.insert_forwarded_txs.inc();
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
                self.metrics.drop_not_well_formed.inc();
            }
            EthTxPoolDropReason::InvalidSignature => {
                self.metrics.drop_invalid_signature.inc();
            }
            EthTxPoolDropReason::NonceTooLow => {
                self.metrics.drop_nonce_too_low.inc();
            }
            EthTxPoolDropReason::FeeTooLow => {
                self.metrics.drop_fee_too_low.inc();
            }
            EthTxPoolDropReason::InsufficientBalance => {
                self.metrics.drop_insufficient_balance.inc();
            }
            EthTxPoolDropReason::ExistingHigherPriority => {
                self.metrics.drop_existing_higher_priority.inc();
            }
            EthTxPoolDropReason::ReplacedByHigherPriority { .. } => {
                self.metrics.drop_replaced_by_higher_priority.inc();
            }
            EthTxPoolDropReason::PoolFull => {
                self.metrics.drop_pool_full.inc();
            }
            EthTxPoolDropReason::PoolNotReady => {
                self.metrics.drop_pool_not_ready.inc();
            }
            EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::ExecutionStateReadError) => {
                self.metrics.drop_internal_state_read_error.inc();
            }
            EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::NotReady) => {
                self.metrics.drop_internal_not_ready.inc();
            }
        }

        match self.events.entry(tx_hash) {
            BTreeMapEntry::Vacant(v) => {
                v.insert(EthTxPoolEventType::Drop { reason });
            }
            BTreeMapEntry::Occupied(mut o) => match &reason {
                EthTxPoolDropReason::NotWellFormed(_)
                | EthTxPoolDropReason::InvalidSignature
                | EthTxPoolDropReason::NonceTooLow
                | EthTxPoolDropReason::FeeTooLow
                | EthTxPoolDropReason::InsufficientBalance
                | EthTxPoolDropReason::ReplacedByHigherPriority { .. }
                | EthTxPoolDropReason::PoolFull
                | EthTxPoolDropReason::PoolNotReady
                | EthTxPoolDropReason::Internal(
                    EthTxPoolInternalDropReason::ExecutionStateReadError,
                )
                | EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::NotReady) => {
                    o.insert(EthTxPoolEventType::Drop { reason });
                }
                EthTxPoolDropReason::ExistingHigherPriority => match o.get() {
                    EthTxPoolEventType::Insert { .. } => {}
                    EthTxPoolEventType::Commit => {
                        error!(%tx_hash, ?reason, "duplicate transaction already has a commit event");
                    }
                    EthTxPoolEventType::Drop { .. } => {
                        // A higher-fee replacement may drop A before a reordered retry of A arrives.
                    }
                    EthTxPoolEventType::Evict {
                        reason: existing_reason,
                    } => {
                        error!(
                            %tx_hash,
                            ?existing_reason,
                            ?reason,
                            "duplicate transaction already has an evict event"
                        );
                    }
                },
            },
        }
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
            self.metrics.tracked.remove_committed_addresses.inc();
        }

        for tx_hash in tx_hashes {
            self.metrics.tracked.remove_committed_txs.inc();

            self.events.insert(tx_hash, EthTxPoolEventType::Commit);
        }
    }

    pub fn tracked_evict_expired(
        &mut self,
        address: bool,
        tx_hashes: impl Iterator<Item = TxHash>,
    ) {
        if address {
            self.metrics.tracked.evict_expired_addresses.inc();
        }

        for tx_hash in tx_hashes {
            self.metrics.tracked.evict_expired_txs.inc();

            self.events.insert(
                tx_hash,
                EthTxPoolEventType::Evict {
                    reason: EthTxPoolEvictReason::Expired,
                },
            );
        }
    }

    pub fn update_aggregate_metrics(&mut self, tracked_addresses: u64, tracked_txs: u64) {
        self.metrics.tracked.addresses.set(tracked_addresses);
        self.metrics.tracked.txs.set(tracked_txs);
    }

    pub fn record_create_proposal(
        &mut self,
        tracked_addresses: usize,
        available_addresses: usize,
        backend_lookups: u64,
        proposal_txs: usize,
    ) {
        self.metrics.create_proposal.inc();
        self.metrics.create_proposal_txs.add(proposal_txs as u64);
        self.metrics
            .create_proposal_tracked_addresses
            .add(tracked_addresses as u64);
        self.metrics
            .create_proposal_available_addresses
            .add(available_addresses as u64);
        self.metrics
            .create_proposal_backend_lookups
            .add(backend_lookups);
    }
}

#[cfg(test)]
mod tests {
    use monad_eth_testutil::{make_legacy_tx, recover_tx, S1};

    use super::*;

    #[test]
    fn preserves_existing_event_on_duplicate_drop() {
        #[derive(Clone, Copy)]
        enum Setup {
            Insert,
            Drop,
            Commit,
        }

        let tx = recover_tx(make_legacy_tx(S1, 100, 30_000, 0, 10));

        for setup in [Setup::Insert, Setup::Drop, Setup::Commit] {
            let metrics = EthTxPoolMetrics::default();
            let mut events = BTreeMap::default();
            let mut tracker = EthTxPoolEventTracker::new(&metrics, &mut events);

            match setup {
                Setup::Insert => tracker.insert(&tx, false),
                Setup::Drop => tracker.drop(*tx.tx_hash(), EthTxPoolDropReason::FeeTooLow),
                Setup::Commit => tracker.tracked_commit(false, [*tx.tx_hash()].into_iter()),
            }
            tracker.drop(*tx.tx_hash(), EthTxPoolDropReason::ExistingHigherPriority);

            assert_eq!(
                events.get(tx.tx_hash()),
                Some(&match setup {
                    Setup::Insert => EthTxPoolEventType::Insert {
                        address: tx.signer(),
                        owned: false,
                        tx: tx.clone_inner(),
                    },
                    Setup::Drop => EthTxPoolEventType::Drop {
                        reason: EthTxPoolDropReason::FeeTooLow,
                    },
                    Setup::Commit => EthTxPoolEventType::Commit,
                })
            );
        }
    }

    #[test]
    fn replaces_insert_event_on_non_duplicate_drop() {
        let tx = recover_tx(make_legacy_tx(S1, 100, 30_000, 0, 10));
        let metrics = EthTxPoolMetrics::default();
        let mut events = BTreeMap::default();
        let mut tracker = EthTxPoolEventTracker::new(&metrics, &mut events);

        tracker.insert(&tx, false);
        tracker.drop(
            *tx.tx_hash(),
            EthTxPoolDropReason::ReplacedByHigherPriority {
                replacement: TxHash::ZERO,
            },
        );

        assert_eq!(
            events.get(tx.tx_hash()),
            Some(&EthTxPoolEventType::Drop {
                reason: EthTxPoolDropReason::ReplacedByHigherPriority {
                    replacement: TxHash::ZERO,
                },
            })
        );
    }
}
