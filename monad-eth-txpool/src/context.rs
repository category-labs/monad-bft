use std::time::Instant;

use alloy_primitives::TxHash;
use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolEvent, EthTxPoolEvictReason};

use crate::{EthTxPoolMetrics, EthTxPoolSnapshotManager};

pub struct EthTxPoolEventTracker<'a> {
    pub now: Instant,

    metrics: &'a mut EthTxPoolMetrics,
    snapshot_manager: &'a mut EthTxPoolSnapshotManager,
    events: &'a mut Vec<EthTxPoolEvent>,
}

impl<'a> EthTxPoolEventTracker<'a> {
    pub fn new(
        metrics: &'a mut EthTxPoolMetrics,
        snapshot_manager: &'a mut EthTxPoolSnapshotManager,
        events: &'a mut Vec<EthTxPoolEvent>,
    ) -> Self {
        Self {
            now: Instant::now(),

            metrics,
            events,
            snapshot_manager,
        }
    }

    pub fn insert_pending(&mut self, tx_hash: TxHash, owned: bool) {
        if owned {
            self.metrics.insert_owned_txs += 1;
        } else {
            self.metrics.insert_forwarded_txs += 1;
        }

        self.snapshot_manager.add_pending(&tx_hash);

        self.events.push(EthTxPoolEvent::Insert {
            tx_hash,
            owned,
            tracked: false,
        });
    }

    pub fn insert_tracked(&mut self, tx_hash: TxHash, owned: bool) {
        if owned {
            self.metrics.insert_owned_txs += 1;
        } else {
            self.metrics.insert_forwarded_txs += 1;
        }

        self.snapshot_manager.add_tracked(&tx_hash);

        self.events.push(EthTxPoolEvent::Insert {
            tx_hash,
            owned,
            tracked: true,
        });
    }

    pub fn replace_pending(&mut self, old_tx_hash: TxHash, new_tx_hash: TxHash, new_owned: bool) {
        if new_owned {
            self.metrics.insert_owned_txs += 1;
        } else {
            self.metrics.insert_forwarded_txs += 1;
        }

        self.snapshot_manager.remove_pending(&old_tx_hash);
        self.snapshot_manager.add_pending(&new_tx_hash);

        self.events.push(EthTxPoolEvent::Replace {
            old_tx_hash,
            new_tx_hash,
            new_owned,
            tracked: false,
        });
    }

    pub fn replace_tracked(&mut self, old_tx_hash: TxHash, new_tx_hash: TxHash, new_owned: bool) {
        if new_owned {
            self.metrics.insert_owned_txs += 1;
        } else {
            self.metrics.insert_forwarded_txs += 1;
        }

        self.snapshot_manager.remove_tracked(&old_tx_hash);
        self.snapshot_manager.add_tracked(&new_tx_hash);

        self.events.push(EthTxPoolEvent::Replace {
            old_tx_hash,
            new_tx_hash,
            new_owned,
            tracked: true,
        });
    }

    pub fn drop(&mut self, tx_hash: TxHash, reason: EthTxPoolDropReason) {
        match reason {
            EthTxPoolDropReason::NotWellFormed => {
                self.metrics.drop_not_well_formed += 1;
            }
            EthTxPoolDropReason::NonceTooLow => {
                self.metrics.drop_nonce_too_low += 1;
            }
            EthTxPoolDropReason::FeeTooLow => {
                self.metrics.drop_fee_too_low += 1;
            }
            EthTxPoolDropReason::InsufficientBalance => {
                self.metrics.drop_insufficient_balance += 1;
            }
            EthTxPoolDropReason::PoolFull => {
                self.metrics.drop_pool_full += 1;
            }
            EthTxPoolDropReason::ExistingHigherPriority => {
                self.metrics.drop_existing_higher_priority += 1;
            }
        }

        self.events.push(EthTxPoolEvent::Drop { tx_hash, reason });
    }

    pub fn pending_promote(&mut self, tx_hashes: impl Iterator<Item = TxHash>) {
        self.metrics.pending.promote_addresses += 1;

        for tx_hash in tx_hashes {
            self.metrics.pending.promote_txs += 1;

            self.snapshot_manager.promote(&tx_hash);

            self.events.push(EthTxPoolEvent::Promoted {
                tx_hash: tx_hash.to_owned(),
            });
        }
    }

    pub fn pending_drop_unknown(&mut self, tx_hashes: impl Iterator<Item = TxHash>) {
        self.metrics.pending.drop_unknown_addresses += 1;

        for tx_hash in tx_hashes {
            self.metrics.pending.drop_unknown_txs += 1;

            self.snapshot_manager.remove_pending(&tx_hash);

            self.events.push(EthTxPoolEvent::Drop {
                tx_hash,
                reason: EthTxPoolDropReason::InsufficientBalance,
            });
        }
    }

    pub fn pending_drop_low_nonce(
        &mut self,
        address: bool,
        tx_hashes: impl Iterator<Item = TxHash>,
    ) {
        if address {
            self.metrics.pending.drop_low_nonce_addresses += 1;
        }

        for tx_hash in tx_hashes {
            self.metrics.pending.drop_low_nonce_txs += 1;

            self.snapshot_manager.remove_pending(&tx_hash);

            self.events.push(EthTxPoolEvent::Drop {
                tx_hash,
                reason: EthTxPoolDropReason::NonceTooLow,
            });
        }
    }

    pub fn tracked_commit(&mut self, address: bool, tx_hashes: impl Iterator<Item = TxHash>) {
        if address {
            self.metrics.tracked.remove_committed_addresses += 1;
        }

        for tx_hash in tx_hashes {
            self.metrics.tracked.remove_committed_txs += 1;

            self.snapshot_manager.remove_tracked(&tx_hash);

            self.events.push(EthTxPoolEvent::Commit { tx_hash });
        }
    }

    pub fn tracked_evict_expired(
        &mut self,
        address: bool,
        tx_hashes: impl Iterator<Item = TxHash>,
    ) {
        if address {
            self.metrics.tracked.evict_expired_addresses += 1;
        }

        for tx_hash in tx_hashes {
            self.metrics.tracked.evict_expired_txs += 1;

            self.snapshot_manager.remove_tracked(&tx_hash);

            self.events.push(EthTxPoolEvent::Evict {
                tx_hash,
                reason: EthTxPoolEvictReason::Expired,
            });
        }
    }

    pub fn update_aggregate_metrics(
        &mut self,
        pending_addresses: u64,
        pending_txs: u64,
        tracked_addresses: u64,
        tracked_txs: u64,
    ) {
        self.metrics.pending.addresses = pending_addresses;
        self.metrics.pending.txs = pending_txs;
        self.metrics.tracked.addresses = tracked_addresses;
        self.metrics.tracked.txs = tracked_txs;
    }
}
