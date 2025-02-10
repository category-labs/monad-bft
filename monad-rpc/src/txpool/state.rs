use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_primitives::{Address, TxHash};
use dashmap::DashMap;
use monad_eth_txpool_types::{
    EthTxPoolDropReason, EthTxPoolEvent, EthTxPoolEvictReason, EthTxPoolSnapshot,
};
use tokio::time::Instant;

const TX_EVICT_DURATION_SECONDS: u64 = 15 * 60;

pub struct EthTxPoolBridgeState {
    status: DashMap<TxHash, TxStatusEntry>,
    hash_address: DashMap<TxHash, Address>,
    address_hashes: DashMap<Address, HashSet<TxHash>>,
}

impl EthTxPoolBridgeState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            status: DashMap::default(),

            hash_address: DashMap::default(),
            address_hashes: DashMap::default(),
        })
    }

    pub fn add_tx(&self, tx: &Recovered<TxEnvelope>) {
        let hash = tx.tx_hash();
        let address = tx.signer();

        self.status
            .entry(*hash)
            .insert(TxStatusEntry::new(TxStatus::Unknown, Instant::now()));
        self.hash_address.entry(*hash).insert(address);
        self.address_hashes
            .entry(address)
            .or_default()
            .insert(*hash);
    }

    pub fn get_status_by_hash(&self, hash: &TxHash) -> Option<TxStatus> {
        Some(self.status.get(hash)?.value().status.to_owned())
    }

    pub fn get_status_by_address(&self, address: &Address) -> Option<HashMap<TxHash, TxStatus>> {
        let hashes = self.address_hashes.get(address)?.value().to_owned();

        let statuses = hashes
            .into_iter()
            .flat_map(|hash| {
                let status = self.status.get(&hash)?.value().status.to_owned();
                Some((hash, status))
            })
            .collect();

        Some(statuses)
    }

    pub fn apply_snapshot(&self, snapshot: EthTxPoolSnapshot) {
        let EthTxPoolSnapshot {
            mut pending,
            mut tracked,
        } = snapshot;

        let now = Instant::now();

        self.status.retain(|hash, status| {
            if pending.remove(hash) {
                *status = TxStatusEntry::new(TxStatus::Pending, now);
                return true;
            }

            if tracked.remove(hash) {
                *status = TxStatusEntry::new(TxStatus::Tracked, now);
                return true;
            }

            let Some((hash, address)) = self.hash_address.remove(hash) else {
                return false;
            };

            self.address_hashes.entry(address).and_modify(|hashes| {
                hashes.remove(&hash);
            });

            false
        });

        for tx_hash in pending {
            self.status
                .insert(tx_hash, TxStatusEntry::new(TxStatus::Pending, now));
        }

        for tx_hash in tracked {
            self.status
                .insert(tx_hash, TxStatusEntry::new(TxStatus::Tracked, now));
        }
    }

    pub fn handle_events(&self, events: Vec<EthTxPoolEvent>) {
        let tx_status = &self.status;

        let now = Instant::now();

        for event in events {
            match event {
                EthTxPoolEvent::Insert {
                    tx_hash,
                    owned: _,
                    tracked,
                } => {
                    tx_status.entry(tx_hash).insert(TxStatusEntry::new(
                        if tracked {
                            TxStatus::Tracked
                        } else {
                            TxStatus::Pending
                        },
                        now,
                    ));
                }
                EthTxPoolEvent::Replace {
                    old_tx_hash,
                    new_tx_hash,
                    new_owned: _,
                    tracked,
                } => {
                    tx_status
                        .entry(old_tx_hash)
                        .insert(TxStatusEntry::new(TxStatus::Replaced, now));
                    tx_status.entry(new_tx_hash).insert(TxStatusEntry::new(
                        if tracked {
                            TxStatus::Tracked
                        } else {
                            TxStatus::Pending
                        },
                        now,
                    ));
                }
                EthTxPoolEvent::Drop { tx_hash, reason } => {
                    tx_status
                        .entry(tx_hash)
                        .insert(TxStatusEntry::new(TxStatus::Dropped { reason }, now));
                }
                EthTxPoolEvent::Promoted { tx_hash } => {
                    tx_status
                        .entry(tx_hash)
                        .insert(TxStatusEntry::new(TxStatus::Tracked, now));
                }
                EthTxPoolEvent::Commit { tx_hash } => {
                    tx_status
                        .entry(tx_hash)
                        .insert(TxStatusEntry::new(TxStatus::Committed, now));
                }
                EthTxPoolEvent::Evict { tx_hash, reason } => {
                    tx_status
                        .entry(tx_hash)
                        .insert(TxStatusEntry::new(TxStatus::Evicted { reason }, now));
                }
            }
        }
    }

    pub fn cleanup(&self, now: Instant) {
        self.status.retain(|hash, status| {
            if now.duration_since(status.last_updated)
                < Duration::from_secs(TX_EVICT_DURATION_SECONDS)
            {
                return true;
            }

            if let Some((hash, address)) = self.hash_address.remove(hash) {
                if let Some(mut address_hashes) = self.address_hashes.get_mut(&address) {
                    address_hashes.remove(&hash);
                }
            }

            false
        });

        self.address_hashes.retain(|_, hashes| !hashes.is_empty());
    }
}

pub struct TxStatusEntry {
    status: TxStatus,
    last_updated: Instant,
}

impl TxStatusEntry {
    pub fn new(status: TxStatus, now: Instant) -> Self {
        Self {
            status,
            last_updated: now,
        }
    }
}

#[derive(Clone)]
pub enum TxStatus {
    // No response
    Unknown,

    // Alive
    Pending,
    Tracked,

    // Dead
    Dropped { reason: EthTxPoolDropReason },
    Evicted { reason: EthTxPoolEvictReason },
    Replaced,
    Committed,
}
