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
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use alloy_consensus::TxEnvelope;
use alloy_primitives::{Address, TxHash};
use dashmap::{DashMap, Entry};
use monad_eth_txpool_types::{EthTxPoolEvent, EthTxPoolEventType, EthTxPoolSnapshot};
use tokio::time::Instant;

use super::TxStatus;

const TX_EVICT_DURATION_SECONDS: u64 = 15 * 60;

pub(super) type EthTxPoolBridgeEvictionQueue = VecDeque<(Instant, TxHash)>;
pub(super) type TxStatusReceiverSender =
    tokio::sync::oneshot::Sender<tokio::sync::watch::Receiver<TxStatus>>;

#[derive(Clone)]
pub struct EthTxPoolBridgeStateView {
    status: Arc<DashMap<TxHash, tokio::sync::watch::Sender<TxStatus>>>,
    address_hashes: Arc<DashMap<Address, HashSet<TxHash>>>,
}

impl EthTxPoolBridgeStateView {
    pub fn get_status_by_hash(&self, hash: &TxHash) -> Option<TxStatus> {
        Some(self.status.get(hash)?.value().borrow().to_owned())
    }

    pub(super) fn get_status_by_address(
        &self,
        address: &Address,
    ) -> Option<HashMap<TxHash, TxStatus>> {
        let hashes = self.address_hashes.get(address)?.value().to_owned();

        let statuses = hashes
            .into_iter()
            .flat_map(|hash| {
                let status = self.status.get(&hash)?.value().borrow().to_owned();
                Some((hash, status))
            })
            .collect();

        Some(statuses)
    }

    pub fn for_testing() -> Self {
        Self {
            status: Default::default(),
            address_hashes: Default::default(),
        }
    }
}

pub struct EthTxPoolBridgeState {
    status: Arc<DashMap<TxHash, tokio::sync::watch::Sender<TxStatus>>>,
    hash_address: Arc<DashMap<TxHash, Address>>,
    address_hashes: Arc<DashMap<Address, HashSet<TxHash>>>,
}

impl EthTxPoolBridgeState {
    pub fn new(
        eviction_queue: &mut EthTxPoolBridgeEvictionQueue,
        snapshot: EthTxPoolSnapshot,
    ) -> Self {
        let this = Self {
            status: Default::default(),
            hash_address: Default::default(),
            address_hashes: Default::default(),
        };

        this.apply_snapshot(eviction_queue, snapshot);

        this
    }

    pub(super) fn create_view(&self) -> EthTxPoolBridgeStateView {
        EthTxPoolBridgeStateView {
            status: Arc::clone(&self.status),
            address_hashes: Arc::clone(&self.address_hashes),
        }
    }

    pub(super) fn add_tx(
        &self,
        eviction_queue: &mut EthTxPoolBridgeEvictionQueue,
        tx: &TxEnvelope,
        tx_status_recv_send: TxStatusReceiverSender,
    ) -> bool {
        let hash = *tx.tx_hash();

        match self.status.entry(hash) {
            Entry::Occupied(mut o) => {
                let mut receiver = o.get_mut().subscribe();

                let known = match &*receiver.borrow() {
                    TxStatus::Unknown => false,
                    TxStatus::Tracked
                    | TxStatus::Dropped { .. }
                    | TxStatus::Evicted { .. }
                    | TxStatus::Committed => true,
                };

                if known {
                    receiver.mark_changed();
                }

                let _ = tx_status_recv_send.send(receiver);

                !known
            }
            Entry::Vacant(v) => {
                let (sender, receiver) = tokio::sync::watch::channel(TxStatus::Unknown);

                v.insert(sender);
                eviction_queue.push_back((Instant::now(), hash));

                let _ = tx_status_recv_send.send(receiver);

                true
            }
        }
    }

    pub(super) fn apply_snapshot(
        &self,
        eviction_queue: &mut EthTxPoolBridgeEvictionQueue,
        snapshot: EthTxPoolSnapshot,
    ) {
        let EthTxPoolSnapshot { mut txs } = snapshot;

        let now = Instant::now();

        eviction_queue.clear();

        self.status.retain(|tx_hash, status| {
            if txs.remove(tx_hash) {
                status.send_if_modified(|tx_status| {
                    *tx_status = TxStatus::Tracked;
                    false
                });
                eviction_queue.push_back((now, *tx_hash));
                return true;
            }

            let Some((tx_hash, address)) = self.hash_address.remove(tx_hash) else {
                return false;
            };

            self.address_hashes.entry(address).and_modify(|hashes| {
                hashes.remove(&tx_hash);
            });

            false
        });

        for tx_hash in txs {
            self.status
                .insert(tx_hash, tokio::sync::watch::channel(TxStatus::Tracked).0);
            eviction_queue.push_back((now, tx_hash));
        }

        // note that self.hash_addresses and self.address_hashes aren't populated for snapshots
    }

    pub(super) fn handle_events(
        &self,
        eviction_queue: &mut EthTxPoolBridgeEvictionQueue,
        events: Vec<EthTxPoolEvent>,
    ) {
        let now = Instant::now();

        let mut insert = |tx_hash, tx_status: TxStatus| {
            match self.status.entry(tx_hash) {
                Entry::Occupied(mut o) => {
                    o.get_mut().send_replace(tx_status);
                }
                Entry::Vacant(v) => {
                    v.insert(tokio::sync::watch::channel(tx_status).0);
                    eviction_queue.push_back((now, tx_hash));
                }
            };
        };

        for EthTxPoolEvent { tx_hash, action } in events {
            match action {
                EthTxPoolEventType::Insert {
                    address,
                    owned: _,
                    tx: _,
                } => {
                    insert(tx_hash, TxStatus::Tracked);

                    self.hash_address.entry(tx_hash).insert(address);
                    self.address_hashes
                        .entry(address)
                        .or_default()
                        .insert(tx_hash);
                }
                EthTxPoolEventType::Commit => {
                    insert(tx_hash, TxStatus::Committed);
                }
                EthTxPoolEventType::Drop { reason } => {
                    insert(tx_hash, TxStatus::Dropped { reason });
                }
                EthTxPoolEventType::Evict { reason } => {
                    insert(tx_hash, TxStatus::Evicted { reason });
                }
            }
        }
    }

    pub(super) fn cleanup(&self, eviction_queue: &mut EthTxPoolBridgeEvictionQueue, now: Instant) {
        while eviction_queue
            .front()
            .map(|entry| {
                now.duration_since(entry.0) >= Duration::from_secs(TX_EVICT_DURATION_SECONDS)
            })
            .unwrap_or_default()
        {
            let (_, hash) = eviction_queue.pop_front().unwrap();

            if self.status.remove(&hash).is_none() {
                continue;
            }

            if let Some((hash, address)) = self.hash_address.remove(&hash) {
                if let Some(mut address_hashes) = self.address_hashes.get_mut(&address) {
                    address_hashes.remove(&hash);
                }
            }
        }

        self.address_hashes.retain(|_, hashes| !hashes.is_empty());
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, time::Duration};

    use alloy_consensus::{transaction::SignerRecoverable, TxEnvelope};
    use monad_eth_testutil::{make_legacy_tx, S1};
    use monad_eth_txpool_types::{
        EthTxPoolDropReason, EthTxPoolEvent, EthTxPoolEventType, EthTxPoolEvictReason,
        EthTxPoolSnapshot,
    };
    use tokio::time::Instant;

    use super::EthTxPoolBridgeStateView;
    use crate::txpool::{
        state::{EthTxPoolBridgeEvictionQueue, EthTxPoolBridgeState, TX_EVICT_DURATION_SECONDS},
        TxStatus,
    };

    const BASE_FEE_PER_GAS: u64 = 100_000_000_000;

    fn setup() -> (
        EthTxPoolBridgeState,
        EthTxPoolBridgeStateView,
        EthTxPoolBridgeEvictionQueue,
        TxEnvelope,
    ) {
        let mut eviction_queue = EthTxPoolBridgeEvictionQueue::default();
        let state = EthTxPoolBridgeState::new(
            &mut eviction_queue,
            EthTxPoolSnapshot {
                txs: HashSet::default(),
            },
        );
        let state_view = state.create_view();

        let tx = make_legacy_tx(S1, BASE_FEE_PER_GAS.into(), 100_000, 0, 0);

        (state, state_view, eviction_queue, tx)
    }

    #[tokio::test]
    async fn test_create_view_linked() {
        let (state, state_view, mut eviction_queue, tx) = setup();

        assert_eq!(state.status.len(), 0);
        assert_eq!(state_view.status.len(), 0);

        state.add_tx(&mut eviction_queue, &tx, tokio::sync::oneshot::channel().0);

        assert_eq!(state.status.len(), 1);
        assert_eq!(state_view.status.len(), 1);
    }

    #[tokio::test]
    async fn test_add_tx() {
        let (state, state_view, mut eviction_queue, tx) = setup();

        assert_eq!(state_view.get_status_by_hash(tx.tx_hash()), None);

        state.add_tx(&mut eviction_queue, &tx, tokio::sync::oneshot::channel().0);
        assert_eq!(
            state_view.get_status_by_hash(tx.tx_hash()),
            Some(TxStatus::Unknown)
        );
    }

    #[tokio::test]
    async fn test_add_duplicate_tx() {
        let (state, state_view, mut eviction_queue, tx) = setup();

        assert_eq!(state_view.get_status_by_hash(tx.tx_hash()), None);

        let (tx_status_recv_send0, mut tx_status_recv_recv0) = tokio::sync::oneshot::channel();
        let (tx_status_recv_send1, mut tx_status_recv_recv1) = tokio::sync::oneshot::channel();

        assert!(state.add_tx(&mut eviction_queue, &tx, tx_status_recv_send0));
        assert_eq!(
            state_view.get_status_by_hash(tx.tx_hash()),
            Some(TxStatus::Unknown)
        );

        assert!(state.add_tx(&mut eviction_queue, &tx, tx_status_recv_send1));
        assert_eq!(
            state_view.get_status_by_hash(tx.tx_hash()),
            Some(TxStatus::Unknown)
        );

        let tx_status_recv0 = tx_status_recv_recv0.try_recv().unwrap();
        let tx_status_recv1 = tx_status_recv_recv1.try_recv().unwrap();

        assert!(tx_status_recv0.same_channel(&tx_status_recv1));

        assert!(!tx_status_recv0.has_changed().unwrap());
        assert!(!tx_status_recv1.has_changed().unwrap());

        assert_eq!(tx_status_recv0.borrow().to_owned(), TxStatus::Unknown);
        assert_eq!(tx_status_recv1.borrow().to_owned(), TxStatus::Unknown);

        state.handle_events(
            &mut eviction_queue,
            vec![EthTxPoolEvent {
                tx_hash: *tx.tx_hash(),
                action: EthTxPoolEventType::Commit,
            }],
        );

        assert!(tx_status_recv0.has_changed().unwrap());
        assert!(tx_status_recv1.has_changed().unwrap());

        assert_eq!(tx_status_recv0.borrow().to_owned(), TxStatus::Committed);
        assert_eq!(tx_status_recv1.borrow().to_owned(), TxStatus::Committed);

        let (tx_status_recv_send2, mut tx_status_recv_recv2) = tokio::sync::oneshot::channel();

        assert!(!state.add_tx(&mut eviction_queue, &tx, tx_status_recv_send2));
        assert_eq!(
            state_view.get_status_by_hash(tx.tx_hash()),
            Some(TxStatus::Committed)
        );

        let tx_status_recv2 = tx_status_recv_recv2.try_recv().unwrap();

        assert!(tx_status_recv1.same_channel(&tx_status_recv2));

        assert!(tx_status_recv2.has_changed().unwrap());

        assert_eq!(tx_status_recv2.borrow().to_owned(), TxStatus::Committed);
    }

    #[tokio::test]
    async fn test_snapshot_does_not_update_tx_status_recv() {
        let (state, state_view, mut eviction_queue, tx) = setup();

        assert_eq!(state_view.get_status_by_hash(tx.tx_hash()), None);

        let (tx_status_recv_send, mut tx_status_recv_recv) = tokio::sync::oneshot::channel();
        state.add_tx(&mut eviction_queue, &tx, tx_status_recv_send);

        let tx_status_recv = tx_status_recv_recv.try_recv().unwrap();
        assert!(!tx_status_recv.has_changed().unwrap());

        state.apply_snapshot(
            &mut eviction_queue,
            EthTxPoolSnapshot {
                txs: HashSet::from_iter([*tx.tx_hash()]),
            },
        );
        assert!(!tx_status_recv.has_changed().unwrap());
    }

    #[tokio::test]
    async fn test_handle_events_and_snapshot() {
        enum TestCases {
            EmptySnapshot,
            Insert,
            InsertSnapshot,
            Drop,
            Commit,
            Evict,
        }

        for test in [
            TestCases::EmptySnapshot,
            TestCases::Insert,
            TestCases::InsertSnapshot,
            TestCases::Drop,
            TestCases::Commit,
            TestCases::Evict,
        ] {
            let (state, state_view, mut eviction_queue, tx) = setup();

            state.add_tx(&mut eviction_queue, &tx, tokio::sync::oneshot::channel().0);
            assert_eq!(
                state_view.get_status_by_hash(tx.tx_hash()),
                Some(TxStatus::Unknown)
            );

            match test {
                TestCases::EmptySnapshot => {
                    state.apply_snapshot(
                        &mut eviction_queue,
                        EthTxPoolSnapshot {
                            txs: HashSet::default(),
                        },
                    );
                    assert_eq!(state_view.get_status_by_hash(tx.tx_hash()), None);
                }
                TestCases::Insert => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent {
                            tx_hash: tx.tx_hash().to_owned(),
                            action: EthTxPoolEventType::Insert {
                                address: tx.recover_signer().unwrap(),
                                owned: true,
                                tx: tx.clone(),
                            },
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Tracked)
                    );
                }
                TestCases::InsertSnapshot => {
                    state.apply_snapshot(
                        &mut eviction_queue,
                        EthTxPoolSnapshot {
                            txs: HashSet::from_iter(std::iter::once(tx.tx_hash().to_owned())),
                        },
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Tracked)
                    );
                }
                TestCases::Drop => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent {
                            tx_hash: tx.tx_hash().to_owned(),
                            action: EthTxPoolEventType::Drop {
                                reason: EthTxPoolDropReason::PoolNotReady,
                            },
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Dropped {
                            reason: EthTxPoolDropReason::PoolNotReady
                        })
                    );
                }
                TestCases::Commit => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent {
                            tx_hash: tx.tx_hash().to_owned(),
                            action: EthTxPoolEventType::Commit,
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Committed)
                    );

                    state.apply_snapshot(
                        &mut eviction_queue,
                        EthTxPoolSnapshot {
                            txs: HashSet::default(),
                        },
                    );
                    assert_eq!(state_view.get_status_by_hash(tx.tx_hash()), None);
                }
                TestCases::Evict => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent {
                            tx_hash: tx.tx_hash().to_owned(),
                            action: EthTxPoolEventType::Evict {
                                reason: EthTxPoolEvictReason::Expired,
                            },
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Evicted {
                            reason: EthTxPoolEvictReason::Expired
                        })
                    );

                    state.apply_snapshot(
                        &mut eviction_queue,
                        EthTxPoolSnapshot {
                            txs: HashSet::default(),
                        },
                    );
                    assert_eq!(state_view.get_status_by_hash(tx.tx_hash()), None);
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_cleanup() {
        for add_duplicate_tx in [false, true] {
            let (state, state_view, mut eviction_queue, tx) = setup();

            assert_eq!(eviction_queue.len(), 0);
            assert_eq!(state_view.status.len(), 0);

            state.add_tx(&mut eviction_queue, &tx, tokio::sync::oneshot::channel().0);
            assert_eq!(eviction_queue.len(), 1);
            assert_eq!(state_view.status.len(), 1);

            state.cleanup(&mut eviction_queue, Instant::now());
            assert_eq!(eviction_queue.len(), 1);
            assert_eq!(state_view.status.len(), 1);

            tokio::time::advance(
                Duration::from_secs(TX_EVICT_DURATION_SECONDS)
                    .checked_sub(Duration::from_millis(1))
                    .unwrap(),
            )
            .await;

            state.cleanup(&mut eviction_queue, Instant::now());
            assert_eq!(eviction_queue.len(), 1);
            assert_eq!(state_view.status.len(), 1);

            if add_duplicate_tx {
                state.add_tx(&mut eviction_queue, &tx, tokio::sync::oneshot::channel().0);
                assert_eq!(eviction_queue.len(), 1);
                assert_eq!(state_view.status.len(), 1);

                state.cleanup(&mut eviction_queue, Instant::now());
                assert_eq!(eviction_queue.len(), 1);
                assert_eq!(state_view.status.len(), 1);
            }

            tokio::time::advance(Duration::from_millis(1)).await;

            state.cleanup(&mut eviction_queue, Instant::now());
            assert_eq!(eviction_queue.len(), 0);
            assert_eq!(state_view.status.len(), 0);
        }
    }
}
