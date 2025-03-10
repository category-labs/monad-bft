use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use alloy_consensus::TxEnvelope;
use alloy_primitives::{Address, TxHash};
use dashmap::{DashMap, Entry};
use monad_eth_txpool_types::{EthTxPoolEvent, EthTxPoolSnapshot};
use tokio::time::Instant;

use super::TxStatus;

const TX_EVICT_DURATION_SECONDS: u64 = 15 * 60;

pub(super) type EthTxPoolBridgeEvictionQueue = VecDeque<(Instant, TxHash)>;

#[derive(Clone)]
pub struct EthTxPoolBridgeStateView {
    status: Arc<DashMap<TxHash, TxStatus>>,
    hash_address: Arc<DashMap<TxHash, Address>>,
    address_hashes: Arc<DashMap<Address, HashSet<TxHash>>>,
}

impl EthTxPoolBridgeStateView {
    pub fn get_status_by_hash(&self, hash: &TxHash) -> Option<TxStatus> {
        Some(self.status.get(hash)?.value().to_owned())
    }

    pub(super) fn get_status_by_address(
        &self,
        address: &Address,
    ) -> Option<HashMap<TxHash, TxStatus>> {
        let hashes = self.address_hashes.get(address)?.value().to_owned();

        let statuses = hashes
            .into_iter()
            .flat_map(|hash| {
                let status = self.status.get(&hash)?.value().to_owned();
                Some((hash, status))
            })
            .collect();

        Some(statuses)
    }
}

#[cfg(test)]
impl EthTxPoolBridgeStateView {
    pub fn for_testing() -> Self {
        Self {
            status: Default::default(),
            hash_address: Default::default(),
            address_hashes: Default::default(),
        }
    }
}

pub struct EthTxPoolBridgeState {
    status: Arc<DashMap<TxHash, TxStatus>>,
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
            hash_address: Arc::clone(&self.hash_address),
            address_hashes: Arc::clone(&self.address_hashes),
        }
    }

    pub(super) fn add_tx(
        &self,
        eviction_queue: &mut EthTxPoolBridgeEvictionQueue,
        tx: &TxEnvelope,
    ) {
        let hash = tx.tx_hash();
        self.status.entry(*hash).insert(TxStatus::Unknown);
        eviction_queue.push_back((Instant::now(), *hash));
    }

    pub(super) fn apply_snapshot(
        &self,
        eviction_queue: &mut EthTxPoolBridgeEvictionQueue,
        snapshot: EthTxPoolSnapshot,
    ) {
        let EthTxPoolSnapshot {
            mut pending,
            mut tracked,
        } = snapshot;

        let now = Instant::now();

        while eviction_queue.pop_front().is_some() {}

        self.status.retain(|tx_hash, status| {
            if pending.remove(tx_hash) {
                *status = TxStatus::Pending;
                eviction_queue.push_back((now, *tx_hash));
                return true;
            }

            if tracked.remove(tx_hash) {
                *status = TxStatus::Tracked;
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

        for tx_hash in pending {
            self.status.insert(tx_hash, TxStatus::Pending);
            eviction_queue.push_back((now, tx_hash));
        }

        for tx_hash in tracked {
            self.status.insert(tx_hash, TxStatus::Tracked);
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

        let mut insert = |tx_hash, status| {
            match self.status.entry(tx_hash) {
                Entry::Occupied(mut o) => {
                    o.insert(status);
                }
                Entry::Vacant(v) => {
                    v.insert(status);
                    eviction_queue.push_back((now, tx_hash));
                }
            };
        };

        for event in events {
            match event {
                EthTxPoolEvent::Insert {
                    tx_hash,
                    address,
                    owned: _,
                    tracked,
                } => {
                    insert(
                        tx_hash,
                        if tracked {
                            TxStatus::Tracked
                        } else {
                            TxStatus::Pending
                        },
                    );

                    self.hash_address.entry(tx_hash).insert(address);
                    self.address_hashes
                        .entry(address)
                        .or_default()
                        .insert(tx_hash);
                }
                EthTxPoolEvent::Replace {
                    old_tx_hash,
                    new_tx_hash,
                    new_owned: _,
                    tracked,
                } => {
                    insert(old_tx_hash, TxStatus::Replaced);
                    insert(
                        new_tx_hash,
                        if tracked {
                            TxStatus::Tracked
                        } else {
                            TxStatus::Pending
                        },
                    );
                }
                EthTxPoolEvent::Drop { tx_hash, reason } => {
                    insert(tx_hash, TxStatus::Dropped { reason });
                }
                EthTxPoolEvent::Promoted { tx_hash } => {
                    insert(tx_hash, TxStatus::Tracked);
                }
                EthTxPoolEvent::Commit { tx_hash } => {
                    insert(tx_hash, TxStatus::Committed);
                }
                EthTxPoolEvent::Evict { tx_hash, reason } => {
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

    use alloy_consensus::TxEnvelope;
    use alloy_primitives::{hex, B256};
    use monad_eth_testutil::make_legacy_tx;
    use monad_eth_txpool_types::{
        EthTxPoolDropReason, EthTxPoolEvent, EthTxPoolEvictReason, EthTxPoolSnapshot,
    };
    use monad_eth_types::BASE_FEE_PER_GAS;
    use tokio::time::Instant;

    use super::EthTxPoolBridgeStateView;
    use crate::txpool::{
        state::{EthTxPoolBridgeEvictionQueue, EthTxPoolBridgeState, TX_EVICT_DURATION_SECONDS},
        TxStatus,
    };

    // pubkey starts with AAA
    const S1: B256 = B256::new(hex!(
        "0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad"
    ));

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
                pending: HashSet::default(),
                tracked: HashSet::default(),
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

        state.add_tx(&mut eviction_queue, &tx);

        assert_eq!(state.status.len(), 1);
        assert_eq!(state_view.status.len(), 1);
    }

    #[tokio::test]
    async fn test_add_tx() {
        let (state, state_view, mut eviction_queue, tx) = setup();

        assert_eq!(state_view.get_status_by_hash(tx.tx_hash()), None);

        state.add_tx(&mut eviction_queue, &tx);
        assert_eq!(
            state_view.get_status_by_hash(tx.tx_hash()),
            Some(TxStatus::Unknown)
        );
    }

    #[tokio::test]
    async fn test_handle_events_and_snapshot() {
        enum TestCases {
            EmptySnapshot,
            InsertPending,
            InsertPendingSnapshot,
            InsertTracked,
            InsertTrackedSnapshot,
            Replace,
            Drop,
            Promote,
            PromoteSnapshot,
            DemoteSnapshot,
            Commit,
            Evict,
        }

        for test in [
            TestCases::EmptySnapshot,
            TestCases::InsertPending,
            TestCases::InsertPendingSnapshot,
            TestCases::InsertTracked,
            TestCases::InsertTrackedSnapshot,
            TestCases::Replace,
            TestCases::Drop,
            TestCases::Promote,
            TestCases::PromoteSnapshot,
            TestCases::DemoteSnapshot,
            TestCases::Commit,
            TestCases::Evict,
        ] {
            let (state, state_view, mut eviction_queue, tx) = setup();

            state.add_tx(&mut eviction_queue, &tx);
            assert_eq!(
                state_view.get_status_by_hash(tx.tx_hash()),
                Some(TxStatus::Unknown)
            );

            match test {
                TestCases::EmptySnapshot => {
                    state.apply_snapshot(
                        &mut eviction_queue,
                        EthTxPoolSnapshot {
                            pending: HashSet::default(),
                            tracked: HashSet::default(),
                        },
                    );
                    assert_eq!(state_view.get_status_by_hash(tx.tx_hash()), None);
                }
                TestCases::InsertPending => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent::Insert {
                            tx_hash: tx.tx_hash().to_owned(),
                            address: tx.recover_signer().unwrap(),
                            owned: true,
                            tracked: false,
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Pending)
                    );
                }
                TestCases::InsertPendingSnapshot => {
                    state.apply_snapshot(
                        &mut eviction_queue,
                        EthTxPoolSnapshot {
                            pending: HashSet::from_iter(std::iter::once(tx.tx_hash().to_owned())),
                            tracked: HashSet::default(),
                        },
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Pending)
                    );
                }
                TestCases::InsertTracked => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent::Insert {
                            tx_hash: tx.tx_hash().to_owned(),
                            address: tx.recover_signer().unwrap(),
                            owned: true,
                            tracked: true,
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Tracked)
                    );
                }
                TestCases::InsertTrackedSnapshot => {
                    state.apply_snapshot(
                        &mut eviction_queue,
                        EthTxPoolSnapshot {
                            pending: HashSet::default(),
                            tracked: HashSet::from_iter(std::iter::once(tx.tx_hash().to_owned())),
                        },
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Tracked)
                    );
                }
                TestCases::Replace => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent::Insert {
                            tx_hash: tx.tx_hash().to_owned(),
                            address: tx.recover_signer().unwrap(),
                            owned: true,
                            tracked: false,
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Pending)
                    );

                    let new_tx = make_legacy_tx(
                        S1,
                        2u128 * Into::<u128>::into(BASE_FEE_PER_GAS),
                        100_000,
                        0,
                        0,
                    );

                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent::Replace {
                            old_tx_hash: tx.tx_hash().to_owned(),
                            new_tx_hash: new_tx.tx_hash().to_owned(),
                            new_owned: true,
                            tracked: true,
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Replaced)
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(new_tx.tx_hash()),
                        Some(TxStatus::Tracked)
                    );
                }
                TestCases::Drop => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent::Drop {
                            tx_hash: tx.tx_hash().to_owned(),
                            reason: EthTxPoolDropReason::PoolNotReady,
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Dropped {
                            reason: EthTxPoolDropReason::PoolNotReady
                        })
                    );
                }
                TestCases::Promote => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent::Insert {
                            tx_hash: tx.tx_hash().to_owned(),
                            address: tx.recover_signer().unwrap(),
                            owned: true,
                            tracked: false,
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Pending)
                    );

                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent::Promoted {
                            tx_hash: tx.tx_hash().to_owned(),
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Tracked)
                    );
                }
                TestCases::PromoteSnapshot => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent::Insert {
                            tx_hash: tx.tx_hash().to_owned(),
                            address: tx.recover_signer().unwrap(),
                            owned: true,
                            tracked: false,
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Pending)
                    );

                    state.apply_snapshot(
                        &mut eviction_queue,
                        EthTxPoolSnapshot {
                            pending: HashSet::default(),
                            tracked: HashSet::from_iter(std::iter::once(tx.tx_hash().to_owned())),
                        },
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Tracked)
                    );
                }
                TestCases::DemoteSnapshot => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent::Insert {
                            tx_hash: tx.tx_hash().to_owned(),
                            address: tx.recover_signer().unwrap(),
                            owned: true,
                            tracked: true,
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Tracked)
                    );

                    state.apply_snapshot(
                        &mut eviction_queue,
                        EthTxPoolSnapshot {
                            pending: HashSet::from_iter(std::iter::once(tx.tx_hash().to_owned())),
                            tracked: HashSet::default(),
                        },
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Pending)
                    );
                }
                TestCases::Commit => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent::Commit {
                            tx_hash: tx.tx_hash().to_owned(),
                        }],
                    );
                    assert_eq!(
                        state_view.get_status_by_hash(tx.tx_hash()),
                        Some(TxStatus::Committed)
                    );

                    state.apply_snapshot(
                        &mut eviction_queue,
                        EthTxPoolSnapshot {
                            pending: HashSet::default(),
                            tracked: HashSet::default(),
                        },
                    );
                    assert_eq!(state_view.get_status_by_hash(tx.tx_hash()), None);
                }
                TestCases::Evict => {
                    state.handle_events(
                        &mut eviction_queue,
                        vec![EthTxPoolEvent::Evict {
                            tx_hash: tx.tx_hash().to_owned(),
                            reason: EthTxPoolEvictReason::Expired,
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
                            pending: HashSet::default(),
                            tracked: HashSet::default(),
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

            state.add_tx(&mut eviction_queue, &tx);
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
                state.add_tx(&mut eviction_queue, &tx);
                assert_eq!(eviction_queue.len(), 2);
                assert_eq!(state_view.status.len(), 1);

                state.cleanup(&mut eviction_queue, Instant::now());
                assert_eq!(eviction_queue.len(), 2);
                assert_eq!(state_view.status.len(), 1);
            }

            tokio::time::advance(Duration::from_millis(1)).await;

            state.cleanup(&mut eviction_queue, Instant::now());
            assert_eq!(eviction_queue.len(), if add_duplicate_tx { 1 } else { 0 });
            assert_eq!(state_view.status.len(), 0);

            if add_duplicate_tx {
                tokio::time::advance(
                    Duration::from_secs(TX_EVICT_DURATION_SECONDS)
                        .checked_sub(Duration::from_millis(1))
                        .unwrap(),
                )
                .await;

                state.cleanup(&mut eviction_queue, Instant::now());
                assert_eq!(eviction_queue.len(), 0);
                assert_eq!(state_view.status.len(), 0);
            }
        }
    }
}
