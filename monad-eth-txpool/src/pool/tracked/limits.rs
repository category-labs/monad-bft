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
    collections::btree_map::{
        OccupiedEntry as BTreeMapOccupiedEntry, VacantEntry as BTreeMapVacantEntry,
    },
    ops::Deref,
    time::{Duration, Instant},
};

use alloy_primitives::Address;
use indexmap::IndexMap;
use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolInternalDropReason};
use tracing::{error, info};

use super::list::TrackedTxList;
use crate::{pool::transaction::ValidEthTransaction, EthTxPoolEventTracker};

// To produce 5k tx blocks, we need the tracked tx map to hold at least 15k addresses so that, after
// pruning the txpool of up to 5k unique addresses in the last committed block update and up to 5k
// unique addresses in the pending blocktree, the tracked tx map will still have at least 5k other
// addresses with at least one tx each to use when creating the next block.
const MAX_ADDRESSES: usize = 16 * 1024;

const MAX_TXS: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub(super) struct TrackedTxLimitsConfig {
    max_addresses: usize,
    max_txs: usize,

    soft_tx_expiry: Duration,
    hard_tx_expiry: Duration,
}

impl TrackedTxLimitsConfig {
    pub fn new(soft_tx_expiry: Duration, hard_tx_expiry: Duration) -> Self {
        Self {
            max_addresses: MAX_ADDRESSES,
            max_txs: MAX_TXS,

            soft_tx_expiry,
            hard_tx_expiry,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct TrackedTxLimits {
    config: TrackedTxLimitsConfig,

    addresses: usize,
    txs: usize,
}

impl TrackedTxLimits {
    pub fn new(config: TrackedTxLimitsConfig) -> Self {
        Self {
            config,

            addresses: 0,
            txs: 0,
        }
    }

    pub fn build_txs_map_with_capacity(&self) -> IndexMap<Address, TrackedTxList> {
        IndexMap::with_capacity(self.config.max_addresses)
    }

    pub fn tx_expiry_during_insert(&self) -> Duration {
        self.config.hard_tx_expiry
    }

    pub fn tx_expiry_during_evict(&self) -> Duration {
        if 3 * (self.addresses / 4) < self.config.max_addresses {
            self.config.hard_tx_expiry
        } else {
            info!(num_txs =? self.txs, "txpool hit soft evict addresses watermark");

            self.config.soft_tx_expiry
        }
    }

    #[inline]
    fn can_increase_limits(&self, inc_addresses: usize, inc_txs: usize) -> bool {
        let Self {
            config,

            addresses,
            txs,
        } = self;

        if addresses.saturating_add(inc_addresses) > config.max_addresses {
            return false;
        }

        if txs.saturating_add(inc_txs) > config.max_txs {
            return false;
        }

        true
    }

    pub fn prepare_add_tx_to_new_address<'et, 'l, 't>(
        &'l mut self,
        event_tracker: &'et mut EthTxPoolEventTracker<'t>,
        tx: ValidEthTransaction,
    ) -> Option<TrackedTxLimitAddToken<'et, 'l, 't>> {
        if !self.can_increase_limits(1, 1) {
            event_tracker.drop(tx.hash(), EthTxPoolDropReason::PoolFull);
            return None;
        }

        Some(TrackedTxLimitAddToken::new(
            TrackedTxLimitAddTokenKind::NewAddress,
            self,
            event_tracker,
            tx,
        ))
    }

    pub fn prepare_add_tx_to_existing<'et, 'l, 't>(
        &'l mut self,
        event_tracker: &'et mut EthTxPoolEventTracker<'t>,
        tx: ValidEthTransaction,
    ) -> Option<TrackedTxLimitAddToken<'et, 'l, 't>> {
        if !self.can_increase_limits(0, 1) {
            event_tracker.drop(tx.hash(), EthTxPoolDropReason::PoolFull);
            return None;
        }

        Some(TrackedTxLimitAddToken::new(
            TrackedTxLimitAddTokenKind::Existing,
            self,
            event_tracker,
            tx,
        ))
    }

    #[inline]
    fn remove_tx(&mut self, tx: ValidEthTransaction) {
        let Self {
            config: _,

            addresses: _,
            txs,
        } = self;

        *txs = txs.checked_sub(1).unwrap_or_else(|| {
            error!("txpool txs limit underflowed");
            0
        });
    }

    #[inline]
    fn remove_address(&mut self) {
        let Self {
            config: _,

            addresses,
            txs: _,
        } = self;

        *addresses = addresses.checked_sub(1).unwrap_or_else(|| {
            error!("txpool address limit underflowed");
            0
        });
    }

    pub fn remove_committed_txs(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        removed_txs: impl Iterator<Item = ValidEthTransaction>,
        removed_address: bool,
    ) {
        let hashes = removed_txs.map(|tx| {
            let tx_hash = tx.hash();
            self.remove_tx(tx);
            tx_hash
        });

        event_tracker.tracked_commit(removed_address, hashes);

        if removed_address {
            self.remove_address();
        }
    }

    pub fn remove_expired_txs(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        removed_txs: impl Iterator<Item = ValidEthTransaction>,
        removed_address: bool,
    ) {
        let hashes = removed_txs.map(|tx| {
            let tx_hash = tx.hash();
            self.remove_tx(tx);
            tx_hash
        });

        event_tracker.tracked_evict_expired(removed_address, hashes);

        if removed_address {
            self.remove_address();
        }
    }

    pub fn reset(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        txs_map: &mut IndexMap<Address, TrackedTxList>,
    ) {
        let txs_map = std::mem::take(txs_map);

        for (_, txs_list) in txs_map {
            for tx in txs_list.into_txs() {
                let tx_hash = tx.hash();
                self.remove_tx(tx);
                event_tracker.drop(tx_hash, EthTxPoolDropReason::PoolNotReady);
            }

            self.remove_address();
        }
    }
}

pub(super) struct TrackedTxLimitAddToken<'et, 'l, 't> {
    kind: TrackedTxLimitAddTokenKind,

    limits: &'l mut TrackedTxLimits,
    event_tracker: &'et mut EthTxPoolEventTracker<'t>,

    tx: ValidEthTransaction,
    used: bool,
}

enum TrackedTxLimitAddTokenKind {
    NewAddress,
    Existing,
}

impl<'et, 'l, 't> TrackedTxLimitAddToken<'et, 'l, 't> {
    fn new(
        kind: TrackedTxLimitAddTokenKind,
        limits: &'l mut TrackedTxLimits,
        event_tracker: &'et mut EthTxPoolEventTracker<'t>,
        tx: ValidEthTransaction,
    ) -> Self {
        Self {
            kind,

            limits,
            event_tracker,

            tx,
            used: false,
        }
    }

    pub fn limits<'a: 'l>(&'a self) -> &'l TrackedTxLimits {
        self.limits
    }

    pub fn now<'a: 'et>(&'a self) -> &'et Instant {
        &self.event_tracker.now
    }

    pub fn insert_vacant<K>(
        mut self,
        v: BTreeMapVacantEntry<K, (ValidEthTransaction, Instant)>,
    ) -> &ValidEthTransaction
    where
        K: Ord,
    {
        self.apply_limits();

        let tx = &v.insert((self.tx.clone(), self.event_tracker.now)).0;

        self.event_tracker.insert(tx.raw(), tx.is_owned());

        tx
    }

    pub fn replace_existing(
        mut self,
        mut o: BTreeMapOccupiedEntry<u64, (ValidEthTransaction, Instant)>,
    ) -> &ValidEthTransaction {
        self.apply_limits();

        let replaced_tx = o.insert((self.tx.clone(), self.event_tracker.now)).0;
        let tx = &o.into_mut().0;

        self.event_tracker.replace(
            tx.signer_ref(),
            replaced_tx.hash(),
            tx.hash(),
            tx.is_owned(),
        );

        tx
    }

    #[inline]
    fn apply_limits(&mut self) {
        let Self {
            kind,
            limits:
                TrackedTxLimits {
                    config: _,

                    addresses,
                    txs,
                },
            event_tracker: _,
            tx: _,
            used,
        } = self;

        *txs += 1;

        match kind {
            TrackedTxLimitAddTokenKind::NewAddress => {
                *addresses += 1;
            }
            TrackedTxLimitAddTokenKind::Existing => {}
        }

        *used = true;
    }

    pub fn cancel(mut self, reason: EthTxPoolDropReason) {
        let Self {
            kind: _,
            limits: _,
            event_tracker,
            tx,
            used,
        } = &mut self;

        event_tracker.drop(tx.hash(), reason);
        *used = true;
    }
}

impl<'et, 'l, 't> Deref for TrackedTxLimitAddToken<'et, 'l, 't> {
    type Target = ValidEthTransaction;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<'et, 'l, 't> Drop for TrackedTxLimitAddToken<'et, 'l, 't> {
    fn drop(&mut self) {
        if self.used {
            return;
        }

        error!("TrackedTxLimitAddToken dropped without consuming tx");
        self.event_tracker.drop(
            self.tx.hash(),
            EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::LimitError),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use monad_chain_config::{revision::ChainRevision, ChainConfig, MockChainConfig};
    use monad_eth_testutil::{generate_consensus_test_block, make_legacy_tx, recover_tx, S1};
    use monad_tfm::base_fee::MIN_BASE_FEE;
    use monad_types::{GENESIS_ROUND, GENESIS_SEQ_NUM};

    use super::{TrackedTxLimits, TrackedTxLimitsConfig};
    use crate::{pool::transaction::ValidEthTransaction, EthTxPoolEventTracker, EthTxPoolMetrics};

    fn make_test_tx() -> ValidEthTransaction {
        let tx = recover_tx(make_legacy_tx(S1, MIN_BASE_FEE.into(), 1_000_000, 0, 0));

        let last_commit = generate_consensus_test_block(
            GENESIS_ROUND,
            GENESIS_SEQ_NUM,
            MIN_BASE_FEE,
            &MockChainConfig::DEFAULT,
            vec![],
        );

        ValidEthTransaction::validate(
            last_commit.block.header(),
            MockChainConfig::DEFAULT.chain_id(),
            MockChainConfig::DEFAULT
                .get_chain_revision(GENESIS_ROUND)
                .chain_params(),
            MockChainConfig::DEFAULT
                .get_execution_chain_revision(0)
                .execution_chain_params(),
            tx,
            true,
        )
        .unwrap()
    }

    #[test]
    fn basic_exceed_limits() {
        let tx = make_test_tx();

        let mut limits = TrackedTxLimits::new(TrackedTxLimitsConfig {
            max_addresses: 1,
            max_txs: 2,
            soft_tx_expiry: Duration::from_secs(1),
            hard_tx_expiry: Duration::from_secs(5),
        });

        let metrics = EthTxPoolMetrics::default();
        let mut events = BTreeMap::default();
        let mut event_tracker = EthTxPoolEventTracker::new(&metrics, &mut events);

        limits
            .prepare_add_tx_to_new_address(&mut event_tracker, tx.clone())
            .expect("Can add new address and tx")
            .apply_limits();

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 1);

        assert!(
            limits
                .prepare_add_tx_to_new_address(&mut event_tracker, tx.clone())
                .is_none(),
            "Exceeds max address limit"
        );

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 1);

        limits
            .prepare_add_tx_to_existing(&mut event_tracker, tx.clone())
            .expect("Can add another tx because max_txs == 2")
            .apply_limits();

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 2);

        assert!(
            limits
                .prepare_add_tx_to_existing(&mut event_tracker, tx)
                .is_none(),
            "Exceeds max txs limit"
        );

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 2);
    }

    #[test]
    fn new_address() {
        let tx = make_test_tx();

        for max_addresses in [0, 1, 10, 100] {
            let mut limits = TrackedTxLimits::new(TrackedTxLimitsConfig {
                max_addresses,
                max_txs: max_addresses + 1,
                soft_tx_expiry: Duration::from_secs(1),
                hard_tx_expiry: Duration::from_secs(5),
            });

            let metrics = EthTxPoolMetrics::default();
            let mut events = BTreeMap::default();
            let mut event_tracker = EthTxPoolEventTracker::new(&metrics, &mut events);

            for _ in 0..max_addresses {
                limits
                    .prepare_add_tx_to_new_address(&mut event_tracker, tx.clone())
                    .expect("Can add new address and tx")
                    .apply_limits();
            }

            assert_eq!(limits.addresses, max_addresses);
            assert_eq!(limits.txs, max_addresses);

            assert!(
                limits
                    .prepare_add_tx_to_new_address(&mut event_tracker, tx.clone())
                    .is_none(),
                "Cannot add another address because max_addresses hit"
            );

            limits
                .prepare_add_tx_to_existing(&mut event_tracker, tx.clone())
                .expect("Can add one more tx")
                .apply_limits();

            assert_eq!(limits.addresses, max_addresses);
            assert_eq!(limits.txs, max_addresses + 1);
        }
    }

    #[test]
    fn existing() {
        let tx = make_test_tx();

        for max_txs in [0, 1, 10, 100] {
            let mut limits = TrackedTxLimits::new(TrackedTxLimitsConfig {
                // This use-case is not permitted by the pool but can be used for testing just txs
                max_addresses: 0,
                max_txs,
                soft_tx_expiry: Duration::from_secs(1),
                hard_tx_expiry: Duration::from_secs(5),
            });

            let metrics = EthTxPoolMetrics::default();
            let mut events = BTreeMap::default();
            let mut event_tracker = EthTxPoolEventTracker::new(&metrics, &mut events);

            for _ in 0..max_txs {
                limits
                    .prepare_add_tx_to_existing(&mut event_tracker, tx.clone())
                    .expect("Can add new tx")
                    .apply_limits();
            }

            assert_eq!(limits.addresses, 0);
            assert_eq!(limits.txs, max_txs);

            assert!(
                limits
                    .prepare_add_tx_to_existing(&mut event_tracker, tx.clone())
                    .is_none(),
                "Cannot add another address because max_addresses and max_txs hit"
            );

            assert!(
                limits
                    .prepare_add_tx_to_new_address(&mut event_tracker, tx.clone())
                    .is_none(),
                "Cannot add another address because max_addresses and max_txs hit"
            );

            assert_eq!(limits.addresses, 0);
            assert_eq!(limits.txs, max_txs);
        }
    }

    #[test]
    fn limits_unchanged_until_finalized() {
        let tx = make_test_tx();

        let mut limits = TrackedTxLimits::new(TrackedTxLimitsConfig {
            max_addresses: 2,
            max_txs: 2,
            soft_tx_expiry: Duration::from_secs(1),
            hard_tx_expiry: Duration::from_secs(5),
        });

        let metrics = EthTxPoolMetrics::default();
        let mut events = BTreeMap::default();
        let mut event_tracker = EthTxPoolEventTracker::new(&metrics, &mut events);

        // New address

        limits
            .prepare_add_tx_to_new_address(&mut event_tracker, tx.clone())
            .expect("Can add new address and tx");

        assert_eq!(limits.addresses, 0);
        assert_eq!(limits.txs, 0);

        limits
            .prepare_add_tx_to_new_address(&mut event_tracker, tx.clone())
            .expect("Can add new address and tx")
            .apply_limits();

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 1);

        // Existing

        limits
            .prepare_add_tx_to_existing(&mut event_tracker, tx.clone())
            .expect("Can add new tx");

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 1);

        limits
            .prepare_add_tx_to_existing(&mut event_tracker, tx)
            .expect("Can add new tx")
            .apply_limits();

        assert_eq!(limits.addresses, 1);
        assert_eq!(limits.txs, 2);
    }
}
