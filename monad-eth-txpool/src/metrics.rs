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

use monad_executor::{ExecutorMetricHandle, ExecutorMetrics};
use serde::{Deserialize, Serialize};

monad_executor::metric_consts! {
    POOL_INSERT_OWNED_TXS {
        name: "monad.bft.txpool.pool.insert_owned_txs",
        help: "Owned transactions inserted into the pool",
    }
    POOL_INSERT_FORWARDED_TXS {
        name: "monad.bft.txpool.pool.insert_forwarded_txs",
        help: "Forwarded transactions inserted into the pool",
    }
    POOL_DROP_NOT_WELL_FORMED {
        name: "monad.bft.txpool.pool.drop_not_well_formed",
        help: "Transactions dropped due to malformed data",
    }
    POOL_DROP_INVALID_SIGNATURE {
        name: "monad.bft.txpool.pool.drop_invalid_signature",
        help: "Transactions dropped due to invalid signature",
    }
    POOL_DROP_NONCE_TOO_LOW {
        name: "monad.bft.txpool.pool.drop_nonce_too_low",
        help: "Transactions dropped due to nonce too low",
    }
    POOL_DROP_FEE_TOO_LOW {
        name: "monad.bft.txpool.pool.drop_fee_too_low",
        help: "Transactions dropped due to fee too low",
    }
    POOL_DROP_INSUFFICIENT_BALANCE {
        name: "monad.bft.txpool.pool.drop_insufficient_balance",
        help: "Transactions dropped due to insufficient balance",
    }
    POOL_DROP_EXISTING_HIGHER_PRIORITY {
        name: "monad.bft.txpool.pool.drop_existing_higher_priority",
        help: "Transactions dropped - existing tx has higher priority",
    }
    POOL_DROP_REPLACED_BY_HIGHER_PRIORITY {
        name: "monad.bft.txpool.pool.drop_replaced_by_higher_priority",
        help: "Transactions replaced by higher priority",
    }
    POOL_DROP_POOL_FULL {
        name: "monad.bft.txpool.pool.drop_pool_full",
        help: "Transactions dropped because pool is full",
    }
    POOL_DROP_POOL_NOT_READY {
        name: "monad.bft.txpool.pool.drop_pool_not_ready",
        help: "Transactions dropped because pool is not ready",
    }
    POOL_DROP_INTERNAL_STATE_BACKEND_ERROR {
        name: "monad.bft.txpool.pool.drop_internal_state_backend_error",
        help: "Transactions dropped due to backend error",
    }
    POOL_DROP_INTERNAL_NOT_READY {
        name: "monad.bft.txpool.pool.drop_internal_not_ready",
        help: "Transactions dropped due to internal not ready",
    }
    POOL_CREATE_PROPOSAL {
        name: "monad.bft.txpool.pool.create_proposal",
        help: "Proposals created from txpool",
    }
    POOL_CREATE_PROPOSAL_TXS {
        name: "monad.bft.txpool.pool.create_proposal_txs",
        help: "Transactions included in proposals",
    }
    POOL_CREATE_PROPOSAL_TRACKED_ADDRESSES {
        name: "monad.bft.txpool.pool.create_proposal_tracked_addresses",
        help: "Tracked addresses during proposal creation",
    }
    POOL_CREATE_PROPOSAL_AVAILABLE_ADDRESSES {
        name: "monad.bft.txpool.pool.create_proposal_available_addresses",
        help: "Available addresses during proposal creation",
    }
    POOL_CREATE_PROPOSAL_BACKEND_LOOKUPS {
        name: "monad.bft.txpool.pool.create_proposal_backend_lookups",
        help: "Backend lookups during proposal creation",
    }
    TRACKED_ADDRESSES {
        name: "monad.bft.txpool.pool.tracked.addresses",
        help: "Addresses being tracked in the pool",
    }
    TRACKED_TXS {
        name: "monad.bft.txpool.pool.tracked.txs",
        help: "Transactions being tracked in the pool",
    }
    TRACKED_EVICT_EXPIRED_ADDRESSES {
        name: "monad.bft.txpool.pool.tracked.evict_expired_addresses",
        help: "Addresses evicted due to expiration",
    }
    TRACKED_EVICT_EXPIRED_TXS {
        name: "monad.bft.txpool.pool.tracked.evict_expired_txs",
        help: "Transactions evicted due to expiration",
    }
    TRACKED_REMOVE_COMMITTED_ADDRESSES {
        name: "monad.bft.txpool.pool.tracked.remove_committed_addresses",
        help: "Addresses removed after commitment",
    }
    TRACKED_REMOVE_COMMITTED_TXS {
        name: "monad.bft.txpool.pool.tracked.remove_committed_txs",
        help: "Transactions removed after commitment",
    }
}

pub fn init_executor_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs([
        POOL_INSERT_OWNED_TXS,
        POOL_INSERT_FORWARDED_TXS,
        POOL_DROP_NOT_WELL_FORMED,
        POOL_DROP_INVALID_SIGNATURE,
        POOL_DROP_NONCE_TOO_LOW,
        POOL_DROP_FEE_TOO_LOW,
        POOL_DROP_INSUFFICIENT_BALANCE,
        POOL_DROP_EXISTING_HIGHER_PRIORITY,
        POOL_DROP_REPLACED_BY_HIGHER_PRIORITY,
        POOL_DROP_POOL_FULL,
        POOL_DROP_POOL_NOT_READY,
        POOL_DROP_INTERNAL_STATE_BACKEND_ERROR,
        POOL_DROP_INTERNAL_NOT_READY,
        POOL_CREATE_PROPOSAL,
        POOL_CREATE_PROPOSAL_TXS,
        POOL_CREATE_PROPOSAL_TRACKED_ADDRESSES,
        POOL_CREATE_PROPOSAL_AVAILABLE_ADDRESSES,
        POOL_CREATE_PROPOSAL_BACKEND_LOOKUPS,
        TRACKED_ADDRESSES,
        TRACKED_TXS,
        TRACKED_EVICT_EXPIRED_ADDRESSES,
        TRACKED_EVICT_EXPIRED_TXS,
        TRACKED_REMOVE_COMMITTED_ADDRESSES,
        TRACKED_REMOVE_COMMITTED_TXS,
    ])
}

#[derive(Debug)]
pub struct EthTxPoolMetrics {
    pub insert_owned_txs: ExecutorMetricHandle,
    pub insert_forwarded_txs: ExecutorMetricHandle,

    pub drop_not_well_formed: ExecutorMetricHandle,
    pub drop_invalid_signature: ExecutorMetricHandle,
    pub drop_nonce_too_low: ExecutorMetricHandle,
    pub drop_fee_too_low: ExecutorMetricHandle,
    pub drop_insufficient_balance: ExecutorMetricHandle,
    pub drop_existing_higher_priority: ExecutorMetricHandle,
    pub drop_replaced_by_higher_priority: ExecutorMetricHandle,
    pub drop_pool_full: ExecutorMetricHandle,
    pub drop_pool_not_ready: ExecutorMetricHandle,
    pub drop_internal_state_backend_error: ExecutorMetricHandle,
    pub drop_internal_not_ready: ExecutorMetricHandle,

    pub create_proposal: ExecutorMetricHandle,
    pub create_proposal_txs: ExecutorMetricHandle,
    pub create_proposal_tracked_addresses: ExecutorMetricHandle,
    pub create_proposal_available_addresses: ExecutorMetricHandle,
    pub create_proposal_backend_lookups: ExecutorMetricHandle,

    pub tracked: EthTxPoolTrackedMetrics,
}

impl EthTxPoolMetrics {
    pub fn from_executor_metrics(executor_metrics: &ExecutorMetrics) -> Self {
        Self {
            insert_owned_txs: executor_metrics.handle(POOL_INSERT_OWNED_TXS),
            insert_forwarded_txs: executor_metrics.handle(POOL_INSERT_FORWARDED_TXS),

            drop_not_well_formed: executor_metrics.handle(POOL_DROP_NOT_WELL_FORMED),
            drop_invalid_signature: executor_metrics.handle(POOL_DROP_INVALID_SIGNATURE),
            drop_nonce_too_low: executor_metrics.handle(POOL_DROP_NONCE_TOO_LOW),
            drop_fee_too_low: executor_metrics.handle(POOL_DROP_FEE_TOO_LOW),
            drop_insufficient_balance: executor_metrics.handle(POOL_DROP_INSUFFICIENT_BALANCE),
            drop_existing_higher_priority: executor_metrics
                .handle(POOL_DROP_EXISTING_HIGHER_PRIORITY),
            drop_replaced_by_higher_priority: executor_metrics
                .handle(POOL_DROP_REPLACED_BY_HIGHER_PRIORITY),
            drop_pool_full: executor_metrics.handle(POOL_DROP_POOL_FULL),
            drop_pool_not_ready: executor_metrics.handle(POOL_DROP_POOL_NOT_READY),
            drop_internal_state_backend_error: executor_metrics
                .handle(POOL_DROP_INTERNAL_STATE_BACKEND_ERROR),
            drop_internal_not_ready: executor_metrics.handle(POOL_DROP_INTERNAL_NOT_READY),

            create_proposal: executor_metrics.handle(POOL_CREATE_PROPOSAL),
            create_proposal_txs: executor_metrics.handle(POOL_CREATE_PROPOSAL_TXS),
            create_proposal_tracked_addresses: executor_metrics
                .handle(POOL_CREATE_PROPOSAL_TRACKED_ADDRESSES),
            create_proposal_available_addresses: executor_metrics
                .handle(POOL_CREATE_PROPOSAL_AVAILABLE_ADDRESSES),
            create_proposal_backend_lookups: executor_metrics
                .handle(POOL_CREATE_PROPOSAL_BACKEND_LOOKUPS),

            tracked: EthTxPoolTrackedMetrics::from_executor_metrics(executor_metrics),
        }
    }

    pub fn update(&self, metrics: &mut ExecutorMetrics) {
        metrics.set(POOL_INSERT_OWNED_TXS, self.insert_owned_txs.get());
        metrics.set(POOL_INSERT_FORWARDED_TXS, self.insert_forwarded_txs.get());

        metrics.set(POOL_DROP_NOT_WELL_FORMED, self.drop_not_well_formed.get());
        metrics.set(
            POOL_DROP_INVALID_SIGNATURE,
            self.drop_invalid_signature.get(),
        );
        metrics.set(POOL_DROP_NONCE_TOO_LOW, self.drop_nonce_too_low.get());
        metrics.set(POOL_DROP_FEE_TOO_LOW, self.drop_fee_too_low.get());
        metrics.set(
            POOL_DROP_INSUFFICIENT_BALANCE,
            self.drop_insufficient_balance.get(),
        );
        metrics.set(
            POOL_DROP_EXISTING_HIGHER_PRIORITY,
            self.drop_existing_higher_priority.get(),
        );
        metrics.set(
            POOL_DROP_REPLACED_BY_HIGHER_PRIORITY,
            self.drop_replaced_by_higher_priority.get(),
        );
        metrics.set(POOL_DROP_POOL_FULL, self.drop_pool_full.get());
        metrics.set(POOL_DROP_POOL_NOT_READY, self.drop_pool_not_ready.get());
        metrics.set(
            POOL_DROP_INTERNAL_STATE_BACKEND_ERROR,
            self.drop_internal_state_backend_error.get(),
        );
        metrics.set(
            POOL_DROP_INTERNAL_NOT_READY,
            self.drop_internal_not_ready.get(),
        );

        metrics.set(POOL_CREATE_PROPOSAL, self.create_proposal.get());
        metrics.set(POOL_CREATE_PROPOSAL_TXS, self.create_proposal_txs.get());
        metrics.set(
            POOL_CREATE_PROPOSAL_TRACKED_ADDRESSES,
            self.create_proposal_tracked_addresses.get(),
        );
        metrics.set(
            POOL_CREATE_PROPOSAL_AVAILABLE_ADDRESSES,
            self.create_proposal_available_addresses.get(),
        );
        metrics.set(
            POOL_CREATE_PROPOSAL_BACKEND_LOOKUPS,
            self.create_proposal_backend_lookups.get(),
        );

        self.tracked.update(metrics);
    }
}

impl Default for EthTxPoolMetrics {
    fn default() -> Self {
        let executor_metrics = init_executor_metrics();
        Self::from_executor_metrics(&executor_metrics)
    }
}

#[derive(Debug)]
pub struct EthTxPoolTrackedMetrics {
    pub addresses: ExecutorMetricHandle,
    pub txs: ExecutorMetricHandle,
    pub evict_expired_addresses: ExecutorMetricHandle,
    pub evict_expired_txs: ExecutorMetricHandle,
    pub remove_committed_addresses: ExecutorMetricHandle,
    pub remove_committed_txs: ExecutorMetricHandle,
}

impl EthTxPoolTrackedMetrics {
    pub fn from_executor_metrics(executor_metrics: &ExecutorMetrics) -> Self {
        Self {
            addresses: executor_metrics.handle(TRACKED_ADDRESSES),
            txs: executor_metrics.handle(TRACKED_TXS),
            evict_expired_addresses: executor_metrics.handle(TRACKED_EVICT_EXPIRED_ADDRESSES),
            evict_expired_txs: executor_metrics.handle(TRACKED_EVICT_EXPIRED_TXS),
            remove_committed_addresses: executor_metrics.handle(TRACKED_REMOVE_COMMITTED_ADDRESSES),
            remove_committed_txs: executor_metrics.handle(TRACKED_REMOVE_COMMITTED_TXS),
        }
    }

    pub fn update(&self, metrics: &mut ExecutorMetrics) {
        metrics.set(TRACKED_ADDRESSES, self.addresses.get());
        metrics.set(TRACKED_TXS, self.txs.get());
        metrics.set(
            TRACKED_EVICT_EXPIRED_ADDRESSES,
            self.evict_expired_addresses.get(),
        );
        metrics.set(TRACKED_EVICT_EXPIRED_TXS, self.evict_expired_txs.get());
        metrics.set(
            TRACKED_REMOVE_COMMITTED_ADDRESSES,
            self.remove_committed_addresses.get(),
        );
        metrics.set(
            TRACKED_REMOVE_COMMITTED_TXS,
            self.remove_committed_txs.get(),
        );
    }
}

impl Default for EthTxPoolTrackedMetrics {
    fn default() -> Self {
        let executor_metrics = init_executor_metrics();
        Self::from_executor_metrics(&executor_metrics)
    }
}

#[derive(Serialize, Deserialize)]
struct EthTxPoolMetricsSnapshot {
    insert_owned_txs: u64,
    insert_forwarded_txs: u64,

    drop_not_well_formed: u64,
    drop_invalid_signature: u64,
    drop_nonce_too_low: u64,
    drop_fee_too_low: u64,
    drop_insufficient_balance: u64,
    drop_existing_higher_priority: u64,
    drop_replaced_by_higher_priority: u64,
    drop_pool_full: u64,
    drop_pool_not_ready: u64,
    drop_internal_state_backend_error: u64,
    drop_internal_not_ready: u64,

    create_proposal: u64,
    create_proposal_txs: u64,
    create_proposal_tracked_addresses: u64,
    create_proposal_available_addresses: u64,
    create_proposal_backend_lookups: u64,

    tracked: EthTxPoolTrackedMetricsSnapshot,
}

impl From<&EthTxPoolMetrics> for EthTxPoolMetricsSnapshot {
    fn from(metrics: &EthTxPoolMetrics) -> Self {
        Self {
            insert_owned_txs: metrics.insert_owned_txs.get(),
            insert_forwarded_txs: metrics.insert_forwarded_txs.get(),

            drop_not_well_formed: metrics.drop_not_well_formed.get(),
            drop_invalid_signature: metrics.drop_invalid_signature.get(),
            drop_nonce_too_low: metrics.drop_nonce_too_low.get(),
            drop_fee_too_low: metrics.drop_fee_too_low.get(),
            drop_insufficient_balance: metrics.drop_insufficient_balance.get(),
            drop_existing_higher_priority: metrics.drop_existing_higher_priority.get(),
            drop_replaced_by_higher_priority: metrics.drop_replaced_by_higher_priority.get(),
            drop_pool_full: metrics.drop_pool_full.get(),
            drop_pool_not_ready: metrics.drop_pool_not_ready.get(),
            drop_internal_state_backend_error: metrics.drop_internal_state_backend_error.get(),
            drop_internal_not_ready: metrics.drop_internal_not_ready.get(),

            create_proposal: metrics.create_proposal.get(),
            create_proposal_txs: metrics.create_proposal_txs.get(),
            create_proposal_tracked_addresses: metrics.create_proposal_tracked_addresses.get(),
            create_proposal_available_addresses: metrics.create_proposal_available_addresses.get(),
            create_proposal_backend_lookups: metrics.create_proposal_backend_lookups.get(),

            tracked: EthTxPoolTrackedMetricsSnapshot::from(&metrics.tracked),
        }
    }
}

impl Serialize for EthTxPoolMetrics {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        EthTxPoolMetricsSnapshot::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EthTxPoolMetrics {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let snapshot = EthTxPoolMetricsSnapshot::deserialize(deserializer)?;
        let metrics = Self::default();

        metrics.insert_owned_txs.set(snapshot.insert_owned_txs);
        metrics
            .insert_forwarded_txs
            .set(snapshot.insert_forwarded_txs);

        metrics
            .drop_not_well_formed
            .set(snapshot.drop_not_well_formed);
        metrics
            .drop_invalid_signature
            .set(snapshot.drop_invalid_signature);
        metrics.drop_nonce_too_low.set(snapshot.drop_nonce_too_low);
        metrics.drop_fee_too_low.set(snapshot.drop_fee_too_low);
        metrics
            .drop_insufficient_balance
            .set(snapshot.drop_insufficient_balance);
        metrics
            .drop_existing_higher_priority
            .set(snapshot.drop_existing_higher_priority);
        metrics
            .drop_replaced_by_higher_priority
            .set(snapshot.drop_replaced_by_higher_priority);
        metrics.drop_pool_full.set(snapshot.drop_pool_full);
        metrics
            .drop_pool_not_ready
            .set(snapshot.drop_pool_not_ready);
        metrics
            .drop_internal_state_backend_error
            .set(snapshot.drop_internal_state_backend_error);
        metrics
            .drop_internal_not_ready
            .set(snapshot.drop_internal_not_ready);

        metrics.create_proposal.set(snapshot.create_proposal);
        metrics
            .create_proposal_txs
            .set(snapshot.create_proposal_txs);
        metrics
            .create_proposal_tracked_addresses
            .set(snapshot.create_proposal_tracked_addresses);
        metrics
            .create_proposal_available_addresses
            .set(snapshot.create_proposal_available_addresses);
        metrics
            .create_proposal_backend_lookups
            .set(snapshot.create_proposal_backend_lookups);

        metrics.tracked.addresses.set(snapshot.tracked.addresses);
        metrics.tracked.txs.set(snapshot.tracked.txs);
        metrics
            .tracked
            .evict_expired_addresses
            .set(snapshot.tracked.evict_expired_addresses);
        metrics
            .tracked
            .evict_expired_txs
            .set(snapshot.tracked.evict_expired_txs);
        metrics
            .tracked
            .remove_committed_addresses
            .set(snapshot.tracked.remove_committed_addresses);
        metrics
            .tracked
            .remove_committed_txs
            .set(snapshot.tracked.remove_committed_txs);

        Ok(metrics)
    }
}

#[derive(Serialize, Deserialize)]
struct EthTxPoolTrackedMetricsSnapshot {
    addresses: u64,
    txs: u64,
    evict_expired_addresses: u64,
    evict_expired_txs: u64,
    remove_committed_addresses: u64,
    remove_committed_txs: u64,
}

impl From<&EthTxPoolTrackedMetrics> for EthTxPoolTrackedMetricsSnapshot {
    fn from(metrics: &EthTxPoolTrackedMetrics) -> Self {
        Self {
            addresses: metrics.addresses.get(),
            txs: metrics.txs.get(),
            evict_expired_addresses: metrics.evict_expired_addresses.get(),
            evict_expired_txs: metrics.evict_expired_txs.get(),
            remove_committed_addresses: metrics.remove_committed_addresses.get(),
            remove_committed_txs: metrics.remove_committed_txs.get(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_metrics_update_shared_executor_metrics() {
        let executor_metrics = init_executor_metrics();
        let metrics = EthTxPoolMetrics::from_executor_metrics(&executor_metrics);

        metrics.insert_owned_txs.inc();
        metrics.create_proposal_txs.add(3);
        metrics.tracked.addresses.set(7);

        assert_eq!(executor_metrics.get(POOL_INSERT_OWNED_TXS), 1);
        assert_eq!(executor_metrics.get(POOL_CREATE_PROPOSAL_TXS), 3);
        assert_eq!(executor_metrics.get(TRACKED_ADDRESSES), 7);
    }
}
