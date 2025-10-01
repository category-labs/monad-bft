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

use std::{ops::Deref, time::Duration};

use alloy_primitives::Address;
use indexmap::IndexMap;
use tracing::{error, info};

use super::list::TrackedTxList;
use crate::pool::transaction::ValidEthTransaction;

// To produce 5k tx blocks, we need the tracked tx map to hold at least 15k addresses so that, after
// pruning the txpool of up to 5k unique addresses in the last committed block update and up to 5k
// unique addresses in the pending blocktree, the tracked tx map will still have at least 5k other
// addresses with at least one tx each to use when creating the next block.
const MAX_ADDRESSES: usize = 16 * 1024;

const MAX_TXS: usize = 64 * 1024;

// Tx batches from rpc can contain up to roughly 500 transactions. Since we don't evict based on how
// many txs are in the pool, we need to ensure that after eviction there is always space for all 500
// txs.
const SOFT_EVICT_ADDRESSES_WATERMARK: usize = MAX_ADDRESSES - 512;

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
        if self.txs < SOFT_EVICT_ADDRESSES_WATERMARK {
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

    pub fn prepare_add_tx_to_new_address<'l>(
        &'l mut self,
        tx: ValidEthTransaction,
    ) -> Result<TrackedTxLimitAddToken<'l>, ValidEthTransaction> {
        if !self.can_increase_limits(1, 1) {
            return Err(tx);
        }

        Ok(TrackedTxLimitAddToken {
            kind: TrackedTxLimitAddTokenKind::NewAddress,
            limits: self,
            tx,
        })
    }

    pub fn prepare_add_tx_to_existing<'l>(
        &'l mut self,
        tx: ValidEthTransaction,
    ) -> Result<TrackedTxLimitAddToken<'l>, ValidEthTransaction> {
        if !self.can_increase_limits(0, 1) {
            return Err(tx);
        }

        Ok(TrackedTxLimitAddToken {
            kind: TrackedTxLimitAddTokenKind::Existing,
            limits: self,
            tx,
        })
    }

    pub fn remove_tx(&mut self) {
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

    pub fn remove_address(&mut self) {
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

    pub fn remove_txs(&mut self, removed_address: bool, removed_txs: usize) {
        let Self {
            config: _,

            addresses,
            txs,
        } = self;

        if removed_address {
            *addresses = addresses.checked_sub(1).unwrap_or_else(|| {
                error!("txpool address limit underflowed");
                0
            });
        }

        *txs = txs.checked_sub(removed_txs).unwrap_or_else(|| {
            error!("txpool txs limit underflowed");
            0
        });
    }

    pub fn reset(&mut self) {
        let Self {
            config: _,

            addresses,
            txs,
        } = self;

        *addresses = 0;
        *txs = 0;
    }
}

pub(super) struct TrackedTxLimitAddToken<'l> {
    kind: TrackedTxLimitAddTokenKind,
    limits: &'l mut TrackedTxLimits,
    tx: ValidEthTransaction,
}

enum TrackedTxLimitAddTokenKind {
    NewAddress,
    Existing,
}

impl<'l> TrackedTxLimitAddToken<'l> {
    pub fn limits(&'l self) -> &'l TrackedTxLimits {
        self.limits
    }

    pub fn finalize(self) -> ValidEthTransaction {
        let Self {
            kind,
            limits:
                TrackedTxLimits {
                    config,
                    addresses,
                    txs,
                },
            tx,
        } = self;

        *txs += 1;

        match kind {
            TrackedTxLimitAddTokenKind::NewAddress => {
                *addresses += 1;
            }
            TrackedTxLimitAddTokenKind::Existing => {}
        }

        tx
    }

    pub fn cancel(self) -> ValidEthTransaction {
        self.tx
    }
}

impl<'l> Deref for TrackedTxLimitAddToken<'l> {
    type Target = ValidEthTransaction;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}
