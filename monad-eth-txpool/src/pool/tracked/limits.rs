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

use std::time::Duration;

use alloy_primitives::Address;
use indexmap::IndexMap;
use tracing::{error, info};

use crate::pool::transaction::ValidEthTransaction;

// To produce 5k tx blocks, we need the tracked tx map to hold at least 15k addresses so that, after
// pruning the txpool of up to 5k unique addresses in the last committed block update and up to 5k
// unique addresses in the pending blocktree, the tracked tx map will still have at least 5k other
// addresses with at least one tx each to use when creating the next block.
const DEFAULT_MAX_ADDRESSES: usize = 16 * 1024;

const DEFAULT_MAX_TXS: usize = 64 * 1024;

const DEFAULT_MAX_EIP2718_BYTES: u64 = 4 * 1024 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct TrackedTxLimitsConfig {
    max_addresses: usize,
    max_txs: usize,
    max_eip2718_bytes: u64,

    soft_evict_addresses_watermark: usize,

    soft_tx_expiry: Duration,
    hard_tx_expiry: Duration,
}

impl TrackedTxLimitsConfig {
    pub fn new(
        max_addresses: Option<usize>,
        max_txs: Option<usize>,
        max_eip2718_bytes: Option<u64>,

        soft_evict_addresses_watermark: Option<usize>,

        soft_tx_expiry: Duration,
        hard_tx_expiry: Duration,
    ) -> Self {
        Self {
            max_addresses: max_addresses.unwrap_or(DEFAULT_MAX_ADDRESSES),
            max_txs: max_txs.unwrap_or(DEFAULT_MAX_TXS),
            max_eip2718_bytes: max_eip2718_bytes.unwrap_or(DEFAULT_MAX_EIP2718_BYTES),

            soft_evict_addresses_watermark: soft_evict_addresses_watermark
                .unwrap_or(DEFAULT_MAX_ADDRESSES - 512),

            soft_tx_expiry,
            hard_tx_expiry,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TrackedTxLimits {
    config: TrackedTxLimitsConfig,

    txs: usize,
    eip2718_bytes: u64,
}

impl TrackedTxLimits {
    pub fn new(config: TrackedTxLimitsConfig) -> Self {
        Self {
            config,

            txs: 0,
            eip2718_bytes: 0,
        }
    }

    pub fn build_txs_map<V>(&self) -> IndexMap<Address, V> {
        IndexMap::with_capacity(self.config.max_addresses)
    }

    pub fn expiry_duration_during_evict(&self) -> Duration {
        if self.txs < self.config.soft_evict_addresses_watermark {
            self.config.hard_tx_expiry
        } else {
            info!(num_txs =? self.txs, "txpool limits hit soft evict addresses watermark");
            self.config.soft_tx_expiry
        }
    }

    pub fn expiry_duration_during_insert(&self) -> Duration {
        self.config.hard_tx_expiry
    }

    pub fn can_add_address(&self, addresses: usize) -> bool {
        addresses < self.config.max_addresses
    }

    pub fn add_tx(&mut self, tx: &ValidEthTransaction) -> bool {
        let txs = self.txs + 1;
        let eip2718_bytes = self.eip2718_bytes + tx.raw().eip2718_encoded_length() as u64;

        if txs > self.config.max_txs {
            return false;
        }

        if eip2718_bytes > self.config.max_eip2718_bytes {
            return false;
        }

        self.txs = txs;
        self.eip2718_bytes = eip2718_bytes;

        true
    }

    pub fn remove_tx(&mut self, tx: &ValidEthTransaction) {
        self.txs = self.txs.checked_sub(1).unwrap_or_else(|| {
            error!("txpool txs limit underflowed, detected during remove_tx");
            0
        });

        self.eip2718_bytes = self
            .eip2718_bytes
            .checked_sub(tx.raw().eip2718_encoded_length() as u64)
            .unwrap_or_else(|| {
                error!("txpool eip2718_bytes limit underflowed, detected during remove_tx");
                0
            });
    }

    pub fn remove_txs<'a>(&mut self, txs: impl Iterator<Item = &'a ValidEthTransaction>) {
        for tx in txs {
            self.remove_tx(tx)
        }
    }
}
