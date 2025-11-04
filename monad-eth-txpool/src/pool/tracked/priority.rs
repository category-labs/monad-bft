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
    collections::{BTreeSet, HashMap},
    time::Instant,
};

use alloy_primitives::Address;
use tracing::error;

use crate::{pool::tracked::TrackedTxList, EthTxPoolEventTracker};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Priority {
    pub nonce_gap: u64,
    pub tips: [u64; 7],
}

#[derive(Clone, Debug, Default)]
pub(super) struct PriorityMap {
    by_address: HashMap<Address, (Priority, Instant)>,
    sorted: BTreeSet<(Priority, Instant, Address)>,
}

impl PriorityMap {
    pub fn update_priority(
        &mut self,
        event_tracker: &EthTxPoolEventTracker<'_>,
        address: Address,
        tx_list: &TrackedTxList,
    ) {
        let time = if let Some((stale_priority, time)) = self.by_address.remove(&address) {
            if !self.sorted.remove(&(stale_priority, time, address)) {
                error!(
                    ?address,
                    "txpool priority map stale priority not in sorted priorities"
                );
            }

            time
        } else {
            event_tracker.now
        };

        let priority = tx_list.compute_priority();

        self.by_address.insert(address, (priority, time));
        self.sorted.insert((priority, time, address));
    }

    pub fn pop_eviction_address(&mut self) -> Option<Address> {
        let (_, _, eviction_address) = self.sorted.pop_last()?;

        if self.by_address.remove(&eviction_address).is_none() {
            error!(
                ?eviction_address,
                "txpool priority map eviction address not in by address map"
            );
        }

        Some(eviction_address)
    }

    pub fn remove(&mut self, address: Address) {
        let Some((priority, time)) = self.by_address.remove(&address) else {
            error!(
                ?address,
                "txpool priority map remove address not in by address map"
            );
            return;
        };

        if !self.sorted.remove(&(priority, time, address)) {
            error!(
                ?address,
                ?priority,
                "txpool priority map remove address not in sorted priorities"
            );
        }
    }

    pub fn reset(&mut self) {
        self.by_address.clear();
        self.sorted.clear();
    }
}
