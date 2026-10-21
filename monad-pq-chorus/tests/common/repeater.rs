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

//! The node's post-halt Repeater (§7), emulated: a halted slot's
//! `Finalized(x)` is re-broadcast every interval while the slot is at least
//! `cap − retention`.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use monad_mcp_chorus::stub::{
    CadenceMessage, FinalizationObserver, Outbound,
    types::{Slot, Timestamp, TimestampDelta},
};
use monad_pq_chorus::{PqEvent, PqHalt, PqMessage, types::Metablock};

use super::Wire;

#[derive(Default)]
struct Retained {
    halted: BTreeMap<Slot, Metablock>,
}

/// The observer half: records halts and drops them below the retention.
#[derive(Clone)]
pub struct HaltFeed {
    retained: Arc<Mutex<Retained>>,
    retention: u64,
}

impl FinalizationObserver<PqEvent, PqHalt> for HaltFeed {
    fn handle_finalization(&mut self, _now: Timestamp, slot: Slot, halt: &PqHalt) {
        let mut retained = self.retained.lock().unwrap();
        retained.halted.insert(slot, halt.metablock.clone());
    }

    fn handle_chain_advance(&mut self, _now: Timestamp, cap: Slot) {
        let floor = Slot(cap.get().saturating_sub(self.retention));
        let mut retained = self.retained.lock().unwrap();
        retained.halted.retain(|slot, _| *slot >= floor);
    }
}

pub struct Repeater {
    retained: Arc<Mutex<Retained>>,
    interval: TimestampDelta,
    next: Timestamp,
}

impl Repeater {
    pub fn new(interval: TimestampDelta, retention: u64) -> (Self, HaltFeed) {
        let retained = Arc::new(Mutex::new(Retained::default()));
        let feed = HaltFeed {
            retained: retained.clone(),
            retention,
        };
        let repeater = Self {
            retained,
            interval,
            next: Timestamp::GENESIS + interval,
        };
        (repeater, feed)
    }

    pub fn next_due(&self) -> Option<Timestamp> {
        let retained = self.retained.lock().unwrap();
        (!retained.halted.is_empty()).then_some(self.next)
    }

    /// The re-broadcasts due at `now`.
    pub fn tick(&mut self, now: Timestamp) -> Vec<Outbound<Wire>> {
        if now < self.next {
            return Vec::new();
        }
        self.next = now + self.interval;
        let retained = self.retained.lock().unwrap();
        retained
            .halted
            .iter()
            .map(|(slot, x)| {
                Outbound::Broadcast(CadenceMessage::Slot(*slot, PqMessage::Finalized(x.clone())))
            })
            .collect()
    }
}
