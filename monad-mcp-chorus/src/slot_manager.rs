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

use std::collections::{BTreeMap, BTreeSet};

use super::{
    slot::{SlotConsensus, SlotOutput},
    types::Slot,
};

pub struct SlotManager<S>
where
    S: SlotConsensus,
{
    slots: BTreeMap<Slot, S>,

    closed_slots: BTreeSet<Slot>,
    cap: Slot,

    config: S::Config,
    context: S::Context,
}

impl<S> SlotManager<S>
where
    S: SlotConsensus,
{
    pub fn new(config: S::Config, context: S::Context) -> Self {
        Self {
            slots: BTreeMap::new(),
            closed_slots: BTreeSet::new(),
            cap: Slot::MIN,
            // pending: PendingSlotBuffers,
            config,
            context,
        }
    }

    pub fn open(&mut self, slot: Slot) {
        if slot < self.cap {
            tracing::warn!("Refuse to open a slot below the cap: {:?}", slot);
            return;
        } else if self.closed_slots.contains(&slot) {
            tracing::warn!("Refuse to open an already-closed slot: {:?}", slot);
            return;
        } else if self.slots.contains_key(&slot) {
            tracing::warn!("Refuse to open an already-open slot: {:?}", slot);
            return;
        }

        let instance = S::new(slot, &self.config, &self.context);
        self.slots.insert(slot, instance);
    }

    /// The instance for an open, not-yet-closed slot, if any.
    pub fn slot_instance(&mut self, slot: Slot) -> Option<&mut S> {
        if slot < self.cap || self.closed_slots.contains(&slot) {
            return None;
        }

        self.slots.get_mut(&slot)
    }

    pub fn poll_any(&mut self) -> Option<(Slot, SlotOutput<S>)> {
        for (slot, instance) in self.slots.iter_mut() {
            if let Some(output) = instance.poll() {
                return Some((*slot, output));
            }
        }

        None
    }

    pub fn advance_cap(&mut self, cap: Slot) {
        let cap = self.cap.max(cap); // refuse regression
        self.cap = cap;

        let retained_slots = self.slots.split_off(&cap); // slots >= cap
        let expired_slots = std::mem::replace(&mut self.slots, retained_slots);
        // maybe signal slot close events
        drop(expired_slots);

        let retained_slots = self.closed_slots.split_off(&cap);
        let expired_slots = std::mem::replace(&mut self.closed_slots, retained_slots);
        drop(expired_slots);
    }

    pub fn close(&mut self, slot: Slot) {
        if slot < self.cap {
            return;
        }

        self.closed_slots.insert(slot);
    }
}
