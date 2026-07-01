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
    collections::{BTreeMap, VecDeque},
    num::NonZeroU64,
};

use super::{Conductor, ConductorInput, ConductorOutput};
use crate::types::{Slot, Timestamp, TimestampDelta};

type WindowId = u64;

// A simple conductor that just open windows at regular interval,
// within each window the slots are scheduled at regular deadline.
#[derive(Clone)]
pub struct DummyConductor {
    genesis: Timestamp,
    slot_interval: TimestampDelta,
    deadline_offset: TimestampDelta,
    slots_per_window: NonZeroU64,

    outputs: VecDeque<ConductorOutput<Self>>,
}

impl Default for DummyConductor {
    fn default() -> Self {
        Self::new(TimestampDelta::new(1000), 5)
    }
}

impl DummyConductor {
    pub fn new(slot_interval: TimestampDelta, slots_per_window: u64) -> Self {
        let slots_per_window =
            NonZeroU64::new(slots_per_window).expect("slots_per_window must be non-zero");
        let init_alarm = ConductorOutput::ScheduleAlarm(Timestamp::GENESIS, 0);

        Self {
            genesis: Timestamp::GENESIS,
            slot_interval,
            deadline_offset: slot_interval,
            slots_per_window,
            outputs: VecDeque::from([init_alarm]),
        }
    }

    pub fn set_deadline_offset(mut self, offset: TimestampDelta) -> Self {
        self.deadline_offset = offset;
        self
    }
    pub fn set_genesis(mut self, genesis: Timestamp) -> Self {
        self.genesis = genesis;
        self
    }

    pub fn window_duration(&self) -> TimestampDelta {
        self.slot_interval * self.slots_per_window.get()
    }

    pub fn window_start(&self, window_id: WindowId) -> Timestamp {
        self.genesis + self.window_duration() * window_id
    }

    pub fn slot_deadline(&self, slot: Slot) -> TimestampDelta {
        // The first slot in a window schedules at exactly the deadline_offset.
        let slot_index = slot.0 % self.slots_per_window.get();
        self.slot_interval * slot_index + self.deadline_offset
    }
}

impl Conductor for DummyConductor {
    type Alarm = WindowId;

    fn handle(&mut self, input: ConductorInput<Self>) {
        match input {
            ConductorInput::Alarm(window_id) => {
                let slots: BTreeMap<_, _> = (0..self.slots_per_window.get())
                    .map(|i| {
                        let slot = Slot(window_id * self.slots_per_window.get() + i);
                        let deadline = self.slot_deadline(slot);
                        (slot, deadline)
                    })
                    .collect();

                let cap = Slot(window_id * self.slots_per_window.get());
                let next_window = window_id + 1;

                self.outputs.push_back(ConductorOutput::ScheduleAlarm(
                    self.window_start(next_window),
                    next_window,
                ));
                self.outputs
                    .push_back(ConductorOutput::OpenSlots { slots, cap });
            }

            ConductorInput::SlotOpened(_slot) => {}
            ConductorInput::SlotFinalized(_at, _slot) => {}
        }
    }

    fn poll(&mut self) -> Option<ConductorOutput<Self>> {
        self.outputs.pop_front()
    }
}
