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
    collections::{BTreeSet, VecDeque},
    num::NonZeroU64,
    ops::Add,
};

use super::{
    Conductor, ConductorOutput,
    acs::{Acs, AcsOutput},
};
use crate::types::{NodeId, Slot, SlotDeadline, Timestamp, TimestampDelta};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct CadenceConductorConfig {
    // the timestamp where the world begins
    pub genesis: Timestamp,
    // the offset from genesis is the first slot's deadline
    pub deadline_offset: TimestampDelta,
    // tau from the paper
    pub slot_interval: TimestampDelta,
    // W from the paper
    pub slots_per_window: NonZeroU64,
    // p from the paper; must be a fraction of slots_per_window
    pub sync_boundary_slots: NonZeroU64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Window {
    id: WindowId,
    start: SlotDeadline, // the deadline of the first slot of the window
    proposed_next_start: Option<SlotDeadline>,
}

pub struct CadenceConductor<A: Acs<SlotDeadline>> {
    config: CadenceConductorConfig,
    output: VecDeque<ConductorOutput<Self>>,

    curr_window: Window,
    finalized_slots: FinalizedSlots,

    context: A::Context,
    acs: Option<A>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct WindowId(u64);
impl Add<u64> for WindowId {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        WindowId(self.0 + rhs)
    }
}

pub enum Alarm {
    // Time to switch over to the next window
    NextWindow(Window),
}

impl<A: Acs<SlotDeadline>> Conductor for CadenceConductor<A> {
    type Alarm = Alarm;
    type Message = (WindowId, A::Message);

    fn handle_message(&mut self, sender: NodeId, message: Self::Message) {
        let (window_id, acs_message) = message;

        if window_id != self.curr_window.id {
            // ignore window messages from other windows
            return;
        }

        if let Some(acs) = &mut self.acs {
            acs.handle_message(sender, acs_message);
        }
        self.run_acs();
    }

    fn handle_alarm(&mut self, alarm: Alarm) {
        match alarm {
            Alarm::NextWindow(window) => {
                self.curr_window = window;
                self.acs = Some(A::new(&self.context));
            }
        }
    }

    fn handle_slot_finalization(&mut self, at: Timestamp, slot: Slot) {
        self.finalized_slots.mark_finalized(slot);
        if self.finalized_slots.cap() < self.sync_boundary() {
            // not yet reached sync boundary, nothing to do.
            return;
        }

        // we've reached or exceeded the sync boundary and all
        // previous slots finalized. now we need need to propose the
        // first slot of the next window.
        if self.curr_window.proposed_next_start.is_some() {
            // we've already proposed for the next first deadline,
            // ignore.
            return;
        }

        let deadline = self.deadline_proposal(at, slot);
        self.curr_window.proposed_next_start = Some(deadline);

        if let Some(acs) = &mut self.acs {
            acs.propose(deadline);
        }
        self.run_acs();
    }

    fn poll(&mut self) -> Option<ConductorOutput<Self>> {
        self.output.pop_front()
    }
}

impl<A: Acs<SlotDeadline>> CadenceConductor<A> {
    pub fn new(config: CadenceConductorConfig, context: A::Context) -> Self {
        assert!(config.sync_boundary_slots.get() < config.slots_per_window.get());

        let first_window = Window {
            id: WindowId(0),
            start: config.genesis + config.deadline_offset,
            proposed_next_start: None,
        };

        let mut this = Self {
            config,
            output: VecDeque::new(),
            curr_window: first_window,
            finalized_slots: FinalizedSlots::default(),
            acs: Some(A::new(&context)),
            context,
        };

        this.open_slots(&first_window);
        this
    }

    fn sync_boundary(&self) -> Slot {
        let sync_boundary_slots = self.config.sync_boundary_slots.get();
        let slot = self.first_slot(&self.curr_window).0 + sync_boundary_slots;
        Slot(slot)
    }

    fn run_acs(&mut self) {
        let Some(acs) = &mut self.acs else {
            return;
        };

        while let Some(output) = acs.poll() {
            match output {
                AcsOutput::Broadcast(msg) => {
                    self.output
                        .push_back(ConductorOutput::Broadcast((self.curr_window.id, msg)));
                }
            }
        }

        if let Some(decision) = acs.decision().copied() {
            self.handle_acs_decision(decision);
            self.acs = None; // abandoned
        }
    }

    fn handle_acs_decision(&mut self, deadline: SlotDeadline) {
        let next_window = self.next_window(deadline);

        self.open_slots(&next_window);
        self.close_slots(self.sync_boundary());
        self.schedule_next_window(next_window);
    }

    fn next_window(&self, deadline: SlotDeadline) -> Window {
        let next_window_id = self.curr_window.id + 1;
        let next_window_start = deadline;

        Window {
            id: next_window_id,
            start: next_window_start,
            proposed_next_start: None,
        }
    }

    fn schedule_next_window(&mut self, next_window: Window) {
        self.output.push_back(ConductorOutput::ScheduleAlarm(
            next_window.start,
            Alarm::NextWindow(next_window),
        ));
    }

    fn first_slot(&self, window: &Window) -> Slot {
        let slots_per_window = self.config.slots_per_window.get();
        let first_slot_index = window.id.0 * slots_per_window;
        Slot(first_slot_index)
    }

    // open all slots for a given window.
    fn open_slots(&mut self, window: &Window) {
        let slots = (0..self.config.slots_per_window.get())
            .map(|i| {
                let slot = self.first_slot(window) + i;
                let deadline = window.start + self.config.slot_interval * i;
                (slot, deadline)
            })
            .collect();

        self.output.push_back(ConductorOutput::OpenSlots(slots));
    }

    fn close_slots(&mut self, cap: Slot) {
        self.output.push_back(ConductorOutput::CloseSlots { cap });
    }

    // propose the deadline for the first slot of the next window
    fn deadline_proposal(&self, now: Timestamp, slot: Slot) -> SlotDeadline {
        let slots_per_window = self.config.slots_per_window.get();
        let slot_index = slot.0 % slots_per_window;
        let slots_left = slots_per_window - slot_index - 1;

        // note: if slots_left == 0, the proposed deadline will be
        // now, which matches the spec from the paper
        now + self.config.slot_interval * slots_left
    }
}

struct FinalizedSlots {
    // slots < cap are all finalized
    cap: Slot,
    // finalized slot >= cap
    finalized: BTreeSet<Slot>,
}

impl Default for FinalizedSlots {
    fn default() -> Self {
        Self {
            // nothing finalized yet
            cap: Slot(0),
            finalized: Default::default(),
        }
    }
}

impl FinalizedSlots {
    fn cap(&self) -> Slot {
        self.cap
    }

    fn is_finalized(&self, slot: Slot) -> bool {
        if slot < self.cap {
            true
        } else {
            self.finalized.contains(&slot)
        }
    }

    fn mark_finalized(&mut self, slot: Slot) {
        if slot < self.cap {
            return;
        }
        self.finalized.insert(slot);
        self.try_advance_cap();
    }

    fn try_advance_cap(&mut self) {
        let mut next_cap = self.cap;

        while self.finalized.contains(&next_cap) {
            next_cap = next_cap.next();
        }

        if next_cap <= self.cap {
            return;
        }

        self.cap = next_cap;
        self.finalized = self.finalized.split_off(&self.cap);
    }
}
