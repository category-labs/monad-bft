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

use std::collections::VecDeque;

use crate::{
    slot::{SlotConsensus, SlotOutput},
    types::{NodeId, Slot, TimestampDelta},
};

/// A dummy one-slot, proposal-less algorithm that automatically
/// decides at N*Delta after deadline.
pub struct DummySlotConsensus {
    slot: Slot, // only used as FinalizationData
    outputs: VecDeque<SlotOutput<DummySlotConsensus>>,
    delta: TimestampDelta,
}

#[derive(Clone)]
pub struct DummySlotConsensusConfig {
    pub num_ticks: usize,
    pub delta: TimestampDelta,
}

#[derive(PartialEq, Eq, Clone, Hash)]
pub struct FinalizeAfter(usize);

impl SlotConsensus for DummySlotConsensus {
    type Config = DummySlotConsensusConfig;
    type Context = ();

    type Message = ();
    type Timer = FinalizeAfter;
    type OptimisticCommitData = (); // unused
    type FinalizationData = Slot;

    fn new(
        slot: Slot,
        deadline: TimestampDelta,
        config: &Self::Config,
        _context: &Self::Context,
    ) -> Self {
        let timer = SlotOutput::ScheduleTimer(deadline, FinalizeAfter(config.num_ticks));

        Self {
            slot,
            delta: config.delta,
            outputs: VecDeque::from([timer]),
        }
    }

    fn handle_message(&mut self, _sender: NodeId, _message: Self::Message) {
        // no messages in this dummy algorithm
    }

    fn handle_timer(&mut self, FinalizeAfter(ticks_left): Self::Timer) {
        if ticks_left == 0 {
            self.outputs.push_back(SlotOutput::Finalize(self.slot));
            return;
        }

        let timer = SlotOutput::ScheduleTimer(self.delta, FinalizeAfter(ticks_left - 1));
        self.outputs.push_back(timer);
    }

    fn poll(&mut self) -> Option<SlotOutput<Self>> {
        self.outputs.pop_front()
    }
}
