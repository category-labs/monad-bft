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

use super::Component;
use crate::{
    chorus::{
        SlotLifecycle,
        slot::chorus::SlotFinalization,
        types::{Lane, ProposalId, Slot, Timestamp},
    },
    da::Resolved,
    finalization::{FinalizationCollector, FinalizedSlot},
};

pub enum FinalizationInput {
    Lifecycle(SlotLifecycle),
    // stamped when cadence finalized the slot
    Finalized(Timestamp, Slot, SlotFinalization),
    FastBlock(Timestamp, Slot), // demo(tx-timeline)
    Resolved {
        at: Timestamp, // demo(tx-timeline)
        slot: Slot,
        lane: Lane,
        pid: ProposalId,
        message: Resolved,
    },
}

impl Component for FinalizationCollector {
    type Input = FinalizationInput;
    type Output = FinalizedSlot;

    fn handle(&mut self, _now: Timestamp, input: FinalizationInput) {
        match input {
            FinalizationInput::Lifecycle(event) => self.handle_lifecycle(event),
            FinalizationInput::Finalized(at, slot, finalization) => {
                self.handle_finalization(at, slot, finalization);
            }
            FinalizationInput::FastBlock(at, slot) => self.handle_fast_block(at, slot),
            FinalizationInput::Resolved {
                at,
                slot,
                lane,
                pid,
                message,
            } => self.handle_resolved(at, slot, lane, pid, message),
        }
    }

    fn poll(&mut self) -> Option<FinalizedSlot> {
        FinalizationCollector::poll(self)
    }
}
