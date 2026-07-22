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

pub mod acs;
pub mod cadence;
pub mod dummy;

use std::collections::BTreeMap;

use super::types::{self, NodeId, Slot, SlotDeadline, Timestamp};

pub trait Conductor
where
    Self: Sized,
{
    type Alarm;
    type Message;

    fn handle_message(&mut self, sender: NodeId, message: Self::Message);
    fn handle_alarm(&mut self, alarm: Self::Alarm);
    fn handle_slot_finalization(&mut self, at: Timestamp, slot: Slot);

    fn poll(&mut self) -> Option<ConductorOutput<Self>>;
}

#[derive(Clone)]
pub enum ConductorOutput<C>
where
    C: Conductor,
{
    Broadcast(C::Message),
    ScheduleAlarm(Timestamp, C::Alarm),

    // Open a batch of slots each with their deadline.
    // Invariant: must be contiguous.
    OpenSlots(BTreeMap<Slot, SlotDeadline>),

    // Close all slots strictly earlier than the cap.
    CloseSlots { cap: Slot },
}
