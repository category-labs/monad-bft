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

use std::collections::BTreeMap;

pub mod acs;
pub mod cadence;
pub mod deadline_agreement;
pub mod dummy;

pub use cadence::{
    CadenceConductor, CapAdvance, ConductorConfig, ConductorError, ConductorMessage,
};
pub use deadline_agreement::{DeadlineAgreementManager, DeadlineAgreementMessage};

use super::{
    message::Outbound,
    types::{self, NodeId, Slot, Timestamp},
};

pub trait Conductor
where
    Self: Sized,
{
    type Alarm;
    type Message;

    fn handle_message(&mut self, at: Timestamp, sender: NodeId, message: Self::Message);
    fn handle_alarm(&mut self, alarm: Self::Alarm);
    fn handle_slot_finalization(&mut self, at: Timestamp, slot: Slot);

    fn poll(&mut self) -> Option<ConductorOutput<Self>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConductorOutput<C>
where
    C: Conductor,
{
    Send(Outbound<C::Message>),
    ScheduleAlarm(Timestamp, C::Alarm),

    // Open a batch of slots each with its deadline.
    // Invariant: the slots must be contiguous.
    OpenSlots(BTreeMap<Slot, Timestamp>),

    // Close all slots strictly earlier than the cap.
    CloseSlots { cap: Slot },
}
