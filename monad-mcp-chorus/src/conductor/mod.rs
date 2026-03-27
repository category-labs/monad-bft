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

mod dummy;

use std::collections::BTreeMap;

use crate::types::{Slot, SlotDeadline, Timestamp};

pub trait Conductor
where
    Self: Sized,
{
    type Alarm;

    fn handle(&mut self, input: ConductorInput<Self>);
    fn poll(&mut self) -> Option<ConductorOutput<Self>>;
}

pub enum ConductorOutput<C>
where
    C: Conductor,
{
    ScheduleAlarm(Timestamp, C::Alarm),

    // Open a batch of slots each with their deadline. Then close all
    // slots strictly earlier than the cap.
    OpenSlots {
        slots: BTreeMap<Slot, SlotDeadline>,
        cap: Slot,
    },
}

pub enum ConductorInput<C>
where
    C: Conductor,
{
    Alarm(C::Alarm),
    SlotOpened(Slot),
    SlotFinalized(Slot),
}
