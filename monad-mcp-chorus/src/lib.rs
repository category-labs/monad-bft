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

mod driver;
mod runtime;
mod slot_manager;

pub mod types;
// indirectly exports Chorus, DummySlotConsensus
pub mod slot;
// indirectly exports DummyConductor
pub mod conductor;

pub use conductor::{Conductor, ConductorOutput};
pub use driver::{CadenceDriver, CadenceDriverMsg, CadenceMessage, Driver, NodeEvent, WakeId};
pub use runtime::{CadenceRuntime, FinalizationObserver, Runtime};
pub use slot::{SlotConsensus, SlotOutput};
pub use slot_manager::SlotManager;
