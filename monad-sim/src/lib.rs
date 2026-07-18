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

//! Generic, single-threaded discrete-event simulation infrastructure.
//!
//! The simulation is a time-ordered queue of *steps*. A step is a closure that
//! runs at a specific [`time::Time`] and may schedule further steps. State lives
//! in *processes*: any value `S: 'static` registered with [`Simulation::spawn`]
//! and addressed by a typed [`Handle`]. When a step scheduled against a handle
//! runs, the framework lends it `&mut S` exclusively for the duration of that
//! step, via a [`Ctx`] that can read time, sample randomness, and schedule more
//! work — but cannot reach any other process's state. Cross-process interaction
//! is therefore always "schedule a step on that handle", which keeps the engine
//! agnostic about event and component-state types and makes reentrancy
//! impossible by construction.
//!
//! [`time`] provides the simulation time type; [`dist`] provides
//! network-latency distributions for modelling message delays.
//!
//! # Not yet implemented
//!
//! The engine runs a single global clock with a deterministic per-step total
//! order. Per-process local clocks (NTP-style drift), the clock-scheduling
//! strategy that orders steps across them, and synchronous signal fan-out are
//! deferred; see `docs/design.md` ("Future work") for the rationale.

pub mod dist;
mod sim;
pub mod time;

pub use sim::{
    CancelToken, Ctx, Handle, ProcessId, RunOutcome, SimMetrics, Simulation, StepInfo, StepLabel,
};
pub use time::{ClockDiff, Time};
