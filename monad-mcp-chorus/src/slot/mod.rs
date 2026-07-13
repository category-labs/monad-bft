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

pub mod chorus;
pub mod dummy;
mod fallback;
mod fast;

use crate::types::{NodeId, Slot, TimestampDelta};

/// A single-slot consensus algorithm
pub trait SlotConsensus: Sized {
    type Config: Clone;
    type Context;

    type Message: Clone;
    type Timer: PartialEq + Eq + std::hash::Hash + Clone;
    type OptimisticCommitData: Clone;
    type FinalizationData: Clone;

    fn new(slot: Slot, config: &Self::Config, context: &Self::Context) -> Self;

    /// Called on the deadline of the slot
    fn handle_deadline(&mut self);

    /// Handle a received message from a peer.
    fn handle_message(&mut self, sender: NodeId, message: Self::Message);

    /// Handle a timer event.
    fn handle_timer(&mut self, timer: Self::Timer);

    /// Poll for output actions to be taken by the conductor.
    fn poll(&mut self) -> Option<SlotOutput<Self>>;
}

#[derive(Clone)]
pub enum SlotOutput<S: SlotConsensus> {
    /// (Re-)schedule a timer event to trigger on now+Delta. When
    /// scheduling the same timer event multiple times, the last one
    /// wins.
    ScheduleTimer(TimestampDelta, S::Timer),

    /// Broadcast a message to all peer validators (including self)
    /// The message is expected to be looped back to self via
    /// SlotConsensusInput.
    Broadcast(S::Message),

    /// Signal execution for optimistic execution.  Q: maybe move this
    /// message into a Context handle method? CommitOptimistic is
    /// meaningless without the knowledge of execution.
    CommitOptimistic(S::OptimisticCommitData),

    /// Finalize the slot. Signal conductor for slot closure.
    Finalize(S::FinalizationData),

    /// Report an unrecoverable fault raised within the slot. Signal
    /// conductor for slot closure.
    Fault { reason: String },
}
