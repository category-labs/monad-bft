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

mod monad_mvba;
use std::sync::Arc;

use super::{
    fast::{CertifiedEntry, EnterFallbackCert},
    types::{KeyPair, NodeId, Slot, TimestampDelta, TotalProposalMap, ValidatorData},
};

#[derive(Clone, Copy, PartialEq, Eq)]
struct FallbackView(u64);

#[derive(Clone)]
pub(crate) struct FallbackPath {
    slot: Slot,
    round: FallbackView,

    input: MVBAInputs,

    // using Arc to avoid lifetime issues.
    key: Arc<KeyPair>,
    validator_data: Arc<ValidatorData>,
}

impl FallbackPath {
    pub(crate) fn new(
        slot: Slot,
        key: Arc<KeyPair>,
        validator_data: Arc<ValidatorData>,
        input: MVBAInputs,
    ) -> Self {
        Self {
            slot,
            round: FallbackView(0),
            key,
            validator_data,
            input,
        }
    }

    pub(crate) fn on_tick(&mut self) {
        todo!()
    }
}

// FIXME: rename it. partialblock concept was deleted
pub(crate) type PartialBlock = TotalProposalMap<CertifiedEntry>;

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct MVBAInputs {
    pub enter_fallback_cert: EnterFallbackCert,
    pub block: PartialBlock,
}

pub trait Validate {
    type Context;
    fn validate(&self, context: &Self::Context) -> bool;
}

/// A protocol for Agreement on a Core Set
pub trait Mvba<V: Validate> {
    type Message;
    type Context;
    type TimerEvent;

    fn new(ctx: &Self::Context) -> Self;

    /// Propose the data to be included in the core set. At most one
    /// proposal is allowed for each Acs instance.
    fn propose(&mut self, data: V);

    /// Handle a message received over network
    fn handle_message(&mut self, sender: NodeId, message: Self::Message);

    fn handle_timer(&mut self, timer_event: Self::TimerEvent);

    // Q: can abandon be implicit via destruction? or do we need it to inform
    // persistence
    fn abandon(&mut self);

    /// Query whether the ACS has made an decision
    fn decision(&self) -> Option<&V>;

    fn poll(&mut self) -> Option<MVBAOutput<Self::Message, Self::TimerEvent>>;
}

pub enum MVBAOutput<M, T> {
    Broadcast(M),
    ScheduleTimer {
        duration: TimestampDelta,
        timer_event: T,
    },
}
