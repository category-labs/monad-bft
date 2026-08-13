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

//! Monad's MVBA (Multi-Valued Validated Byzantine Agreement) instance used by
//! the fallback path.
//!
//! Everything here is a placeholder: the types name the pieces the protocol
//! needs, and every body is `todo!()`.

use std::sync::Arc;

use super::{
    super::types::{KeyPair, NodeId, Slot, ValidatorData},
    FallbackView, MVBAInputs, MVBAOutput, Mvba, Validate,
};

/// Per-instance state handed to [`MonadMvba::new`].
#[allow(dead_code)]
pub(crate) struct Context {
    /// Slot this MVBA instance decides for.
    pub slot: Slot,
    /// Fallback view that spawned this instance.
    pub view: FallbackView,
    pub key: Arc<KeyPair>,
    pub validator_data: Arc<ValidatorData>,
    // TODO: it will need access to both slot and view leader election
}

/// Timers driven by the MVBA state machine.
#[allow(dead_code)]
pub(crate) enum TimerEvent {
    /// View change timeout: no decision reached in the current MVBA view.
    ViewTimeout(FallbackView),
}

/// Wire messages exchanged between MVBA participants.
#[allow(dead_code)]
pub(crate) enum Message {
    /// Leader's proposal for the core set.
    Propose(Input),
    /// Vote on the current leader's proposal.
    Vote,
    /// View change / abandon-leader message.
    ViewChange(FallbackView),
}

/// Value proposed into the MVBA.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct Input {
    pub inputs: MVBAInputs,
}

impl Validate for Input {
    type Context = ();

    fn validate(&self, context: &Self::Context) -> bool {
        todo!()
    }
}

#[allow(dead_code)]
pub(crate) struct MonadMvba {
    context: Context,
    /// Current MVBA view; advances on `TimerEvent::ViewTimeout`.
    view: FallbackView,
    /// This node's proposal, set by [`MVBA::propose`].
    proposal: Option<Input>,
    decision: Option<Input>,
    abandoned: bool,
}

impl Mvba<Input> for MonadMvba {
    type Message = Message;
    type Context = Context;
    type TimerEvent = TimerEvent;

    fn new(_ctx: &Self::Context) -> Self {
        todo!()
    }

    fn propose(&mut self, _data: Input) {
        todo!()
    }

    fn handle_message(&mut self, _sender: NodeId, _message: Self::Message) {
        todo!()
    }

    fn handle_timer(&mut self, _timer_event: Self::TimerEvent) {
        todo!()
    }

    fn abandon(&mut self) {
        todo!()
    }

    fn decision(&self) -> Option<&Input> {
        self.decision.as_ref()
    }

    fn poll(&mut self) -> Option<MVBAOutput<Self::Message, Self::TimerEvent>> {
        todo!()
    }
}
