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

use super::{Acs, AcsOutput, types::NodeId};

/// An ACS that never communicates and immediately decides on its own
/// proposal.
pub struct NopAcs<V> {
    decision: Option<V>,
}

#[derive(Debug, Clone)]
pub enum NoMessage {}

impl<V> Acs<V> for NopAcs<V> {
    type Message = NoMessage;
    type Context = ();

    fn new(_ctx: &Self::Context) -> Self {
        Self { decision: None }
    }

    fn propose(&mut self, data: V) {
        self.decision = Some(data);
    }

    fn handle_message(&mut self, _sender: NodeId, message: Self::Message) {
        match message {}
    }

    fn decision(&self) -> Option<&V> {
        self.decision.as_ref()
    }

    fn poll(&mut self) -> Option<AcsOutput<Self::Message>> {
        None
    }
}
