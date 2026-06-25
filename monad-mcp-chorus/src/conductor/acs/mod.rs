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

pub mod median;
pub mod nop;

use super::types::{self, NodeId};

/// A protocol for Agreement on a Core Set
pub trait Acs<V> {
    type Message;
    type Context;

    fn new(ctx: &Self::Context) -> Self;

    /// Propose the data to be included in the core set. At most one
    /// proposal is allowed for each Acs instance.
    fn propose(&mut self, data: V);

    /// Handle a message received over network
    fn handle_message(&mut self, sender: NodeId, message: Self::Message);

    /// Query whether the ACS has made an decision
    fn decision(&self) -> Option<&V>;

    fn poll(&mut self) -> Option<AcsOutput<Self::Message>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcsOutput<M> {
    /// A message that needs to be broadcasted to all peers, including
    /// self.
    Broadcast(M),
}
