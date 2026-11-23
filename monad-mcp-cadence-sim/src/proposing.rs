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

use std::convert::Infallible;

use bytes::Bytes;
use monad_mcp_common::{ProposalScope, SlotLifecycle, Timestamp, stub::NodeId};
use monad_mcp_proposing::stub::{ProposalSource, ProposingRuntime};

use crate::da::mock_payload;

pub(crate) type MockProposing = ProposingRuntime<MockSource>;

// seals the canonical mock payload of its node
pub struct MockSource {
    me: NodeId,
}

impl MockSource {
    pub fn new(me: NodeId) -> Self {
        Self { me }
    }
}

impl ProposalSource for MockSource {
    type LocalIn = Infallible;

    fn handle_lifecycle(&mut self, _event: SlotLifecycle) {}

    fn handle_input(&mut self, input: Infallible, _now: Timestamp) {
        match input {}
    }

    fn seal(&mut self, scope: ProposalScope, _now: Timestamp) -> Bytes {
        mock_payload(self.me, scope.slot, scope.lane)
    }
}
