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

use super::types::{NodeId, ProposalIndex, Slot};

pub trait ProposerElection {
    // invariant: get_proposer(s, i) == Some(n) iff get_index(s, n) == Some(i)
    // corollary: for each slot, a node can occupy at most one index

    // returns None if index has no proposer
    fn get_proposer(&self, slot: Slot, index: ProposalIndex) -> Option<&NodeId>;

    // returns None if node is not a valid proposer for the slot
    fn get_index(&self, slot: Slot, node: &NodeId) -> Option<ProposalIndex>;
}
