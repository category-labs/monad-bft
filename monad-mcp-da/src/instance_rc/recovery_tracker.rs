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

use std::collections::HashMap;

use bitvec::{bitbox, boxed::BitBox};

use super::super::{assignment::ChunkId, types::NodeId};

// each chunk is served to a requester at most once
pub(crate) struct ChunkRecoveryTracker {
    num_chunks: usize,

    // the chunks served to each requester
    served_chunks: HashMap<NodeId, BitBox>,
}

impl ChunkRecoveryTracker {
    pub(crate) fn new(num_chunks: usize) -> Self {
        Self {
            num_chunks,
            served_chunks: HashMap::new(),
        }
    }

    // returns whether to serve the chunk to the requester, recording it as
    // served if true.
    pub(crate) fn try_serve(&mut self, requester: &NodeId, chunk_id: ChunkId) -> bool {
        let num_chunks = self.num_chunks;
        let served = self
            .served_chunks
            .entry(*requester)
            .or_insert_with(|| bitbox![0; num_chunks]);

        if served[usize::from(chunk_id)] {
            return false;
        }
        served.set(usize::from(chunk_id), true);
        true
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_chorus::spec::Stake as _;

    use super::{
        super::super::{
            assignment::{ChunkAssignment, StakePartition},
            types::Stake,
        },
        *,
    };

    // author 0 owning nothing, then 3 nodes with equal stake over 30
    // source chunks at 2.5x redundancy: 75 chunks
    fn assignment() -> ChunkAssignment {
        let author = NodeId::dummy(0);
        let mut weights = vec![(author, Stake::ZERO)];
        weights.extend((1..=3).map(|id| (NodeId::dummy(id), Stake::from(1))));
        StakePartition::new(weights).assign(&author, 30, 2.5)
    }

    #[test]
    fn served_chunks_are_not_served_twice() {
        let assignment = assignment();
        let mut tracker = ChunkRecoveryTracker::new(assignment.num_chunks());
        let requester = NodeId::dummy(2);
        let chunk_id = assignment.chunk_ids().next().unwrap();

        assert!(tracker.try_serve(&requester, chunk_id));
        assert!(!tracker.try_serve(&requester, chunk_id));

        // per requester
        assert!(tracker.try_serve(&NodeId::dummy(3), chunk_id));
    }
}
