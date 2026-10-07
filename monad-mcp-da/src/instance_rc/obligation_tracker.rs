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

use super::super::assignment::{ChunkAssignment, ChunkRouting, NodeIndex, Upstream};

// our owned chunks of a proposal, owed to us directly by its author
pub(crate) struct ObligationTracker {
    // our chunks still needed from the author, after the packet-loss trim
    remaining: usize,
    // set when our owned chunks complete, until taken
    fulfilled: bool,
}

impl ObligationTracker {
    // a receiver outside the assignment (None) is owed nothing
    pub(crate) fn new(assignment: &ChunkAssignment, receiver: Option<NodeIndex>) -> Self {
        let mut owed = 0;
        if let Some(receiver) = receiver {
            for chunk_id in assignment.chunk_ids() {
                if from_author(&assignment.routing(chunk_id), receiver) {
                    owed += 1;
                }
            }
        }
        let remaining = assignment.packet_loss_resistance().trim(owed);
        Self {
            remaining,
            fulfilled: remaining == 0,
        }
    }

    pub(crate) fn mark(&mut self, routing: &ChunkRouting<'_>, receiver: NodeIndex) {
        if self.remaining == 0 || !from_author(routing, receiver) {
            return;
        }
        self.remaining -= 1;
        self.fulfilled = self.remaining == 0;
    }

    // true once, when our owned chunks are complete
    pub(crate) fn take_fulfilled(&mut self) -> bool {
        std::mem::take(&mut self.fulfilled)
    }
}

fn from_author(routing: &ChunkRouting<'_>, receiver: NodeIndex) -> bool {
    routing.upstream(receiver) == Some(Upstream::Author)
}

#[cfg(test)]
mod tests {
    use monad_mcp_chorus::spec::Stake as _;

    use super::{
        super::super::{
            assignment::StakePartition,
            test_util::PACKET_LOSS_RESISTANCE,
            types::{NodeId, Stake},
        },
        *,
    };

    // author 0 owning nothing, then 3 nodes with equal stake over 30
    // source chunks at 2.5x redundancy: 75 chunks, 25 owned by each
    fn assignment() -> ChunkAssignment {
        let author = NodeId::dummy(0);
        let mut weights = vec![(author, Stake::ZERO)];
        weights.extend((1..=3).map(|id| (NodeId::dummy(id), Stake::from(1))));
        StakePartition::new(weights).assign(&author, 30, 2.5, PACKET_LOSS_RESISTANCE)
    }

    #[test]
    fn nothing_owed_is_fulfilled_from_the_start() {
        let assignment = assignment();

        // the author owns nothing, so is owed nothing; reported once
        let author = assignment.index_of(&NodeId::dummy(0));
        let mut tracker = ObligationTracker::new(&assignment, author);
        assert!(tracker.take_fulfilled());
        assert!(!tracker.take_fulfilled());

        // a node outside the assignment is owed nothing either
        let mut tracker = ObligationTracker::new(&assignment, None);
        assert!(tracker.take_fulfilled());

        // the author owes a member its owned chunks
        let member = assignment.index_of(&NodeId::dummy(1));
        let mut tracker = ObligationTracker::new(&assignment, member);
        assert!(!tracker.take_fulfilled());
    }

    #[test]
    fn our_share_is_fulfilled_once_by_its_last_needed_chunk() {
        let assignment = assignment();
        let member = assignment
            .index_of(&NodeId::dummy(1))
            .expect("in the assignment");
        let owner = assignment
            .index_of(&NodeId::dummy(2))
            .expect("in the assignment");
        let ours: Vec<_> = assignment.owned_chunks(member).collect();
        let theirs: Vec<_> = assignment.owned_chunks(owner).collect();
        let mut tracker = ObligationTracker::new(&assignment, Some(member));

        // 25 chunks are ours, of which the trim leaves 23 needed
        assert_eq!(ours.len(), 25);
        let last = PACKET_LOSS_RESISTANCE.trim(25) - 1;
        assert_eq!(last, 22);
        for routing in &ours[..last] {
            tracker.mark(routing, member);
        }
        // another owner's chunks are not owed by the author
        for routing in &theirs {
            tracker.mark(routing, member);
        }
        assert!(!tracker.take_fulfilled());

        tracker.mark(&ours[last], member);
        assert!(tracker.take_fulfilled());

        // fulfilled once
        tracker.mark(&ours[last + 1], member);
        assert!(!tracker.take_fulfilled());
    }
}
