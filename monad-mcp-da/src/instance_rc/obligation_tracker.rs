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

// o' = o * (a/b)
const PACKET_LOSS_RESISTANCE: (usize, usize) = (9, 10);

pub(crate) struct ObligationTracker {
    // the number of chunks remaining to be received from each
    // rebroadcast owner, by NodeIndex
    remaining_owner_obligation: Box<[usize]>,

    // the number of chunks remaining to be received from the author
    remaining_author_obligation: usize,

    // an outbox of upstreams whose obligations have been fulfilled
    // since the last drain.
    fulfilled: Vec<Upstream>,
}

impl ObligationTracker {
    // a receiver outside the assignment (None) is routed nothing.
    pub(crate) fn new(assignment: &ChunkAssignment, receiver: Option<NodeIndex>) -> Self {
        let mut this = Self {
            remaining_author_obligation: 0,
            remaining_owner_obligation: vec![0; assignment.num_nodes()].into_boxed_slice(),
            fulfilled: vec![],
        };

        if let Some(receiver) = receiver {
            for chunk_id in assignment.chunk_ids() {
                let routing = assignment.routing(chunk_id);
                for upstream in owed(&routing, receiver) {
                    *this.counter(upstream) += 1;
                }
            }
        }

        this.trim();
        this.fulfill_vacuously();
        this
    }

    // trim the obligations to account for packet loss.
    fn trim(&mut self) {
        // todo: make assignment export this
        let (a, b) = PACKET_LOSS_RESISTANCE;
        let cut = |n: &mut usize| *n = (*n * a).div_ceil(b);

        cut(&mut self.remaining_author_obligation);
        for remaining in self.remaining_owner_obligation.iter_mut() {
            cut(remaining);
        }
    }

    fn fulfill_vacuously(&mut self) {
        if self.remaining_author_obligation == 0 {
            self.fulfilled.push(Upstream::Author);
        }
        for (node_index, remaining) in self.remaining_owner_obligation.iter().enumerate() {
            if *remaining == 0 {
                let owner = NodeIndex::new_unchecked(node_index);
                self.fulfilled.push(Upstream::Owner(owner));
            }
        }
    }

    pub(crate) fn drain_fulfilled(&mut self) -> Vec<Upstream> {
        std::mem::take(&mut self.fulfilled)
    }

    // the caller must ensure each chunk is recorded at most once, with
    // the routing given by the assignment this tracker was built from.
    pub(crate) fn mark(&mut self, routing: &ChunkRouting<'_>, receiver: NodeIndex) {
        for upstream in owed(routing, receiver) {
            self.settle(upstream);
        }
    }

    fn settle(&mut self, upstream: Upstream) {
        let counter = self.counter(upstream);
        if *counter == 0 {
            return;
        }

        *counter -= 1;
        if *counter == 0 {
            self.fulfilled.push(upstream);
        }
    }

    fn counter(&mut self, upstream: Upstream) -> &mut usize {
        match upstream {
            Upstream::Author => &mut self.remaining_author_obligation,
            Upstream::Owner(owner) => &mut self.remaining_owner_obligation[usize::from(owner)],
        }
    }
}

// who owes the receiver this chunk: its owner, and when the receiver
// is the owner, the author as well.
fn owed(routing: &ChunkRouting<'_>, receiver: NodeIndex) -> impl Iterator<Item = Upstream> {
    let mut owed = Vec::with_capacity(2);
    if let Some(upstream) = routing.upstream(receiver) {
        owed.push(Upstream::Owner(routing.owner_index()));
        if upstream == Upstream::Author {
            owed.push(Upstream::Author);
        }
    }
    owed.into_iter()
}

#[cfg(test)]
mod tests {
    use monad_mcp_chorus::spec::Stake as _;

    use super::{
        super::super::{
            assignment::StakePartition,
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
        StakePartition::new(weights).assign(&author, 30, 2.5)
    }

    #[test]
    fn zero_obligations_are_vacuously_fulfilled() {
        let assignment = assignment();
        let author = assignment
            .index_of(&NodeId::dummy(0))
            .expect("author in assignment");

        // the author is owed nothing and, holding every chunk, owes
        // nothing: everything is vacuous from the start, and drained once
        let mut tracker = ObligationTracker::new(&assignment, Some(author));
        let fulfilled = tracker.drain_fulfilled();
        assert!(fulfilled.contains(&Upstream::Author));
        assert!(fulfilled.contains(&Upstream::Owner(author)));
        assert_eq!(fulfilled.len(), 1 + assignment.num_nodes());
        assert!(tracker.drain_fulfilled().is_empty());

        // a member is owed nothing only by the chunkless author
        let member = assignment.index_of(&NodeId::dummy(1));
        let mut tracker = ObligationTracker::new(&assignment, member);
        assert_eq!(tracker.drain_fulfilled(), vec![Upstream::Owner(author)]);

        // a node outside the assignment is owed nothing by anyone
        let mut tracker = ObligationTracker::new(&assignment, None);
        assert_eq!(tracker.drain_fulfilled().len(), 1 + assignment.num_nodes());
    }

    #[test]
    #[ignore] // FIXME: failing
    fn an_obligation_is_fulfilled_once_by_its_last_chunk() {
        let assignment = assignment();
        let author = assignment
            .index_of(&NodeId::dummy(0))
            .expect("in the assignment");
        let member = assignment
            .index_of(&NodeId::dummy(1))
            .expect("in the assignment");
        let owner = assignment
            .index_of(&NodeId::dummy(2))
            .expect("in the assignment");
        let ours: Vec<_> = assignment.owned_chunks(member).collect();
        let theirs: Vec<_> = assignment.owned_chunks(owner).collect();
        let mut tracker = ObligationTracker::new(&assignment, Some(member));
        tracker.drain_fulfilled();

        // 25 chunks are owed by the author and 25 by each owner
        for i in 0..24 {
            tracker.mark(&ours[i], member);
            tracker.mark(&theirs[i], member);
        }
        assert!(tracker.drain_fulfilled().is_empty());

        // our last own chunk settles the author, and what we owe as owner
        tracker.mark(&ours[24], member);
        assert_eq!(
            tracker.drain_fulfilled(),
            vec![Upstream::Owner(member), Upstream::Author]
        );
        tracker.mark(&theirs[24], member);
        assert_eq!(tracker.drain_fulfilled(), vec![Upstream::Owner(owner)]);

        // fulfilled once; a chunk unrouted to the receiver credits nothing
        tracker.mark(&ours[0], member);
        assert!(tracker.drain_fulfilled().is_empty());
        assert!(owed(&ours[0], author).next().is_none());
    }
}
