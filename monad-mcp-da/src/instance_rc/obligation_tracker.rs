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

use super::super::assignment::{ChunkAssignment, NodeIndex, Upstream};

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
        let mut remaining_owner_obligation = vec![0; assignment.num_nodes()].into_boxed_slice();
        let mut remaining_author_obligation = 0;

        if let Some(receiver) = receiver {
            for upstream in assignment.upstreams(receiver) {
                match upstream {
                    Upstream::Author => {
                        remaining_author_obligation += 1;
                    }
                    Upstream::Owner(owner) => {
                        remaining_owner_obligation[usize::from(owner)] += 1;
                    }
                }
            }
        }

        // todo: reduce obligations by a small fraction to account for
        // network packet loss. q: should we reduce obligation from
        // author as well?

        let mut fulfilled = Vec::new();
        if remaining_author_obligation == 0 {
            // vacuously fulfilled from the start.
            fulfilled.push(Upstream::Author);
        }
        for (owner, _) in assignment.nodes() {
            if Some(owner) == receiver {
                continue;
            }
            if remaining_owner_obligation[usize::from(owner)] == 0 {
                // vacuously fulfilled from the start.
                fulfilled.push(Upstream::Owner(owner));
            }
        }

        Self {
            remaining_author_obligation,
            remaining_owner_obligation,
            fulfilled,
        }
    }

    pub(crate) fn drain_fulfilled(&mut self) -> Vec<Upstream> {
        std::mem::take(&mut self.fulfilled)
    }

    // the caller must ensure each chunk is recorded at most once, with
    // the upstream given by the assignment this tracker was built from.
    pub(crate) fn mark(&mut self, upstream: Option<Upstream>) {
        let Some(upstream) = upstream else {
            // chunk not routed to us, no obligation to record
            return;
        };
        let counter: &mut usize = match upstream {
            Upstream::Author => &mut self.remaining_author_obligation,
            Upstream::Owner(owner) => &mut self.remaining_owner_obligation[usize::from(owner)],
        };

        if *counter == 0 {
            return;
        }

        *counter -= 1;
        if *counter == 0 {
            self.fulfilled.push(upstream);
        }
    }
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

        // the author is owed nothing: everything but itself is vacuous
        // from the start, and drained once
        let mut tracker = ObligationTracker::new(&assignment, Some(author));
        let fulfilled = tracker.drain_fulfilled();
        assert!(fulfilled.contains(&Upstream::Author));
        assert!(!fulfilled.contains(&Upstream::Owner(author)));
        assert_eq!(fulfilled.len(), assignment.num_nodes());
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
    fn an_obligation_is_fulfilled_once_by_its_last_chunk() {
        let assignment = assignment();
        let member = assignment.index_of(&NodeId::dummy(1));
        let owner = assignment
            .index_of(&NodeId::dummy(2))
            .expect("in the assignment");
        let mut tracker = ObligationTracker::new(&assignment, member);
        tracker.drain_fulfilled();

        // 25 chunks are owed by the author and 25 by each owner
        for _ in 0..24 {
            tracker.mark(Some(Upstream::Author));
            tracker.mark(Some(Upstream::Owner(owner)));
        }
        assert!(tracker.drain_fulfilled().is_empty());

        tracker.mark(Some(Upstream::Author));
        assert_eq!(tracker.drain_fulfilled(), vec![Upstream::Author]);
        tracker.mark(Some(Upstream::Owner(owner)));
        assert_eq!(tracker.drain_fulfilled(), vec![Upstream::Owner(owner)]);

        // fulfilled once; unrouted chunks credit nothing
        tracker.mark(Some(Upstream::Author));
        tracker.mark(None);
        assert!(tracker.drain_fulfilled().is_empty());
    }
}
