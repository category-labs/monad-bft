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

use std::collections::BTreeSet;

use super::super::assignment::{ChunkAssignment, ChunkId, NodeIndex};

// our owned chunks of a proposal not yet relayed to their second hop
pub(crate) struct RelayTracker {
    obligation: BTreeSet<ChunkId>,
}

impl RelayTracker {
    // a receiver outside the assignment (None) relays nothing
    pub(crate) fn new(assignment: &ChunkAssignment, owner: Option<NodeIndex>) -> Self {
        let obligation = match owner {
            Some(owner) => assignment
                .owned_chunks(owner)
                .map(|r| r.chunk_id())
                .collect(),
            None => BTreeSet::new(),
        };

        Self { obligation }
    }

    // true once per owned chunk: when it goes out
    pub(crate) fn take(&mut self, chunk_id: ChunkId) -> bool {
        self.obligation.remove(&chunk_id)
    }

    // every owned chunk not relayed yet
    pub(crate) fn take_all(&mut self) -> BTreeSet<ChunkId> {
        std::mem::take(&mut self.obligation)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::super::{
            encoding_scheme::DAEncodingScheme as _,
            test_util::{author, epoch_handle, scheme},
        },
        *,
    };

    fn id(wire: u16) -> ChunkId {
        ChunkId::unchecked(wire)
    }

    // the default fixture's assignment, and our index in it: we own 2, 6
    // and 10
    fn assignment() -> (ChunkAssignment, Option<NodeIndex>) {
        let epoch_handle = epoch_handle();
        let scheme = scheme(&epoch_handle);
        let assignment = scheme.chunk_assignment(&author(), &epoch_handle.validator_data);
        let self_index = assignment.index_of(&epoch_handle.self_id);
        (assignment, self_index)
    }

    #[test]
    fn each_owned_chunk_goes_out_once() {
        let (assignment, self_index) = assignment();
        let mut tracker = RelayTracker::new(&assignment, self_index);

        // not ours
        assert!(!tracker.take(id(1)));
        assert!(tracker.take(id(6)));
        assert!(!tracker.take(id(6)));

        assert_eq!(tracker.take_all(), BTreeSet::from([id(2), id(10)]));
        assert!(tracker.take_all().is_empty());
        assert!(!tracker.take(id(2)));
    }

    #[test]
    fn outside_the_assignment_nothing_goes_out() {
        let (assignment, _) = assignment();
        let mut tracker = RelayTracker::new(&assignment, None);

        assert!(!tracker.take(id(2)));
        assert!(tracker.take_all().is_empty());
    }
}
