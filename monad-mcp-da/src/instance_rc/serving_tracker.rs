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

use super::super::{
    assignment::{ByNode, ChunkAssignment, NodeIndex},
    types::{Timestamp, TimestampDelta},
};

// each requester is served at most one round of honest demand per window
pub(crate) struct ServingTracker {
    window: TimestampDelta,
    // chunks each node owns
    shares: ByNode<usize>,
    own_share: usize,
    // each requester's current window: its start, and the chunks served in it
    served: ByNode<Option<(Timestamp, usize)>>,
}

impl ServingTracker {
    pub(crate) fn new(
        assignment: &ChunkAssignment,
        self_index: Option<NodeIndex>,
        window: TimestampDelta,
    ) -> Self {
        let mut shares = assignment.by_node(|_| 0);
        for chunk_id in assignment.chunk_ids() {
            let owner = assignment.routing(chunk_id).owner_index();
            shares[owner] += 1;
        }
        let own_share = match self_index {
            Some(index) => shares[index],
            None => 0,
        };
        Self {
            window,
            shares,
            own_share,
            served: assignment.by_node(|_| None),
        }
    }

    // how many of the wanted chunks the requester may be sent now
    pub(crate) fn grant(&mut self, requester: NodeIndex, wanted: usize, now: Timestamp) -> usize {
        let quota = self.quota(requester);
        let (start, served) = self.served[requester].get_or_insert((now, 0));
        if now >= *start + self.window {
            *start = now;
            *served = 0;
        }
        let granted = wanted.min(quota - *served);
        *served += granted;
        granted
    }

    // the requester's chunks, ours, and the header
    fn quota(&self, requester: NodeIndex) -> usize {
        self.shares[requester] + self.own_share + 1
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::super::{
            encoding_scheme::DAEncodingScheme as _,
            test_util::{NOW, author, epoch_handle, scheme, timing},
        },
        *,
    };

    fn assignment() -> (ChunkAssignment, Option<NodeIndex>) {
        let epoch_handle = epoch_handle();
        let scheme = scheme(&epoch_handle);
        let assignment = scheme.chunk_assignment(&author(), &epoch_handle.validator_data);
        let self_index = assignment.index_of(&epoch_handle.self_id);
        (assignment, self_index)
    }

    fn tracker() -> ServingTracker {
        let (assignment, self_index) = assignment();
        ServingTracker::new(&assignment, self_index, timing().serve_window)
    }

    fn requesters() -> (NodeIndex, NodeIndex) {
        let (assignment, _) = assignment();
        let mut indices = assignment.nodes().map(|(index, _)| index);
        (indices.next().unwrap(), indices.next().unwrap())
    }

    #[test]
    fn the_quota_is_both_shares_and_the_header() {
        let (assignment, self_index) = assignment();
        let self_index = self_index.expect("we are a validator");
        let (requester, _) = requesters();

        let theirs = assignment.owned_chunks(requester).count();
        let ours = assignment.owned_chunks(self_index).count();
        assert_eq!(tracker().quota(requester), theirs + ours + 1);
    }

    #[test]
    fn a_round_of_honest_demand_is_granted_in_full() {
        let mut tracker = tracker();
        let (requester, _) = requesters();
        let quota = tracker.quota(requester);

        assert_eq!(tracker.grant(requester, quota, NOW), quota);
    }

    #[test]
    fn demand_beyond_the_quota_is_cut() {
        let mut tracker = tracker();
        let (requester, _) = requesters();
        let quota = tracker.quota(requester);

        assert_eq!(tracker.grant(requester, quota - 1, NOW), quota - 1);
        assert_eq!(tracker.grant(requester, 3, NOW), 1);
        assert_eq!(tracker.grant(requester, 1, NOW), 0);
    }

    #[test]
    fn a_window_later_grants_again() {
        let mut tracker = tracker();
        let (requester, _) = requesters();
        let quota = tracker.quota(requester);
        tracker.grant(requester, quota, NOW);

        let window = timing().serve_window;
        let just_before = NOW + TimestampDelta::from_millis(window.as_millis() - 1);
        assert_eq!(tracker.grant(requester, 1, just_before), 0);
        assert_eq!(tracker.grant(requester, 1, NOW + window), 1);
    }

    #[test]
    fn requesters_do_not_share_a_budget() {
        let mut tracker = tracker();
        let (first, second) = requesters();
        let quota = tracker.quota(first);
        tracker.grant(first, quota, NOW);

        assert_eq!(tracker.grant(second, 1, NOW), 1);
    }
}
