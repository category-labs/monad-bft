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

use std::collections::{BTreeSet, HashMap};

use crate::types::{Entry, NodeId, ProposalId, ProposalIndex, ProposalMap};

/// Per proposal index, the senders of positive entries per root and of
/// negative entries, over the accepted votes (`votes_i`).
pub(crate) struct VoteTally {
    positive: Vec<HashMap<ProposalId, BTreeSet<NodeId>>>,
    negative: Vec<BTreeSet<NodeId>>,
}

impl VoteTally {
    pub fn new(k: usize) -> Self {
        Self {
            positive: vec![HashMap::new(); k],
            negative: vec![BTreeSet::new(); k],
        }
    }

    /// Counts one vote; returns the `(j, root)` pairs it voted positively.
    pub fn add(
        &mut self,
        sender: NodeId,
        entries: &ProposalMap<Entry>,
    ) -> Vec<(ProposalIndex, ProposalId)> {
        let mut positives = Vec::new();
        for j in 0..entries.size() {
            match entries[j] {
                Entry::Root(pid) => {
                    self.positive[j].entry(pid).or_default().insert(sender);
                    positives.push((j, pid));
                }
                Entry::Bot => {
                    self.negative[j].insert(sender);
                }
            }
        }
        positives
    }

    pub fn positive_voters(&self, j: ProposalIndex, pid: &ProposalId) -> Option<&BTreeSet<NodeId>> {
        self.positive[j].get(pid)
    }

    pub fn positive(&self, j: ProposalIndex, pid: &ProposalId) -> usize {
        self.positive_voters(j, pid).map_or(0, BTreeSet::len)
    }

    pub fn negative_voters(&self, j: ProposalIndex) -> &BTreeSet<NodeId> {
        &self.negative[j]
    }

    pub fn negative(&self, j: ProposalIndex) -> usize {
        self.negative[j].len()
    }

    /// The root with the most positive entries, ties to the smallest pid,
    /// if it has at least `threshold`.
    pub fn leading_root(&self, j: ProposalIndex, threshold: usize) -> Option<ProposalId> {
        self.positive[j]
            .iter()
            .map(|(pid, voters)| (voters.len(), pid))
            .filter(|(count, _)| *count >= threshold)
            .max_by(|(a, pa), (b, pb)| a.cmp(b).then(pb.as_bytes().cmp(pa.as_bytes())))
            .map(|(_, pid)| *pid)
    }

    /// The value `strong` votes name for proposer j, if any (unique, as
    /// two strong sets of distinct senders cannot both fit in n).
    pub fn quorum_value(&self, j: ProposalIndex, strong: usize) -> Option<Entry> {
        if let Some(pid) = self.leading_root(j, strong) {
            return Some(Entry::Root(pid));
        }
        (self.negative(j) >= strong).then_some(Entry::Bot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::test_util::pid;

    fn entries(values: &[Entry]) -> ProposalMap<Entry> {
        ProposalMap::new(values.len(), |j| values[j])
    }

    #[test]
    fn counts_positive_and_negative_per_index() {
        let mut tally = VoteTally::new(2);
        let positives = tally.add(
            NodeId::dummy(1),
            &entries(&[Entry::Root(pid(1)), Entry::Bot]),
        );
        assert_eq!(positives, vec![(0, pid(1))]);
        tally.add(
            NodeId::dummy(2),
            &entries(&[Entry::Root(pid(1)), Entry::Bot]),
        );
        tally.add(
            NodeId::dummy(3),
            &entries(&[Entry::Root(pid(2)), Entry::Root(pid(3))]),
        );

        assert_eq!(tally.positive(0, &pid(1)), 2);
        assert_eq!(tally.negative(1), 2);
        assert_eq!(tally.quorum_value(1, 2), Some(Entry::Bot));
        assert_eq!(tally.quorum_value(0, 2), Some(Entry::Root(pid(1))));
        assert_eq!(tally.quorum_value(0, 3), None);
    }

    #[test]
    fn leading_root_breaks_ties_by_smallest_pid() {
        let mut tally = VoteTally::new(1);
        tally.add(NodeId::dummy(1), &entries(&[Entry::Root(pid(9))]));
        tally.add(NodeId::dummy(2), &entries(&[Entry::Root(pid(4))]));
        assert_eq!(tally.leading_root(0, 1), Some(pid(4)));
        tally.add(NodeId::dummy(3), &entries(&[Entry::Root(pid(9))]));
        assert_eq!(tally.leading_root(0, 1), Some(pid(9)));
        assert_eq!(tally.leading_root(0, 3), None);
    }
}
