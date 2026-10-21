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

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{Entry, Metablock, NodeId, OrdMap};

/// The halting gadget's tally: received Finalized messages, at most one per
/// validator (`ln:qv:echo`, `ln:qv:halt`).
#[derive(Default)]
pub(crate) struct FinalizedTally {
    senders: BTreeSet<NodeId>,
    tally: BTreeMap<OrdMap<Entry>, BTreeSet<NodeId>>,
}

impl FinalizedTally {
    pub fn add(&mut self, sender: NodeId, metablock: Metablock) {
        if self.senders.insert(sender) {
            self.tally
                .entry(OrdMap(metablock))
                .or_default()
                .insert(sender);
        }
    }

    /// A metablock `threshold` validators announced.
    pub fn reached(&self, threshold: usize) -> Option<&Metablock> {
        self.tally
            .iter()
            .find(|(_, senders)| senders.len() >= threshold)
            .map(|(OrdMap(metablock), _)| metablock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ProposalMap;

    #[test]
    fn one_message_per_validator() {
        let x = ProposalMap::new(1, |_| Entry::Bot);
        let mut tally = FinalizedTally::default();
        tally.add(NodeId::dummy(1), x.clone());
        tally.add(NodeId::dummy(1), x.clone());
        assert!(tally.reached(2).is_none());
        tally.add(NodeId::dummy(2), x.clone());
        assert_eq!(tally.reached(2), Some(&x));
    }
}
