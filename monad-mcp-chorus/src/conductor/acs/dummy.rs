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

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use super::{Acs, AcsOutput};
use crate::types::{NodeId, ValidatorData};

/// A dummy, single-round ACS protocol that decides on median of all
/// received messages after receiving all proposals.
pub struct DummyAcs<V> {
    proposals: BTreeMap<NodeId, Option<V>>,
    outbox: VecDeque<AcsOutput<V>>,
    decision: Option<V>,
}

impl<V> Acs<V> for DummyAcs<V>
where
    // Ord required for median calculation
    V: Ord,
{
    type Message = V;
    type Context = Arc<ValidatorData>;

    fn new(val_data: &Arc<ValidatorData>) -> Self {
        let proposals: BTreeMap<NodeId, _> = val_data
            .valset_unordered()
            .keys()
            .map(|id| (*id, None))
            .collect();
        assert!(!proposals.is_empty());

        Self {
            proposals,
            decision: None,
            outbox: Default::default(),
        }
    }

    fn decision(&self) -> Option<&V> {
        self.decision.as_ref()
    }

    fn propose(&mut self, value: V) {
        self.outbox.push_back(AcsOutput::Broadcast(value));
    }

    fn handle_message(&mut self, sender: NodeId, message: V) {
        if self.decision.is_some() {
            return;
        }

        let proposal = self
            .proposals
            .get_mut(&sender)
            .expect("sender must be from validator set");
        *proposal = Some(message);

        if !self.proposals.values().all(|v| v.is_some()) {
            return;
        }

        // we've seen all proposals, decide on the median value
        let mut values: Vec<_> = std::mem::take(&mut self.proposals)
            .into_values()
            .map(|v| v.unwrap())
            .collect();
        values.sort();

        let median_idx = values.len() / 2;
        // safety: |valset| != 0 as asserted on creation
        let decision = values.swap_remove(median_idx);
        self.decision = Some(decision);
    }

    fn poll(&mut self) -> Option<AcsOutput<V>> {
        self.outbox.pop_front()
    }
}
