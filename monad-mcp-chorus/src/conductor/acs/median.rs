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

use super::{
    Acs, AcsOutput,
    types::{NodeId, ValidatorData},
};
use crate::spec::validator::ValidatorData as _;

/// A single-round ACS protocol that decides on median of all
/// received messages after receiving all proposals.
pub struct MedianAcs<V> {
    proposals: BTreeMap<NodeId, Option<V>>,
    outbox: VecDeque<AcsOutput<V>>,
    decision: Option<V>,
}

impl<V> Acs<V> for MedianAcs<V>
where
    // Ord required for median calculation
    V: Ord,
{
    type Message = V;
    type Context = Arc<ValidatorData>;

    fn new(val_data: &Arc<ValidatorData>) -> Self {
        let proposals: BTreeMap<NodeId, _> = val_data.nodes().map(|id| (*id, None)).collect();
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use super::{
        super::types::{Stake, Timestamp},
        *,
    };
    use crate::spec::vote::KeyPair as _;

    fn context(validators: &[NodeId]) -> Arc<ValidatorData> {
        let valset = validators
            .iter()
            .map(|validator| (*validator, Stake::from(1)))
            .collect::<HashMap<_, _>>();
        let mapping = validators
            .iter()
            .map(|validator| (*validator, validator.keypair().pubkey()))
            .collect::<HashMap<_, _>>();
        Arc::new(ValidatorData::new(valset, mapping))
    }

    #[test]
    fn decides_median_proposal() {
        let validators = [NodeId::dummy(1), NodeId::dummy(2), NodeId::dummy(3)];
        let mut acs = MedianAcs::<Timestamp>::new(&context(&validators));

        acs.propose(Timestamp::from_nanos(20));
        assert_eq!(
            acs.poll(),
            Some(AcsOutput::Broadcast(Timestamp::from_nanos(20)))
        );

        acs.handle_message(validators[0], Timestamp::from_nanos(10));
        acs.handle_message(validators[1], Timestamp::from_nanos(30));
        assert_eq!(acs.decision(), None);
        acs.handle_message(validators[2], Timestamp::from_nanos(20));

        assert_eq!(acs.decision(), Some(&Timestamp::from_nanos(20)));
    }
}
