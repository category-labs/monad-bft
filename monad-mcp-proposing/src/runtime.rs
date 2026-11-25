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

use std::collections::VecDeque;

use bytes::Bytes;
use monad_mcp_common::{Proposal, ProposalScope, SlotLifecycle, Timestamp};

use super::{planner::FixedLeadPlanner, source::ProposalSource};

pub enum ProposingInput<S: ProposalSource> {
    Lifecycle(SlotLifecycle),
    Source(S::LocalIn),
}

// Joins the planner and a source: the source seals a payload each time the
// planner says that a proposal is due.
pub struct ProposingRuntime<S> {
    planner: FixedLeadPlanner,
    source: S,
    outputs: VecDeque<Proposal<Bytes>>,
}

impl<S: ProposalSource> ProposingRuntime<S> {
    pub fn new(planner: FixedLeadPlanner, source: S) -> Self {
        Self {
            planner,
            source,
            outputs: VecDeque::new(),
        }
    }

    pub fn handle_input(&mut self, input: ProposingInput<S>, now: Timestamp) {
        match input {
            ProposingInput::Lifecycle(event) => {
                self.planner.handle_lifecycle(event, now);
                self.source.handle_lifecycle(event);
            }
            ProposingInput::Source(input) => self.source.handle_input(input, now),
        }
        self.step(now);
    }

    pub fn next_due(&self) -> Option<Timestamp> {
        self.planner.next_due()
    }

    pub fn handle_due(&mut self, now: Timestamp) {
        self.step(now);
    }

    pub fn poll(&mut self) -> Option<Proposal<Bytes>> {
        self.outputs.pop_front()
    }

    fn step(&mut self, now: Timestamp) {
        while let Some((slot, lane)) = self.planner.poll(now) {
            let scope = ProposalScope::new(slot, lane);
            let payload = self.source.seal(scope, now);
            let proposal = Proposal {
                slot,
                lane,
                payload,
            };
            self.outputs.push_back(proposal);
        }
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_common::{Lane, Slot, TimestampDelta};
    use rand::SeedableRng as _;
    use rand_chacha::ChaCha8Rng;

    use super::{
        super::{
            env::{
                common::{NodeId, ValidatorData},
                epoch::{EpochHandle, election::FixedProposerSchedule},
            },
            planner::{FixedLeadConfig, FixedLeadPlanner},
            source::RandomSource,
        },
        *,
    };

    const LEAD: TimestampDelta = TimestampDelta::from_millis(60);
    const PROPOSAL_SIZE: usize = 8;

    #[test]
    fn the_source_seals_when_the_planner_says_to_propose() {
        let me = NodeId::dummy(0);
        let election = FixedProposerSchedule::new(vec![NodeId::dummy(1), me]);
        let epoch_handle = EpochHandle::new(ValidatorData::dummy(4), election);
        let config = FixedLeadConfig {
            lead: LEAD,
            min_lead: TimestampDelta::ZERO,
            observation_cutoff: 0,
        };
        let planner = FixedLeadPlanner::new(me, epoch_handle, config);
        let source = RandomSource::fixed_size(PROPOSAL_SIZE, ChaCha8Rng::seed_from_u64(7));
        let mut runtime = ProposingRuntime::new(planner, source);

        let deadline = Timestamp::from_millis(1_000);
        let opened = SlotLifecycle::Opened {
            slot: Slot(3),
            deadline,
        };
        runtime.handle_input(
            ProposingInput::Lifecycle(opened),
            Timestamp::from_millis(100),
        );
        assert!(runtime.poll().is_none());

        let due = runtime.next_due().expect("a seal is due");
        assert_eq!(due, deadline.saturating_sub_delta(LEAD));
        runtime.handle_due(due);

        let proposal = runtime.poll().expect("a proposal at the seal time");
        assert_eq!(proposal.slot, Slot(3));
        assert_eq!(proposal.lane, Lane::new_unchecked(1));
        assert_eq!(proposal.payload.len(), PROPOSAL_SIZE);
        assert!(runtime.poll().is_none());
    }
}
