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

use super::{Component, ProposalSource};
use crate::{
    chorus::{
        proposing::ProposalPlanner,
        types::{ProposalIndex, Slot, Timestamp},
    },
    config::ProposalConfig,
    epoch::EpochHandle,
};

pub enum ProposingInput {
    SlotOpen(Slot, Timestamp),
    // every slot strictly below cap is finalized
    CapAdvance(Slot),
}

pub type ProposingOutput = (Slot, ProposalIndex, Bytes);

// decides when we propose in a slot; the payload is assembled here
pub struct Proposing {
    planner: ProposalPlanner,
    source: Box<dyn ProposalSource + Send>,
    proposal_size_limit: usize,
    outbox: VecDeque<ProposingOutput>,
}

impl Proposing {
    pub fn new(
        epoch_handle: &EpochHandle,
        config: &ProposalConfig,
        source: Box<dyn ProposalSource + Send>,
    ) -> Self {
        let planner = ProposalPlanner::new(
            epoch_handle.self_id,
            epoch_handle.proposers.clone(),
            config.planner(&epoch_handle.proposers),
        );
        Self {
            planner,
            source,
            proposal_size_limit: config.proposal_size_limit(),
            outbox: VecDeque::new(),
        }
    }

    fn collect(&mut self, now: Timestamp) {
        while let Some((slot, index)) = self.planner.poll(now) {
            let payload = self
                .source
                .next_payload(slot, index, self.proposal_size_limit);
            self.outbox.push_back((slot, index, payload));
        }
    }
}

impl Component for Proposing {
    type Input = ProposingInput;
    type Output = ProposingOutput;

    fn handle(&mut self, now: Timestamp, input: ProposingInput) {
        match input {
            ProposingInput::SlotOpen(slot, deadline) => {
                self.planner.handle_slot_open(now, slot, deadline);
            }
            ProposingInput::CapAdvance(cap) => self.planner.handle_cap_advance(now, cap),
        }
        self.collect(now);
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.planner.next_due()
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.collect(now);
    }

    fn poll(&mut self) -> Option<ProposingOutput> {
        self.outbox.pop_front()
    }
}
