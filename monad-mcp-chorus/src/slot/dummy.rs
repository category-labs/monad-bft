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

use crate::{
    slot::{SlotConsensus, SlotOutput},
    types::{IsVote, KeyPair, NodeId, Slot, VoteMsg, VotePool, dummy_serialize},
};

/// A dummy one-slot, proposal-less algorithm for testing
///
/// 1. On deadline, cast a vote.
/// 2. On each received vote, finalize the slot once quorum is met.
pub struct DummySlotConsensus {
    slot: Slot, // only used as FinalizationData
    outputs: VecDeque<SlotOutput<DummySlotConsensus>>,
    config: DummySlotConsensusConfig,
    votes: VotePool<DummyVote>,
}

#[derive(Clone)]
pub struct DummySlotConsensusConfig {
    pub quorum: usize,
}

impl Default for DummySlotConsensusConfig {
    fn default() -> Self {
        Self { quorum: 1 }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DummyVote;

impl IsVote for DummyVote {
    type Scope = Slot;

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

impl SlotConsensus for DummySlotConsensus {
    type Config = DummySlotConsensusConfig;
    type Context = ();

    type Message = VoteMsg<DummyVote>;
    type Timer = ();
    type OptimisticCommitData = ();
    type FinalizationData = ();

    fn new(slot: Slot, config: &Self::Config, _context: &Self::Context) -> Self {
        Self {
            slot,
            config: config.clone(),
            votes: VotePool::new(slot),
            outputs: VecDeque::new(),
        }
    }

    fn handle_deadline(&mut self) {
        // dummy signing: no node identity is wired into this consensus
        let key = KeyPair::dummy(0);
        let vote = VoteMsg::new_signed(self.slot, DummyVote, &key);
        self.outputs.push_back(SlotOutput::Broadcast(vote));
    }

    fn handle_timer(&mut self, _timer: Self::Timer) {
        // no timers in this dummy implementation
    }

    fn handle_message(&mut self, sender: NodeId, vote: VoteMsg<DummyVote>) {
        self.votes.add_vote(sender, vote);
        if self.votes.all_voters().count() == self.config.quorum {
            self.outputs.push_back(SlotOutput::Finalize(()));
        }
    }

    fn poll(&mut self) -> Option<SlotOutput<Self>> {
        self.outputs.pop_front()
    }
}
