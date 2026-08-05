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

/// Chorus module for managing single-slot consensus state and logic.
use std::{collections::VecDeque, sync::Arc};

use super::{
    SlotConsensus, SlotOutput,
    fallback::{FallbackPath, MVBAInputs},
    fast::{
        BatchVoteMsg, CommitVoteDeadlineOutcome, EnterFallbackCert, FallbackVoteMsg, FastBlock,
        FastCommitQc, FastCommitVoteMsg, FastPath,
    },
    types::{
        EquivCert, HeaderAuth, KeyPair, MerkleRoot, NodeId, ProposalHeader, ProposalIndex, Slot,
        TimestampDelta, ValidatorData,
    },
};

#[derive(derive_more::From, Clone, PartialEq, Eq, Hash, Debug)]
#[non_exhaustive]
pub enum Message {
    // vote on proposal deadline (D_s)
    #[from]
    BatchVote(BatchVoteMsg),

    // vote when forming fast block
    #[from]
    FastCommitVote(FastCommitVoteMsg),
    #[from]
    FastBlock(FastBlock),

    // vote on vote deadline (D_s + Delta)
    #[from]
    FallbackVote(FallbackVoteMsg),

    // disseminate on forming fast commit qc
    #[from]
    FastCommitQc(FastCommitQc),

    // disseminate on entering fallback path
    #[from]
    EnterFallbackCert(EnterFallbackCert),
    // messages for the fallback path (todo)
    // ProposeBlock(PartialBlock),
    // FallbackCommitQc(...)
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum TimerEvent {
    // emitted on D_s + Delta
    FallbackTransitionTimeout,
    // emitted on D_s + 2*Delta
    FallbackDecisionDelayElapsed,

    // ticks every Delta after FallbackDeadline
    FallbackTick,
}

/// Static per-slot configuration.
#[derive(Clone)]
pub struct ChorusConfig {
    pub delta: TimestampDelta,
    pub num_proposals: usize,
}

/// Shared resources needed to spawn a slot instance.
pub struct ChorusContext {
    pub key: Arc<KeyPair>,
    pub validator_data: Arc<ValidatorData>,
    pub header_auth: Arc<HeaderAuth>,
}

#[derive(derive_more::From, Clone, PartialEq, Eq, Hash, Debug)]
pub enum SlotFinalization {
    #[from]
    Fast(FastCommitQc),
    // Fallback(FallbackCommitQc),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ChorusDAEvent {
    pub j: ProposalIndex,
    pub event: ProposalDAEvent,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum ProposalDAEvent {
    // a validated proposer-signed header observed (once per root)
    HeaderSeen(ProposalHeader),

    // all our own assigned chunks under the root have arrived
    ProposerObligationFulfilled(MerkleRoot),

    // the owner's rebroadcast obligation to us fulfilled under the root
    OwnerObligationFulfilled { owner: NodeId, root: MerkleRoot },

    // decode-then-re-encode verified. implies all chunks recoverable
    // from DA.
    Decoded(MerkleRoot),
    DecodingFailed(MerkleRoot),

    // two validly signed conflicting headers observed by DA
    Equivocation(EquivCert),
}

// An effect directed at the DA layer. Roots named here are pinned by DA.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum ChorusDACommand {
    // batch vote cast; rebroadcast our obliged chunks for roots we
    // voted positive.
    SlotVoted {
        positive: Vec<(ProposalIndex, MerkleRoot)>,
    },

    // positive fallback entry cast; re-encode and send every validator
    // its assigned chunks
    FallbackEntryCast {
        j: ProposalIndex,
        root: MerkleRoot,
    },

    // MVBA decided; ensure our own chunks under these roots: recover
    // them from the targets (who voted for decode success)
    RecoverOwnChunks {
        targets: Vec<NodeId>,
        roots: Vec<(ProposalIndex, MerkleRoot)>,
    },

    // report header learned from consensus evidence
    ObserveProposal {
        j: ProposalIndex,
        header: ProposalHeader,
    },
}

#[derive(Clone)]
struct FallbackMsgBuffer;

/// The single-slot MCP consensus algorithm from the paper.
#[derive(Clone)]
pub struct Chorus {
    slot: Slot,
    key: Arc<KeyPair>,
    validator_data: Arc<ValidatorData>,

    fast: FastPath,
    fallback: Option<FallbackPath>,
    outputs: VecDeque<SlotOutput<Chorus>>,

    // capital Delta from the paper
    delta: TimestampDelta,

    // buffer messages for the fallback path until we enter it.
    buffer: FallbackMsgBuffer,

    // Decided but slot not yet closed, ignore all subsequent messages
    // and timer events.
    decided: bool,
}

impl SlotConsensus for Chorus {
    type Config = ChorusConfig;
    type Context = ChorusContext;
    type Message = Message;
    type Timer = TimerEvent;
    type OptimisticCommitData = FastBlock;
    type FinalizationData = SlotFinalization;
    type DAEvent = ChorusDAEvent;
    type DACommand = ChorusDACommand;

    fn new(slot: Slot, config: &Self::Config, context: &Self::Context) -> Self {
        let ChorusConfig {
            delta,
            num_proposals,
        } = config;
        let ChorusContext {
            key,
            validator_data,
            header_auth,
        } = context;

        let fast = FastPath::new(
            slot,
            *num_proposals,
            key.clone(),
            validator_data.clone(),
            header_auth.clone(),
        );

        Self {
            slot,
            delta: *delta,
            key: key.clone(),
            validator_data: validator_data.clone(),
            fast,
            fallback: None,
            buffer: FallbackMsgBuffer,
            outputs: Default::default(),
            decided: false,
        }
    }

    fn poll(&mut self) -> Option<SlotOutput<Self>> {
        self.outputs.pop_front()
    }

    fn handle_da_event(&mut self, event: ChorusDAEvent) {
        if self.decided {
            return;
        }

        if let Some(fast_block) = self.fast.handle_da_event(event) {
            self.commit_fast_block(fast_block);
        }

        self.drain_da_commands();
    }

    fn handle_message(&mut self, author: NodeId, message: Self::Message) {
        if self.decided {
            return;
        }

        match message {
            Message::BatchVote(batch_vote_msg) => {
                if let Some(fast_block) = self.fast.handle_batch_vote(author, batch_vote_msg) {
                    self.commit_fast_block(fast_block);
                }
            }
            Message::FastCommitVote(fast_commit_vote) => {
                if let Some(fast_qc) = self.fast.handle_commit_vote(author, fast_commit_vote) {
                    self.broadcast(fast_qc.clone());
                    self.finalize(fast_qc);
                }
            }

            Message::FastBlock(fast_block) => {
                if let Some(fast_block) = self.fast.handle_fast_block(fast_block) {
                    self.commit_fast_block(fast_block);
                }
            }

            Message::FallbackVote(fallback_vote_msg) => {
                self.fast.handle_fallback_vote(author, fallback_vote_msg);
            }
            Message::FastCommitQc(qc) => {
                let scope_matches = qc.scope == self.slot;
                if !scope_matches || !qc.verify(&self.validator_data) {
                    return;
                }
                self.broadcast(qc.clone());
                self.finalize(qc);
            }
            Message::EnterFallbackCert(cert) => {
                // a peer certified that 2f+1 validators entered
                // fallback. enter too, building our block from local
                // evidence. if we don't have enough evidence, we will
                // resort to retry mechanism in FallbackDeadline.
                if let Some(inputs) = self.fast.try_build_mvba_inputs(cert) {
                    self.enter_fallback(inputs);
                }
            }
        }

        self.drain_da_commands();
    }

    fn handle_deadline(&mut self) {
        if self.decided {
            return;
        }

        self.schedule_timer(self.delta, TimerEvent::FallbackTransitionTimeout);

        if let Some(batch_vote) = self.fast.on_propose_deadline() {
            self.broadcast(batch_vote);
        }

        self.drain_da_commands();
    }

    fn handle_timer(&mut self, event: Self::Timer) {
        match event {
            TimerEvent::FallbackTransitionTimeout => {
                self.schedule_timer(self.delta, TimerEvent::FallbackDecisionDelayElapsed);

                match self.fast.on_commit_vote_deadline() {
                    CommitVoteDeadlineOutcome::AlreadyVoted => {}
                    CommitVoteDeadlineOutcome::NotEnoughVotes => {
                        // not enough valid votes, wait for more votes to arrive
                        self.schedule_timer(self.delta, TimerEvent::FallbackTransitionTimeout);
                    }
                    CommitVoteDeadlineOutcome::FallbackVote(fallback_vote) => {
                        self.broadcast(fallback_vote);
                    }
                }
            }
            TimerEvent::FallbackDecisionDelayElapsed => match self.fast.on_fallback_deadline() {
                None => {
                    // not enough evidence yet, wait for more messages to arrive
                    self.schedule_timer(self.delta, TimerEvent::FallbackDecisionDelayElapsed);
                    // todo: maybe rebroadcast fallback vote?
                }
                Some(inputs) => {
                    // we formed the fallback certificate locally; disseminate
                    // it so lagging validators can enter without re-deriving it.
                    self.broadcast(inputs.enter_fallback_cert.clone());
                    self.enter_fallback(inputs);
                }
            },
            TimerEvent::FallbackTick => {
                assert!(self.fallback.is_some());
                self.schedule_timer(self.delta, TimerEvent::FallbackTick);
                self.fallback.as_mut().unwrap().on_tick();
            }
        }

        self.drain_da_commands();
    }
}

impl Chorus {
    // speculatively commit a newly completed fast block, cast our
    // commit vote, and disseminate the block for peers to adopt
    fn commit_fast_block(&mut self, fast_block: FastBlock) {
        let commit_vote = fast_block.commit_vote(self.slot, &self.key);
        self.push(SlotOutput::CommitOptimistic(fast_block.clone()));
        self.broadcast(commit_vote);
        self.broadcast(fast_block);
    }

    fn drain_da_commands(&mut self) {
        while let Some(command) = self.fast.next_da_command() {
            self.push(SlotOutput::DA(command));
        }
    }

    // helpers mostly for documentation purpose
    fn broadcast(&mut self, msg: impl Into<Message>) {
        self.push(SlotOutput::Broadcast(msg.into()));
    }
    fn schedule_timer(&mut self, delta: TimestampDelta, event: TimerEvent) {
        self.push(SlotOutput::ScheduleTimer(delta, event));
    }
    fn finalize(&mut self, cert: impl Into<SlotFinalization>) {
        if self.decided {
            // idempotent
            return;
        }

        self.decided = true;
        self.push(SlotOutput::Finalize(cert.into()));
    }
    fn enter_fallback(&mut self, inputs: MVBAInputs) {
        if self.fallback.is_some() {
            // already in the fallback path.
            return;
        }
        self.schedule_timer(self.delta, TimerEvent::FallbackTick);
        self.fallback = Some(self.fast.spawn_fallback(inputs));
    }
    fn push(&mut self, out: SlotOutput<Chorus>) {
        self.outputs.push_back(out);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        super::types::{HeaderAuth, NodeId, Slot, Stake, TimestampDelta, ValidatorData},
        *,
    };
    use crate::spec::vote::KeyPair as _;

    fn validator_data(n: u64) -> ValidatorData {
        let validators = (0..n).map(NodeId::dummy).collect::<Vec<_>>();
        let valset = validators.iter().map(|id| (*id, Stake::from(1))).collect();
        let mapping = validators
            .iter()
            .map(|id| (*id, id.keypair().pubkey()))
            .collect();

        ValidatorData::new(valset, mapping)
    }

    #[test]
    fn deadline_emits_slot_voted_command() {
        let config = ChorusConfig {
            delta: TimestampDelta::from_millis(100),
            num_proposals: 2,
        };
        let context = ChorusContext {
            key: Arc::new(NodeId::dummy(0).keypair()),
            validator_data: Arc::new(validator_data(4)),
            header_auth: Arc::new(HeaderAuth),
        };
        let mut chorus = Chorus::new(Slot(1), &config, &context);

        chorus.handle_deadline();

        let mut slot_voted = None;
        while let Some(output) = chorus.poll() {
            if let SlotOutput::DA(ChorusDACommand::SlotVoted { positive }) = output {
                slot_voted = Some(positive);
            }
        }

        // nothing is available, so the batch vote is all-negative
        assert_eq!(slot_voted, Some(vec![]));
    }
}
