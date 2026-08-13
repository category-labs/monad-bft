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
        DAHandle, KeyPair, NodeId, ProposalIndex, ProposalMeta, Slot, TimestampDelta, ValidatorData,
    },
};

// emitted from the DA layer upon validating a proposal
// chunk. possibly only emitted after a significant fraction of
// the chunks for first-hop-recipient is available.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Proposal {
    pub j: ProposalIndex,
    pub meta: ProposalMeta,
}

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
    pub da_handle: Arc<DAHandle>,
}

#[derive(derive_more::From, Clone, PartialEq, Eq, Hash, Debug)]
pub enum SlotFinalization {
    #[from]
    Fast(FastCommitQc),
    // Fallback(FallbackCommitQc),
}

#[derive(Clone)]
struct FallbackMsgBuffer;

/// The single-slot MCP consensus algorithm from the paper.
#[derive(Clone)]
pub struct Chorus {
    slot: Slot,
    key: Arc<KeyPair>,

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

    fn new(slot: Slot, config: &Self::Config, context: &Self::Context) -> Self {
        let ChorusConfig {
            delta,
            num_proposals,
        } = config;
        let ChorusContext {
            key,
            validator_data,
            da_handle,
        } = context;

        let fast = FastPath::new(
            slot,
            *num_proposals,
            key.clone(),
            validator_data.clone(),
            da_handle.clone(),
        );

        Self {
            slot,
            delta: *delta,
            key: key.clone(),
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

    fn handle_message(&mut self, author: NodeId, message: Self::Message) {
        if self.decided {
            return;
        }

        match message {
            Message::BatchVote(batch_vote_msg) => {
                if let Some(fast_block) = self.fast.handle_batch_vote(author, batch_vote_msg) {
                    let commit_vote = fast_block.commit_vote(self.slot, &self.key);
                    self.push(SlotOutput::CommitOptimistic(fast_block.clone()));
                    self.broadcast(commit_vote);
                    self.broadcast(fast_block);
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
                    let commit_vote = fast_block.commit_vote(self.slot, &self.key);
                    self.push(SlotOutput::CommitOptimistic(fast_block));
                    self.broadcast(commit_vote);
                }
            }

            Message::FallbackVote(fallback_vote_msg) => {
                self.fast.handle_fallback_vote(author, fallback_vote_msg);
            }
            Message::FastCommitQc(qc) => {
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
    }

    fn handle_deadline(&mut self) {
        if self.decided {
            return;
        }

        self.schedule_timer(self.delta, TimerEvent::FallbackTransitionTimeout);

        if let Some(batch_vote) = self.fast.on_propose_deadline() {
            self.broadcast(batch_vote);
        }
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
    }
}

impl Chorus {
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
        // FIXME: MVBA should be responsible for scheduling its own timer
        self.schedule_timer(self.delta, TimerEvent::FallbackTick);
        self.fallback = Some(self.fast.spawn_fallback(inputs));
    }
    fn push(&mut self, out: SlotOutput<Chorus>) {
        self.outputs.push_back(out);
    }
}
