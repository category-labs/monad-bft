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
    fallback::{
        FallbackCommitQc, MVBAOutput, Metablock, Mvba as _,
        monad_mvba::{self, MonadMvba, MvbaContext},
    },
    fast::{
        BatchVoteMsg, CommitVoteDeadlineOutcome, EnterFallbackCert, Entry, FallbackVoteMsg,
        FastBlock, FastCommitQc, FastCommitVoteMsg, FastPath,
    },
    types::{
        EquivCert, KeyPair, MerkleRoot, NodeId, ProposalIndex, ProposalMap, ProposalMeta, Slot,
        TimestampDelta, ValidatorData,
    },
};

/// The fallback path's agreement protocol, at the instantiation Chorus runs it
type FallbackState = MonadMvba<Metablock, EnterFallbackCert>;
type FallbackMessage = monad_mvba::MvbaMessage<Metablock, EnterFallbackCert>;
type FallbackTimer = monad_mvba::TimerEvent<Metablock>;

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

    // the fallback path's own protocol, dispatched into the MVBA
    #[from]
    Fallback(FallbackMessage),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum TimerEvent {
    // emitted on D_s + Delta
    FallbackTransitionTimeout,
    // emitted on D_s + 2*Delta
    FallbackDecisionDelayElapsed,
    // armed by the MVBA, fed straight back to it
    Fallback(FallbackTimer),
}

/// Static per-slot configuration.
#[derive(Clone)]
pub struct ChorusConfig {
    pub delta: TimestampDelta,
    pub num_proposals: usize,
}

/// Shared resources needed to spawn a slot instance.
pub struct ChorusContext {
    pub node_id: NodeId,
    pub key: Arc<KeyPair>,
    pub validator_data: Arc<ValidatorData>,
}

#[derive(derive_more::From, Clone, PartialEq, Eq, Hash, Debug)]
pub enum SlotFinalization {
    #[from]
    Fast(FastCommitQc),
    #[from]
    Fallback(FallbackCommitQc<Metablock>),
}

impl SlotFinalization {
    /// The verdict committed for each proposer, whichever path decided
    pub fn entries(&self) -> ProposalMap<Entry> {
        match self {
            SlotFinalization::Fast(qc) => qc.verdict.entries.clone(),
            SlotFinalization::Fallback(qc) => qc.verdict.0.clone(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ChorusDAEvent {
    pub j: ProposalIndex,
    pub event: ProposalDAEvent,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum ProposalDAEvent {
    // a validated proposer-signed header observed (once per root)
    HeaderSeen(ProposalMeta),

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

    // report header learned from consensus evidence
    ObserveProposal {
        j: ProposalIndex,
        meta: ProposalMeta,
    },
}

/// The single-slot MCP consensus algorithm from the paper.
pub struct Chorus {
    slot: Slot,
    key: Arc<KeyPair>,

    fast: FastPath,
    /// memory is unbounded until this validator proposes into it: the MVBA
    /// buffers what arrives without entering views or garbage-collecting. I
    /// have a proposal to conditionally start fallback without self propose
    fallback: FallbackState,
    outputs: VecDeque<SlotOutput<Chorus>>,

    // capital Delta from the paper
    delta: TimestampDelta,

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
            node_id,
            key,
            validator_data,
        } = context;

        let fast = FastPath::new(slot, *num_proposals, key.clone(), validator_data.clone());

        let fallback = FallbackState::new(MvbaContext {
            slot,
            num_proposals: *num_proposals,
            delta: *delta,
            node_id: *node_id,
            key: key.clone(),
            validator_data: validator_data.clone(),
        });

        Self {
            slot,
            delta: *delta,
            key: key.clone(),
            fast,
            fallback,
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
            self.vote_fast_block(fast_block);
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
                    self.vote_fast_block(fast_block);
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
                    self.vote_fast_block(fast_block);
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
                // an unsolicited certificate is checked before it is acted
                // on: entering the fallback path on a forged one would leave
                // this validator alone in an MVBA nobody else runs.

                // TODO: the retry re-derives the cert from our own vote pool
                // and this one is dropped; if the votes never arrive we hold a
                // valid cert yet never enter. Keep it for the deadline path.
                if self.fast.enter_fallback_cert_is_valid(&cert)
                    && let Some(block) = self.fast.try_build_fallback_block()
                {
                    self.enter_fallback(cert, block);
                }
            }

            Message::Fallback(message) => {
                self.fallback.handle_message(author, message);
                self.drain_fallback();
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
                Some((cert, block)) => {
                    // we formed the fallback certificate locally; disseminate
                    // it so lagging validators can enter without re-deriving it.
                    self.broadcast(cert.clone());
                    self.enter_fallback(cert, block);
                }
            },
            TimerEvent::Fallback(event) => {
                self.fallback.handle_timer(event);
                self.drain_fallback();
            }
        }

        self.drain_da_commands();
    }
}

impl Chorus {
    /// speculatively commit a newly completed fast block, cast our commit
    /// vote, and disseminate the block for peers to adopt
    fn vote_fast_block(&mut self, fast_block: FastBlock) {
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
        // TODO: introduce fast path handling of fallback commit
        if self.decided {
            // idempotent
            return;
        }

        self.decided = true;
        self.fallback.abandon();
        self.push(SlotOutput::Finalize(cert.into()));
    }

    /// `propose` is idempotent, so a second certificate changes nothing
    fn enter_fallback(&mut self, cert: EnterFallbackCert, block: Metablock) {
        self.fallback.propose(block, Some(cert));
        self.drain_fallback();
    }

    /// The MVBA arms its own timers and asks for its own sends; this layer only
    /// converts them, and finalizes once a commit certificate is complete
    fn drain_fallback(&mut self) {
        // drained first: finalize -> abandon clears the MVBA's output queue,
        // which holds the decide-broadcast certificate
        while let Some(output) = self.fallback.poll() {
            match output {
                MVBAOutput::Broadcast(message) => self.broadcast(message),
                MVBAOutput::Unicast { to, message } => self.push(SlotOutput::Unicast {
                    to,
                    message: message.into(),
                }),
                MVBAOutput::ScheduleTimer {
                    duration,
                    timer_event,
                } => self.schedule_timer(duration, TimerEvent::Fallback(timer_event)),
            }
        }

        if let Some(qc) = self.fallback.decision_proof() {
            let qc = qc.clone();
            self.finalize(qc);
        }
    }

    fn push(&mut self, out: SlotOutput<Chorus>) {
        self.outputs.push_back(out);
    }
}
