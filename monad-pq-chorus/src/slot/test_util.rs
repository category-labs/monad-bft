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

use std::{collections::HashMap, sync::Arc};

use monad_mcp_chorus::{
    spec::{ProposalHeader as _, vote::KeyPair as _},
    stub::{
        SlotConsensus, SlotOutput,
        env::{D25, EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader, ProposalSignature},
        proposers::header_auth,
        types::{ProposerSchedule, SignedProposalHeader, Stake, ValidatorData},
    },
};

use super::{PqTimer, QuantumChorus};
use crate::{
    config::{QuantumConfig, QuantumContext},
    da::{ChorusDAEvent, PqDACommand, ProposalDAEvent},
    finalization::{PqEvent, PqHalt},
    message::{PathVoteBody, PqMessage, VoteBody},
    sig::{Signed, StubPq},
    types::{Entry, NodeId, PathEntry, ProposalId, ProposalMap, Slot, TimestampDelta},
};

pub const SLOT: Slot = Slot(7);
pub const DELTA: TimestampDelta = TimestampDelta::from_millis(100);

/// A header signed by validator `signer`, distinct per `byte`.
pub fn header(slot: Slot, signer: u64, byte: u8) -> SignedProposalHeader {
    SignedProposalHeader {
        header: ProposalHeader {
            root: MerkleRoot(MerkleHash([byte; 20])),
            scheme: EncodingScheme::D25(D25 {
                slot,
                msg_len: 1,
                unix_ts: 0,
                depth: 3,
            }),
        },
        sig: ProposalSignature {
            signer: NodeId::dummy(signer),
            checksum: 0,
        },
    }
}

pub fn validator_data(n: u64) -> Arc<ValidatorData> {
    let ids: Vec<NodeId> = (0..n).map(NodeId::dummy).collect();
    let valset: HashMap<_, _> = ids.iter().map(|id| (*id, Stake::from(1))).collect();
    let mapping = ids.iter().map(|id| (*id, id.keypair().pubkey())).collect();
    Arc::new(ValidatorData::new(valset, mapping))
}

pub fn context(
    me: u64,
    n: u64,
    proposers: Arc<dyn ProposerSchedule + Send + Sync>,
) -> QuantumContext {
    struct Schedule(Arc<dyn ProposerSchedule + Send + Sync>);
    impl ProposerSchedule for Schedule {
        fn num_indices(&self) -> usize {
            self.0.num_indices()
        }
        fn proposers_at(
            &self,
            slot: Slot,
        ) -> Result<
            monad_mcp_chorus::stub::types::ProposerSet,
            monad_mcp_chorus::stub::types::ScheduleError,
        > {
            self.0.proposers_at(slot)
        }
    }
    let scheme = StubPq::default();
    QuantumContext::new(
        NodeId::dummy(me),
        Arc::new(scheme.signer(NodeId::dummy(me))),
        Arc::new(scheme),
        validator_data(n),
        Arc::new(header_auth(Arc::new(Schedule(proposers.clone())))),
        proposers,
    )
    .expect("equal stake")
}

pub fn config() -> QuantumConfig {
    QuantumConfig::with_delta(DELTA)
}

pub fn signed_vote(signer: u64, entries: &[Entry]) -> PqMessage {
    let id = NodeId::dummy(signer);
    let body = VoteBody {
        entries: ProposalMap::new(entries.len(), |j| entries[j]),
    };
    PqMessage::Vote(Signed::sign(SLOT, id, body, &StubPq::default().signer(id)))
}

pub fn signed_path_vote(signer: u64, entries: &[PathEntry]) -> PqMessage {
    let id = NodeId::dummy(signer);
    let body = PathVoteBody {
        entries: ProposalMap::new(entries.len(), |j| entries[j]),
    };
    PqMessage::PathVote(Signed::sign(SLOT, id, body, &StubPq::default().signer(id)))
}

pub fn metablock(entries: &[Entry]) -> ProposalMap<Entry> {
    ProposalMap::new(entries.len(), |j| entries[j])
}

/// The output kinds a test asserts on (`SlotOutput` has no `Debug`).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Out {
    Timer(TimestampDelta, PqTimer),
    Broadcast(PqMessage),
    Da(PqDACommand),
    Event(PqEvent),
    Halt(PqHalt),
}

/// One slot instance of validator `me`, its own broadcasts looped back as
/// the node does.
pub struct Harness {
    pub slot: QuantumChorus,
    pub me: NodeId,
    pub loopback: bool,
}

impl Harness {
    pub fn new(me: u64, n: u64, proposers: Arc<dyn ProposerSchedule + Send + Sync>) -> Self {
        Self::with_config(me, n, proposers, config())
    }

    pub fn with_config(
        me: u64,
        n: u64,
        proposers: Arc<dyn ProposerSchedule + Send + Sync>,
        config: QuantumConfig,
    ) -> Self {
        let context = context(me, n, proposers);
        Self {
            slot: QuantumChorus::new(SLOT, &config, &context),
            me: NodeId::dummy(me),
            loopback: true,
        }
    }

    pub fn drain(&mut self) -> Vec<Out> {
        let mut outs = Vec::new();
        while let Some(output) = self.slot.poll() {
            let out = match output {
                SlotOutput::ScheduleTimer(delta, timer) => Out::Timer(delta, timer),
                SlotOutput::Broadcast(message) => {
                    if self.loopback {
                        self.slot.handle_message(self.me, message.clone());
                    }
                    Out::Broadcast(message)
                }
                SlotOutput::Unicast { .. } => panic!("quantum chorus never unicasts"),
                SlotOutput::DA(command) => Out::Da(command),
                SlotOutput::CommitOptimistic(event) => Out::Event(event),
                SlotOutput::Finalize(halt) => Out::Halt(halt),
                SlotOutput::Fault { reason } => panic!("fault: {reason}"),
            };
            outs.push(out);
        }
        outs
    }

    pub fn deadline(&mut self) -> Vec<Out> {
        self.slot.handle_deadline();
        self.drain()
    }

    pub fn timer(&mut self, timer: PqTimer) -> Vec<Out> {
        self.slot.handle_timer(timer);
        self.drain()
    }

    pub fn message(&mut self, sender: u64, message: PqMessage) -> Vec<Out> {
        self.slot.handle_message(NodeId::dummy(sender), message);
        self.drain()
    }

    pub fn da(&mut self, j: usize, event: ProposalDAEvent) -> Vec<Out> {
        self.slot.handle_da_event(ChorusDAEvent { j, event });
        self.drain()
    }

    /// Header and the proposer's chunks of `header` for index j.
    pub fn chunk(&mut self, j: usize, header: &SignedProposalHeader) -> Vec<Out> {
        let mut outs = self.da(j, ProposalDAEvent::HeaderSeen(header.clone()));
        outs.extend(self.da(
            j,
            ProposalDAEvent::ProposerObligationFulfilled(header.pid()),
        ));
        outs
    }
}

pub fn pid_of(header: &SignedProposalHeader) -> ProposalId {
    header.pid()
}

pub fn votes(outs: &[Out]) -> Vec<&Signed<VoteBody>> {
    outs.iter()
        .filter_map(|out| match out {
            Out::Broadcast(PqMessage::Vote(vote)) => Some(vote),
            _ => None,
        })
        .collect()
}

pub fn path_votes(outs: &[Out]) -> Vec<&Signed<PathVoteBody>> {
    outs.iter()
        .filter_map(|out| match out {
            Out::Broadcast(PqMessage::PathVote(vote)) => Some(vote),
            _ => None,
        })
        .collect()
}

pub fn events(outs: &[Out]) -> Vec<&PqEvent> {
    outs.iter()
        .filter_map(|out| match out {
            Out::Event(event) => Some(event),
            _ => None,
        })
        .collect()
}

pub fn finals(outs: &[Out]) -> Vec<&PqEvent> {
    events(outs)
        .into_iter()
        .filter(|event| matches!(event, PqEvent::Final { .. }))
        .collect()
}

pub fn halts(outs: &[Out]) -> Vec<&PqHalt> {
    outs.iter()
        .filter_map(|out| match out {
            Out::Halt(halt) => Some(halt),
            _ => None,
        })
        .collect()
}

pub fn da_commands(outs: &[Out]) -> Vec<&PqDACommand> {
    outs.iter()
        .filter_map(|out| match out {
            Out::Da(command) => Some(command),
            _ => None,
        })
        .collect()
}
