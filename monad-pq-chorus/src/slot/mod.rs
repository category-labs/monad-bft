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

//! Quantum Chorus (Alg. 1) as a `SlotConsensus`. The ledger-visible
//! finalization is `CommitOptimistic(PqEvent::Final)`; `Finalize` closes
//! the slot at halt (D4).

pub(crate) mod availability;
pub mod evidence;
mod gadget;
mod retransmit;
mod second_round;
#[cfg(test)]
pub(crate) mod test_util;
#[cfg(test)]
mod tests;
mod votes;

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
};

use availability::ProposalAvailability;
use evidence::{Evidence, EvidenceLog};
use gadget::FinalizedTally;
use monad_mcp_chorus::{
    spec::ProposalHeader as _,
    stub::{
        SlotConsensus, SlotOutput,
        types::{EquivCert, ProposerSet},
    },
};
use retransmit::Retransmit;
use second_round::SecondRound;
use tracing::{debug, error, warn};
use votes::VoteTally;

use crate::{
    coda::{Coda, CodaOutput},
    config::{QuantumConfig, QuantumContext, ReleaseChunks},
    da::{ChorusDAEvent, PqDACommand, ProposalDAEvent},
    finalization::{PqEvent, PqHalt, PqPath},
    message::{PathVoteBody, PqMessage, VoteBody},
    quorum::Quorum,
    sig::{PqSigner, PqVerifier, Signed},
    types::{
        CodaValue, Entry, Metablock, NodeId, PathEntry, ProposalId, ProposalIndex, ProposalMap,
        Slot, TimestampDelta, coda_output_to_metablock,
    },
};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum PqTimer {
    /// D + Δ
    PathVoteGate,
    /// D + Δ + decode_grace
    DecodeGrace,
    /// D + 2Δ
    Adoption,
    /// 0: D + 2Δ + decode_grace (`lem:qv:acc-half`), 1: D + 4Δ +
    /// decode_grace (`lem:qv:acc-f`)
    Accountability(u8),
    /// the retransmission chain's generation
    Retransmit(u32),
    /// the Coda timeout of a view
    Coda(u64),
}

// once-flags of the time guards; duplicate timer firings are no-ops (D17)
#[derive(Default)]
struct Gates {
    deadline_passed: bool,
    path_vote: bool,
    decode_grace_over: bool,
    adopt: bool,
    accountability: [bool; 2],
}

pub struct QuantumChorus {
    slot: Slot,
    me: NodeId,
    cfg: QuantumConfig,
    q: Quorum,
    k: usize,
    signer: Arc<dyn PqSigner>,
    verifier: Arc<dyn PqVerifier>,
    validators: Arc<[NodeId]>,
    // None: the schedule could not name the proposers; the slot is inert
    proposers: Option<ProposerSet>,

    // Alg. 1 local variables
    chunk: Vec<Option<ProposalId>>,
    path_vote: Option<ProposalMap<PathEntry>>,
    adopted: Option<Metablock>,
    finalized: Option<(Metablock, PqPath)>,
    halted: bool,
    sent_vote: bool,
    speculated: bool,
    released: bool,
    votes: BTreeMap<NodeId, Signed<VoteBody>>,
    path_votes: BTreeMap<NodeId, Signed<PathVoteBody>>,

    // the root chosen for the decode case of the path vote, fixed once made
    fallback_choice: Vec<Option<ProposalId>>,
    tally: VoteTally,
    second: SecondRound,
    gadget: FinalizedTally,
    da: Vec<ProposalAvailability>,
    // positive voters last announced with PullRoot, per (j, root)
    pulled: HashMap<(ProposalIndex, ProposalId), usize>,
    // announced roots not yet resolved, re-pulled on every retransmission
    unresolved_pulls: Vec<(ProposalIndex, ProposalId)>,
    // roots voted for by f+1 whose header has not been seen
    missing_headers: Vec<(ProposalIndex, ProposalId)>,
    gates: Gates,
    coda: Coda<Entry>,
    retransmit: Retransmit,
    evidence: EvidenceLog,
    outputs: VecDeque<SlotOutput<QuantumChorus>>,
}

impl SlotConsensus for QuantumChorus {
    type Config = QuantumConfig;
    type Context = QuantumContext;
    type Message = PqMessage;
    type Timer = PqTimer;
    type OptimisticCommitData = PqEvent;
    type FinalizationData = PqHalt;
    type DAEvent = ChorusDAEvent;
    type DACommand = PqDACommand;

    fn new(slot: Slot, cfg: &QuantumConfig, ctx: &QuantumContext) -> Self {
        let proposers = match ctx.proposers.proposers_at(slot) {
            Ok(proposers) => Some(proposers),
            Err(err) => {
                // a wiring error: this slot never completes
                error!(?slot, %err, "proposer schedule unavailable, slot inert");
                None
            }
        };
        let k = proposers
            .as_ref()
            .map_or(ctx.proposers.num_indices(), ProposerSet::num_indices);
        let q = ctx.quorum();
        Self {
            slot,
            me: ctx.node_id,
            cfg: cfg.clone(),
            q,
            k,
            signer: ctx.signer.clone(),
            verifier: ctx.verifier.clone(),
            validators: ctx.validators().clone(),
            proposers,
            chunk: vec![None; k],
            path_vote: None,
            adopted: None,
            finalized: None,
            halted: false,
            sent_vote: false,
            speculated: false,
            released: false,
            votes: BTreeMap::new(),
            path_votes: BTreeMap::new(),
            fallback_choice: vec![None; k],
            tally: VoteTally::new(k),
            second: SecondRound::new(q, k),
            gadget: FinalizedTally::default(),
            da: vec![ProposalAvailability::default(); k],
            pulled: HashMap::new(),
            unresolved_pulls: Vec::new(),
            missing_headers: Vec::new(),
            gates: Gates::default(),
            coda: Coda::new(slot, ctx.node_id, ctx.validators().clone(), q, k),
            retransmit: Retransmit::default(),
            evidence: EvidenceLog::default(),
            outputs: VecDeque::new(),
        }
    }

    fn handle_deadline(&mut self) {
        if self.inactive() || self.gates.deadline_passed {
            return;
        }
        self.gates.deadline_passed = true;
        let delta = self.cfg.delta;
        let grace = self.cfg.decode_grace;
        self.schedule(delta, PqTimer::PathVoteGate);
        self.schedule(delta + grace, PqTimer::DecodeGrace);
        self.schedule(delta * 2, PqTimer::Adoption);
        self.schedule(delta * 2 + grace, PqTimer::Accountability(0));
        self.schedule(delta * 4 + grace, PqTimer::Accountability(1));
        self.settle();
    }

    fn handle_message(&mut self, sender: NodeId, message: PqMessage) {
        if self.inactive() {
            return;
        }
        match message {
            PqMessage::Vote(vote) => self.on_vote(sender, vote),
            PqMessage::PathVote(path_vote) => self.on_path_vote(sender, path_vote),
            PqMessage::Finalized(metablock) => self.on_finalized(sender, metablock),
            PqMessage::Coda(message) => self.coda.handle_message(sender, message),
        }
        self.settle();
    }

    fn handle_timer(&mut self, timer: PqTimer) {
        if self.inactive() {
            return;
        }
        match timer {
            PqTimer::PathVoteGate => self.gates.path_vote = true,
            PqTimer::DecodeGrace => self.gates.decode_grace_over = true,
            PqTimer::Adoption => self.gates.adopt = true,
            PqTimer::Accountability(round) => self.on_accountability(round),
            PqTimer::Retransmit(generation) => self.on_retransmit(generation),
            PqTimer::Coda(view) => self.coda.on_timeout(view),
        }
        self.settle();
    }

    // ln:qv:chunk
    fn handle_da_event(&mut self, event: ChorusDAEvent) {
        if self.inactive() {
            return;
        }
        let ChorusDAEvent { j, event } = event;
        if j >= self.k || self.vacant(j) {
            return;
        }
        if self.cfg.release_chunks == ReleaseChunks::Receipt {
            self.release_chunks();
        }
        let seen = match &event {
            ProposalDAEvent::HeaderSeen(header) => Some(header.pid()),
            _ => None,
        };
        if let Some(EquivCert(a, b)) = self.da[j].ingest(event) {
            self.file(Evidence::EquivocatingHeaders { j, a, b });
        }
        if let Some(pid) = seen {
            self.missing_headers.retain(|missing| *missing != (j, pid));
        }
        if !self.gates.deadline_passed && self.chunk[j].is_none() {
            self.chunk[j] = self.da[j].fetch_proposal().map(|header| header.pid());
        }
        self.settle();
    }

    fn poll(&mut self) -> Option<SlotOutput<Self>> {
        self.outputs.pop_front()
    }
}

impl QuantumChorus {
    fn inactive(&self) -> bool {
        self.halted || self.proposers.is_none()
    }

    // M4: no proposer may propose at a vacant index; its entry is fixed ⊥
    fn vacant(&self, j: ProposalIndex) -> bool {
        self.proposers
            .as_ref()
            .is_none_or(|proposers| proposers.proposer(j).is_none())
    }

    fn is_validator(&self, node: &NodeId) -> bool {
        self.validators.binary_search(node).is_ok()
    }

    fn roots_only_at_occupied(&self, values: impl Iterator<Item = Entry>) -> bool {
        values
            .enumerate()
            .all(|(j, value)| value == Entry::Bot || !self.vacant(j))
    }

    // every rule fires as soon as its condition holds
    fn settle(&mut self) {
        while !self.halted {
            let mut changed = self.try_vote();
            changed |= self.try_accept();
            changed |= self.try_path_vote();
            changed |= self.try_speculate();
            changed |= self.try_fast_finalize();
            changed |= self.try_adopt();
            changed |= self.drain_coda();
            changed |= self.try_gadget();
            if !changed {
                break;
            }
        }
    }

    // ln:qv:vote, at D or early once every occupied index has a chunk
    fn try_vote(&mut self) -> bool {
        if self.sent_vote {
            return false;
        }
        let early =
            self.cfg.early_vote && (0..self.k).all(|j| self.vacant(j) || self.chunk[j].is_some());
        if !self.gates.deadline_passed && !early {
            return false;
        }
        self.sent_vote = true;
        let entries = ProposalMap::new(self.k, |j| self.chunk[j].map_or(Entry::Bot, Entry::Root));
        let vote = Signed::sign(
            self.slot,
            self.me,
            VoteBody { entries },
            self.signer.as_ref(),
        );
        debug!(slot = ?self.slot, early = !self.gates.deadline_passed, "vote cast");
        self.broadcast(PqMessage::Vote(vote));
        if self.cfg.release_chunks == ReleaseChunks::Vote {
            self.release_chunks();
        }
        true
    }

    // ln:qv:allornothing, without chunks (D1, D2)
    fn on_vote(&mut self, sender: NodeId, vote: Signed<VoteBody>) {
        if !self.is_validator(&sender) || vote.body.entries.size() != self.k {
            return;
        }
        if let Some(first) = self.votes.get(&sender) {
            if *first != vote && vote.verify(self.slot, sender, self.verifier.as_ref()) {
                let evidence = Evidence::ConflictingVotes {
                    signer: sender,
                    a: first.clone(),
                    b: vote,
                };
                self.file(evidence);
            }
            return;
        }
        if !vote.verify(self.slot, sender, self.verifier.as_ref()) {
            debug!(slot = ?self.slot, ?sender, "vote with an invalid signature");
            return;
        }
        if !self.roots_only_at_occupied(vote.body.entries.as_ref().into_iter().copied()) {
            debug!(slot = ?self.slot, ?sender, "vote with a root at a vacant index");
            return;
        }
        let positives = self.tally.add(sender, &vote.body.entries);
        self.votes.insert(sender, vote);
        for (j, pid) in positives {
            self.request_root(j, pid);
        }
    }

    // §4.2: pull a root with f+1 positive voters, and its header if unseen
    fn request_root(&mut self, j: ProposalIndex, pid: ProposalId) {
        if self.tally.positive(j, &pid) < self.q.weak {
            return;
        }
        let voters = self.peer_voters(j, &pid);
        let announced = self.pulled.entry((j, pid)).or_default();
        if !self.da[j].is_resolved(&pid) && voters.len() > *announced {
            if *announced == 0 {
                self.unresolved_pulls.push((j, pid));
            }
            *announced = voters.len();
            let command = PqDACommand::PullRoot { j, pid, voters };
            self.outputs.push_back(SlotOutput::DA(command));
        }
        if self.da[j].header_for(&pid).is_none() && !self.missing_headers.contains(&(j, pid)) {
            self.missing_headers.push((j, pid));
            self.pull_header(j, pid);
        }
    }

    // the positive voters of (j, pid) to pull from: everyone but us
    fn peer_voters(&self, j: ProposalIndex, pid: &ProposalId) -> Vec<NodeId> {
        self.tally
            .positive_voters(j, pid)
            .into_iter()
            .flatten()
            .copied()
            .filter(|voter| *voter != self.me)
            .collect()
    }

    fn pull_header(&mut self, j: ProposalIndex, pid: ProposalId) {
        let holders = self.peer_voters(j, &pid);
        if holders.is_empty() {
            return;
        }
        let command = PqDACommand::PullHeader {
            j,
            missing: vec![(pid, holders.into_iter().collect())],
        };
        self.outputs.push_back(SlotOutput::DA(command));
    }

    fn on_path_vote(&mut self, sender: NodeId, path_vote: Signed<PathVoteBody>) {
        if !self.is_validator(&sender) || path_vote.body.entries.size() != self.k {
            return;
        }
        if let Some(first) = self.path_votes.get(&sender) {
            if *first != path_vote && path_vote.verify(self.slot, sender, self.verifier.as_ref()) {
                let evidence = Evidence::ConflictingPathVotes {
                    signer: sender,
                    a: first.clone(),
                    b: path_vote,
                };
                self.file(evidence);
            }
            return;
        }
        if !path_vote.verify(self.slot, sender, self.verifier.as_ref()) {
            debug!(slot = ?self.slot, ?sender, "path vote with an invalid signature");
            return;
        }
        let values = path_vote
            .body
            .entries
            .as_ref()
            .into_iter()
            .map(PathEntry::value);
        if !self.roots_only_at_occupied(values) {
            debug!(slot = ?self.slot, ?sender, "path vote with a root at a vacant index");
            return;
        }
        self.second.add(sender, &path_vote.body.entries);
        self.path_votes.insert(sender, path_vote);
    }

    // ln:qv:accept: a fast entry is corroborated by f+1 matching votes; a
    // root additionally needs DA to have resolved it (D1)
    fn try_accept(&mut self) -> bool {
        let (tally, da, weak) = (&self.tally, &self.da, self.q.weak);
        self.second.try_accept(|j, value| match value {
            Entry::Root(pid) => tally.positive(j, &pid) >= weak && da[j].is_resolved(&pid),
            Entry::Bot => tally.negative(j) >= weak,
        })
    }

    // ln:qv:pathvote, at D + Δ with 2f+1 votes, or early when every index
    // has 2f+1 matching entries
    fn try_path_vote(&mut self) -> bool {
        if self.path_vote.is_some() {
            return false;
        }
        let strong = self.q.strong;
        let all_fast = (0..self.k).all(|j| self.tally.quorum_value(j, strong).is_some());
        let gate = self.gates.path_vote && self.votes.len() >= strong;
        if !all_fast && !gate {
            return false;
        }
        let mut entries = Vec::with_capacity(self.k);
        for j in 0..self.k {
            match self.path_entry(j) {
                Some(entry) => entries.push(entry),
                // waiting for an f+1-voted root to resolve
                None => return false,
            }
        }
        let entries = ProposalMap::new(self.k, |j| entries[j]);
        self.path_vote = Some(entries.clone());
        let path_vote = Signed::sign(
            self.slot,
            self.me,
            PathVoteBody { entries },
            self.signer.as_ref(),
        );
        debug!(slot = ?self.slot, all_fast, "path vote cast");
        self.broadcast(PqMessage::PathVote(path_vote));
        true
    }

    fn path_entry(&mut self, j: ProposalIndex) -> Option<PathEntry> {
        // ln:qv:fastroot, ln:qv:fastbot
        if let Some(value) = self.tally.quorum_value(j, self.q.strong) {
            return Some(PathEntry::Fast(value));
        }
        // ln:qv:decode: the f+1-voted root, fixed once chosen
        if self.fallback_choice[j].is_none() {
            self.fallback_choice[j] = self.tally.leading_root(j, self.q.weak);
        }
        let Some(pid) = self.fallback_choice[j] else {
            return Some(PathEntry::Fallback(Entry::Bot));
        };
        match self.da[j].checked(&pid) {
            Some(value) => Some(PathEntry::Fallback(value)),
            None if self.gates.decode_grace_over => Some(PathEntry::Fallback(Entry::Bot)),
            None => None,
        }
    }

    // ln:qv:speccheck, ln:qv:speculate
    fn try_speculate(&mut self) -> bool {
        if self.speculated || self.finalized.is_some() {
            return false;
        }
        let Some(entries) = &self.path_vote else {
            return false;
        };
        let mut values = Vec::with_capacity(self.k);
        for j in 0..self.k {
            match entries[j] {
                PathEntry::Fallback(_) => {
                    self.speculated = true;
                    return false;
                }
                PathEntry::Fast(Entry::Bot) => values.push(Entry::Bot),
                PathEntry::Fast(Entry::Root(pid)) => match self.da[j].checked(&pid) {
                    Some(value) => values.push(value),
                    None => return false,
                },
            }
        }
        self.speculated = true;
        let metablock = ProposalMap::new(self.k, |j| values[j]);
        self.event(PqEvent::Speculative { metablock });
        true
    }

    // ln:qv:fastcheck, ln:qv:fastdecide
    fn try_fast_finalize(&mut self) -> bool {
        if self.finalized.is_some() {
            return false;
        }
        let mut values = Vec::with_capacity(self.k);
        let mut voters = std::collections::BTreeSet::new();
        for j in 0..self.k {
            let Some((value, senders)) = self.second.fast_quorum(j) else {
                return false;
            };
            let checked = match value {
                Entry::Bot => Some(Entry::Bot),
                Entry::Root(pid) if self.tally.positive(j, &pid) >= self.q.weak => {
                    self.da[j].checked(&pid)
                }
                Entry::Root(_) => None,
            };
            let Some(checked) = checked else {
                return false;
            };
            values.push(checked);
            voters.extend(senders.iter().copied());
        }
        let path_votes = voters
            .into_iter()
            .map(|voter| (voter, self.path_votes[&voter].clone()))
            .collect();
        let metablock = ProposalMap::new(self.k, |j| values[j]);
        self.finalize(metablock, PqPath::Fast, None, path_votes);
        true
    }

    // ln:qv:adopt, ln:qv:input, at D + 2Δ once every index has a clean set
    fn try_adopt(&mut self) -> bool {
        if self.adopted.is_some() || !self.gates.adopt || !self.second.all_clean() {
            return false;
        }
        let mut values = Vec::with_capacity(self.k);
        for j in 0..self.k {
            let value = match self.vacant(j) {
                true => Some(Entry::Bot),
                false => self.second.adopt(j, |pid| self.da[j].checked(pid)),
            };
            match value {
                Some(value) => values.push(value),
                None => return false,
            }
        }
        let adopted = ProposalMap::new(self.k, |j| values[j]);
        debug!(slot = ?self.slot, ?adopted, "entering fallback coda");
        self.coda
            .start(adopted.as_ref().map(|value| CodaValue::Value(*value)));
        self.adopted = Some(adopted);
        true
    }

    fn drain_coda(&mut self) -> bool {
        let mut changed = false;
        while let Some(output) = self.coda.poll() {
            changed = true;
            match output {
                CodaOutput::Broadcast(message) => self.broadcast(PqMessage::Coda(message)),
                CodaOutput::ScheduleTimeout(view) => {
                    self.schedule(self.cfg.coda_timeout(), PqTimer::Coda(view))
                }
                CodaOutput::Abandon { below } => self.retransmit.abandon_coda_below(below),
                // ln:qv:fallbackdecide
                CodaOutput::Decide { metablock, view } => {
                    debug!(slot = self.slot.get(), view, "coda decided");
                    let metablock = coda_output_to_metablock(&metablock);
                    self.finalize(metablock, PqPath::Coda, Some(view), Vec::new());
                }
            }
        }
        changed
    }

    fn on_finalized(&mut self, sender: NodeId, metablock: Metablock) {
        if self.is_validator(&sender)
            && metablock.size() == self.k
            && self.roots_only_at_occupied(metablock.as_ref().into_iter().copied())
        {
            self.gadget.add(sender, metablock);
        }
    }

    // ln:qv:echo, ln:qv:halt
    fn try_gadget(&mut self) -> bool {
        if self.finalized.is_none()
            && let Some(metablock) = self.gadget.reached(self.q.weak).cloned()
        {
            self.finalize(metablock, PqPath::Gadget, None, Vec::new());
        }
        let Some(metablock) = self.gadget.reached(self.q.strong).cloned() else {
            return false;
        };
        if self.finalized.is_none() {
            self.finalize(metablock, PqPath::Gadget, None, Vec::new());
        }
        self.halt();
        true
    }

    fn finalize(
        &mut self,
        metablock: Metablock,
        path: PqPath,
        at_view: Option<u64>,
        path_votes: evidence::SignedPathVotes,
    ) {
        if self.finalized.is_some() {
            return;
        }
        self.finalized = Some((metablock.clone(), path));
        debug!(slot = ?self.slot, ?path, ?metablock, "finalized");
        self.broadcast(PqMessage::Finalized(metablock.clone()));
        for j in 0..self.k {
            let pid = metablock[j].root();
            self.outputs
                .push_back(SlotOutput::DA(PqDACommand::PinFinal { j, pid }));
        }
        for j in 0..self.k {
            let Entry::Root(pid) = metablock[j] else {
                continue;
            };
            // senders of ⟨fallback, pid⟩ decoded it (ln:qv:decode)
            let node_ids: Vec<NodeId> = self
                .path_votes
                .iter()
                .filter(|(_, vote)| vote.body.entries[j] == PathEntry::Fallback(Entry::Root(pid)))
                .map(|(sender, _)| *sender)
                .collect();
            if !node_ids.is_empty() {
                let command = PqDACommand::AddDecodedHolders { j, pid, node_ids };
                self.outputs.push_back(SlotOutput::DA(command));
            }
        }
        self.event(PqEvent::Final {
            metablock,
            path,
            at_view,
            path_votes,
        });
    }

    fn halt(&mut self) {
        if self.halted {
            return;
        }
        let Some((metablock, path)) = self.finalized.clone() else {
            return;
        };
        self.halted = true;
        self.retransmit.stop();
        debug!(slot = ?self.slot, "halted");
        self.outputs
            .push_back(SlotOutput::Finalize(PqHalt { metablock, path }));
    }

    fn on_retransmit(&mut self, generation: u32) {
        let Some((next, alive)) = self.retransmit.fire(generation) else {
            return;
        };
        let alive = alive.to_vec();
        for message in alive {
            self.outputs.push_back(SlotOutput::Broadcast(message));
        }
        let da = &self.da;
        self.unresolved_pulls
            .retain(|(j, pid)| !da[*j].is_resolved(pid));
        for (j, pid) in self.unresolved_pulls.clone() {
            let voters = self.peer_voters(j, &pid);
            let command = PqDACommand::PullRoot { j, pid, voters };
            self.outputs.push_back(SlotOutput::DA(command));
        }
        for (j, pid) in self.missing_headers.clone() {
            self.pull_header(j, pid);
        }
        self.schedule(self.cfg.retransmit_interval, PqTimer::Retransmit(next));
    }

    // §11: evidence for a slot that missed its timely-slot bound
    fn on_accountability(&mut self, round: u8) {
        let Some(done) = self.gates.accountability.get_mut(round as usize) else {
            return;
        };
        if std::mem::replace(done, true) || self.finalized.is_some() {
            return;
        }
        match round {
            0 => self.detect_partial_dissemination(),
            _ => self.detect_fallback_quorum(),
        }
    }

    // lem:qv:acc-half (2): a header with ⌊f/2⌋+1 negative votes
    fn detect_partial_dissemination(&mut self) {
        let threshold = self.q.f / 2 + 1;
        for j in 0..self.k {
            if self.vacant(j) {
                continue;
            }
            let Some(header) = self.da[j].first_header().cloned() else {
                continue;
            };
            let negatives: Vec<_> = self
                .tally
                .negative_voters(j)
                .iter()
                .map(|voter| (*voter, self.votes[voter].clone()))
                .collect();
            if negatives.len() >= threshold {
                self.file(Evidence::PartialDissemination {
                    j,
                    header,
                    negatives,
                });
            }
        }
    }

    // lem:qv:acc-f case 1, and the vote dump for case 2 (D20)
    fn detect_fallback_quorum(&mut self) {
        for j in 0..self.k {
            let Some(proposer) = self.proposers.as_ref().and_then(|p| p.proposer(j)) else {
                continue;
            };
            let path_votes: Vec<_> = self
                .path_votes
                .iter()
                .filter(|(_, vote)| !vote.body.entries[j].is_fast())
                .map(|(sender, vote)| (*sender, vote.clone()))
                .collect();
            if path_votes.len() >= self.q.strong {
                self.file(Evidence::FallbackQuorum {
                    j,
                    proposer,
                    path_votes,
                });
            }
        }
        let votes = self.votes.iter().map(|(s, v)| (*s, v.clone())).collect();
        let path_votes = self
            .path_votes
            .iter()
            .map(|(s, v)| (*s, v.clone()))
            .collect();
        self.file(Evidence::VoteDump { votes, path_votes });
    }

    fn file(&mut self, evidence: Evidence) {
        if !self.evidence.file(&evidence) {
            return;
        }
        warn!(
            slot = self.slot.get(),
            kind = evidence.kind(),
            culprit = ?evidence.culprit(),
            "misbehavior evidence"
        );
        self.event(PqEvent::Evidence(evidence));
    }

    fn release_chunks(&mut self) {
        if !std::mem::replace(&mut self.released, true) {
            self.outputs
                .push_back(SlotOutput::DA(PqDACommand::ReleaseChunks));
        }
    }

    // a broadcast kept alive until halt or abandonment (§7)
    fn broadcast(&mut self, message: PqMessage) {
        self.outputs
            .push_back(SlotOutput::Broadcast(message.clone()));
        if let Some(generation) = self.retransmit.add(message) {
            self.schedule(
                self.cfg.retransmit_interval,
                PqTimer::Retransmit(generation),
            );
        }
    }

    fn event(&mut self, event: PqEvent) {
        self.outputs.push_back(SlotOutput::CommitOptimistic(event));
    }

    fn schedule(&mut self, delta: TimestampDelta, timer: PqTimer) {
        self.outputs
            .push_back(SlotOutput::ScheduleTimer(delta, timer));
    }
}
