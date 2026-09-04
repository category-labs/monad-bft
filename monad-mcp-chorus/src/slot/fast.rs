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
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use bytes::Bytes;
use itertools::Either;

use super::{
    availability::ProposalAvailability,
    chorus::{ChorusDACommand, ChorusDAEvent, ChunkRequestType, ProposalDAEvent},
    fallback::Metablock,
    types::{
        Admission, EquivCert, GatedVotePool, GatingRoot, HeaderAuth, IsVote, KeyPair, MerkleRoot,
        NodeId, ProposalHeader, ProposalIndex, ProposalMap, Signature, Slot, StrongQc,
        TotalProposalMap, ValidatorData, VoteMsg, VotePool, WeakQc, dummy_serialize,
    },
};
use crate::spec::{
    Stake as _, proposal::HeaderAuth as _, validator::ValidatorData as _,
    vote::SignatureCollection as _,
};

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum Phase {
    Propose,              // initial phase
    Vote,                 // vote casted
    FastCommit,           // fast commit vote cast
    TransitionToFallback, // fallback vote cast
}

pub enum CommitVoteDeadlineOutcome {
    AlreadyVoted,   // already voted reactively
    NotEnoughVotes, // not yet have 2f+1 votes
    FallbackVote(FallbackVoteMsg),
}

#[derive(Clone)]
pub struct FastPath {
    slot: Slot,

    certs: ProposalMap<LocalCertifiedEntry>,
    phase: Phase,

    votes: ProposalMap<GatedVotePool<Entry>>,
    commit_votes: VotePool<FastCommitVote>,

    enter_fallback_votes: VotePool<EnterFallbackVote>,
    fallback_entry_votes: ProposalMap<GatedVotePool<FallbackEntry>>,

    // helper field to construct per-proposal values
    proposals: ProposalMap<ProposalIndex>,

    // what we know about each proposal's DA
    availability: ProposalMap<ProposalAvailability>,

    // effects for the DA layer, drained by the slot consensus wrapper
    commands: VecDeque<ChorusDACommand>,

    // using Arc to avoid lifetime issues.
    key: Arc<KeyPair>,
    validator_data: Arc<ValidatorData>,
    header_auth: Arc<HeaderAuth>,
}

impl FastPath {
    pub(crate) fn new(
        s: Slot,
        num_proposals: usize,
        key: Arc<KeyPair>,
        validator_data: Arc<ValidatorData>,
        header_auth: Arc<HeaderAuth>,
    ) -> Self {
        Self {
            slot: s,

            votes: ProposalMap::new(num_proposals, |j| GatedVotePool::new(VotePool::new((s, j)))),
            certs: ProposalMap::new_default(num_proposals),
            commit_votes: VotePool::new(s),

            enter_fallback_votes: VotePool::new(s),
            fallback_entry_votes: ProposalMap::new(num_proposals, |j| {
                GatedVotePool::new(VotePool::new((s, j)))
            }),

            phase: Phase::Propose,
            proposals: ProposalMap::new(num_proposals, |j| j),
            availability: ProposalMap::new_default(num_proposals),
            commands: VecDeque::new(),

            key,
            validator_data,
            header_auth,
        }
    }

    pub(crate) fn next_da_command(&mut self) -> Option<ChorusDACommand> {
        self.commands.pop_front()
    }

    fn emit(&mut self, command: ChorusDACommand) {
        self.commands.push_back(command);
    }

    // admission of gated votes can complete the fast block
    #[must_use]
    pub(crate) fn handle_da_event(&mut self, event: ChorusDAEvent) -> Option<FastBlock> {
        let ChorusDAEvent { j, event } = event;

        match &event {
            ProposalDAEvent::OwnerObligationFulfilled { owner, root } => {
                self.votes[j].open(*owner, *root);
            }
            ProposalDAEvent::Decoded(root) => {
                // decode implies possession of every chunk, so it
                // satisfies both admission gates
                self.votes[j].open_all(*root);
                self.fallback_entry_votes[j].open_all(*root);
            }
            ProposalDAEvent::ProposerObligationFulfilled(root) => {
                self.fallback_entry_votes[j].open_all(*root);
            }
            _ => {}
        }

        if let Some(cert) = self.availability[j].ingest(event) {
            self.certs[j].try_upgrade(cert);
        }

        self.try_form_fast_qc(j);
        self.try_form_fallback_qc(j);
        self.try_cast_fast_commit_vote()
    }

    /// Whether a fallback certificate received from a peer admits this slot
    /// to the fallback path: it is scoped to this slot and its signatures
    /// verify. Checked here because the certificate no longer rides inside the
    /// MVBA input, where admission used to be re-checked on every proposal.
    pub(crate) fn enter_fallback_cert_is_valid(&self, cert: &EnterFallbackCert) -> bool {
        cert.scope == self.slot && cert.verify(&self.validator_data)
    }

    pub(crate) fn handle_batch_vote(
        &mut self,
        node_id: NodeId,
        vote_msg: BatchVoteMsg,
    ) -> Option<FastBlock> {
        let shape_valid =
            vote_msg.slot == self.slot && vote_msg.votes.size() == self.proposals.size();
        if !shape_valid {
            return None;
        }

        for vote_msg in vote_msg.split() {
            self.handle_vote(node_id, vote_msg);
        }

        self.try_cast_fast_commit_vote()
    }

    fn handle_vote(&mut self, node_id: NodeId, vote_msg: VoteMsg<Entry>) {
        debug_assert!(self.validator_data.contains(&node_id));

        let (_s, j) = vote_msg.scope;
        self.votes[j].add_vote(node_id, vote_msg);
        self.try_form_fast_qc(j);
    }

    #[must_use]
    pub(crate) fn handle_commit_vote(
        &mut self,
        node_id: NodeId,
        vote_msg: FastCommitVoteMsg,
    ) -> Option<FastCommitQc> {
        debug_assert!(self.validator_data.contains(&node_id));

        if vote_msg.scope != self.slot {
            return None;
        }

        // no phase guard on purpose
        self.commit_votes.add_vote(node_id, vote_msg);
        self.commit_votes.try_form_strong_qc(&self.validator_data)
    }

    pub(crate) fn handle_fast_block(&mut self, fast_block: FastBlock) -> Option<FastBlock> {
        let all_qcs_valid = fast_block
            .0
            .as_ref()
            .into_iter()
            .all(|qc| qc.verify(&self.validator_data));
        if !all_qcs_valid {
            return None;
        }

        for (j, qc) in fast_block.0.into_indexed_iter() {
            if self.certs[j].try_upgrade(qc) {
                self.recover_certified(j);
            }
        }

        self.try_cast_fast_commit_vote()
    }

    pub(crate) fn handle_fallback_vote(&mut self, node_id: NodeId, vote_msg: FallbackVoteMsg) {
        debug_assert!(self.validator_data.contains(&node_id));

        let shape_valid = vote_msg.enter_fallback_vote.scope == self.slot
            && vote_msg.evidences.size() == self.proposals.size();
        if !shape_valid {
            return;
        }

        // admission is all-or-nothing: any invalid evidence rejects the
        // whole vote, including its enter-fallback part.
        let all_evidences_valid = vote_msg
            .evidences
            .as_ref()
            .into_indexed_iter()
            .all(|(j, evidence)| self.evidence_valid(j, evidence));
        if !all_evidences_valid {
            return;
        }

        self.enter_fallback_votes
            .add_vote(node_id, vote_msg.enter_fallback_vote);

        for (j, evidence) in vote_msg.evidences.into_indexed_iter() {
            match evidence {
                ProposalEvidence::FallbackSignedEntry(entry) => {
                    if let Some(header) = entry.header() {
                        // positive fallback entry
                        let avail = &mut self.availability[j];
                        if let Some(equiv_cert) = avail.record_header(header.clone()) {
                            self.certs[j].try_upgrade(equiv_cert);
                        }
                    }

                    let positive_root = entry.header().map(|header| header.root);
                    let vote = entry.into_vote_msg(self.slot, j);
                    let admission = self.fallback_entry_votes[j].add_vote(node_id, vote);
                    if admission == Admission::Held
                        && let Some(root) = positive_root
                    {
                        // P2: the signer holds the decoded proposal,
                        // so it can serve our own chunks under the root
                        self.request_chunks(ChunkRequestType::MyChunks, j, root, vec![node_id]);
                    }
                    self.try_form_fallback_qc(j);
                }

                ProposalEvidence::Certified(cert) => {
                    if self.certs[j].try_upgrade(cert) {
                        self.recover_certified(j);
                    }
                }
            }
        }
    }

    // P1: pull a newly certified root from the certificate's signers.
    // A positive FallbackQc's signers also hold the decoded proposal,
    // so they can serve our own chunks.
    fn recover_certified(&mut self, j: ProposalIndex) {
        let LocalCertifiedEntry::Certified(cert) = &self.certs[j] else {
            return;
        };
        let Entry::Positive(root) = cert.entry() else {
            return;
        };
        if self.availability[j].is_resolved(&root) {
            return;
        }
        let signers = cert.signers(&self.validator_data);
        let signers_decoded = matches!(cert, CertifiedEntry::FallbackQc(_));

        if signers_decoded && !self.availability[j].author_fulfilled(&root) {
            self.request_chunks(ChunkRequestType::MyChunks, j, root, signers.clone());
        }
        self.request_chunks(ChunkRequestType::YourChunks, j, root, signers);
    }

    fn request_chunks(
        &mut self,
        request_type: ChunkRequestType,
        j: ProposalIndex,
        root: MerkleRoot,
        mut voters: Vec<NodeId>,
    ) {
        // stable request order across runs
        voters.sort();
        self.emit(ChorusDACommand::RecoverChunks {
            j,
            root,
            request_type,
            voters,
        });
    }

    fn evidence_valid(&self, j: ProposalIndex, evidence: &ProposalEvidence) -> bool {
        match evidence {
            ProposalEvidence::FallbackSignedEntry(entry) => {
                let header_valid = match entry.header() {
                    Some(header) => self.header_auth.validate(header, self.slot.get(), j),
                    None => true,
                };
                entry.well_formed() && header_valid
            }
            ProposalEvidence::Certified(cert) => {
                cert.verify((self.slot, j), &self.header_auth, &self.validator_data)
            }
        }
    }

    // D_s
    #[must_use]
    pub(crate) fn on_deadline(&mut self) -> Option<BatchVoteMsg> {
        if self.phase != Phase::Propose {
            return None; // already voted; no-op
        }

        self.phase = Phase::Vote;

        let votes = self.proposals.as_ref().map(|j| {
            let entry = match self.availability[*j].fetch_proposal() {
                Some(proposal) => Entry::Positive(proposal.root),
                None => Entry::Negative,
            };

            let vote_msg = VoteMsg::new_signed((self.slot, *j), entry.clone(), &self.key);
            (entry, vote_msg.signature)
        });

        for (j, (entry, _)) in votes.as_ref().into_indexed_iter() {
            if let Entry::Positive(root) = entry {
                self.emit(ChorusDACommand::PinRoot { j, root: *root });
            }
        }
        self.emit(ChorusDACommand::ReleaseChunks);

        Some(BatchVoteMsg {
            slot: self.slot,
            votes,
        })
    }

    // D_s + Delta
    #[must_use]
    pub(crate) fn on_commit_vote_deadline(&mut self) -> CommitVoteDeadlineOutcome {
        if self.phase != Phase::Vote {
            return CommitVoteDeadlineOutcome::AlreadyVoted;
        }

        // do we have at least 2f+1 valid vote messages?
        //
        // todo: handle invalid signatures
        let has_enough_votes = self.votes.as_ref().into_iter().all(|votes| {
            let voter_stake = self.validator_data.sum_stake(votes.pool().all_voters());
            voter_stake > self.validator_data.total_stake().supermajority_threshold()
        });
        if !has_enough_votes {
            // wait for more votes to arrive.
            return CommitVoteDeadlineOutcome::NotEnoughVotes;
        }

        // cast a fallback vote and enter transition to fallback
        self.phase = Phase::TransitionToFallback;
        let enter_fallback_vote = VoteMsg::new_signed(self.slot, EnterFallbackVote, &self.key);

        let evidences = self.proposals.as_ref().map(|j| self.proposal_evidence(*j));

        // the roots our positive fallback entries vouch for
        for (j, evidence) in evidences.as_ref().into_indexed_iter() {
            if let ProposalEvidence::FallbackSignedEntry(entry) = evidence
                && let Some(header) = entry.header()
            {
                self.emit(ChorusDACommand::PinRoot {
                    j,
                    root: header.root,
                });
            }
        }
        self.recover_at_transition();

        let fallback_vote = FallbackVoteMsg {
            enter_fallback_vote,
            evidences,
        };
        CommitVoteDeadlineOutcome::FallbackVote(fallback_vote)
    }

    // D_s + 2Delta
    #[must_use]
    /// The certificate this validator just formed, and the block it can enter
    /// the fallback path with. The certificate is returned alongside rather
    /// than folded into the block: it admits the path, it is not part of the
    /// value the MVBA agrees on, and the caller has to disseminate it.
    pub(crate) fn on_fallback_deadline(&self) -> Option<(EnterFallbackCert, Metablock)> {
        // Note: fast commit qc is impossible at this point because
        // the possible fast commit qc must have been formed
        // reactively when handling fast commit votes.

        let enter_fallback_cert = self
            .enter_fallback_votes
            .try_form_strong_qc(&self.validator_data)?;
        let block = self.try_build_fallback_block()?;

        Some((enter_fallback_cert, block))
    }

    /// This validator's MVBA input: one certified entry per proposer, built
    /// from local evidence. `None` until it holds evidence for every proposer.
    pub(crate) fn try_build_fallback_block(&self) -> Option<Metablock> {
        self.certs
            .as_ref()
            .map(|cert| match cert {
                LocalCertifiedEntry::Absent => None,
                LocalCertifiedEntry::Certified(cert) => Some(cert),
            })
            .try_into_total()
            .map(|block| Metablock::new(block.into_owned()))
    }

    // ------- internal helper methods ---------
    fn proposal_evidence(&self, j: ProposalIndex) -> ProposalEvidence {
        if let LocalCertifiedEntry::Certified(cert) = &self.certs[j] {
            return cert.clone().into();
        }

        let scope = (self.slot, j);

        // if there are f+1 positive votes on a root and it's decoded,
        // vote positive.
        if let Some(root) = self.weak_available_root(j)
            && let Some(header) = self.availability[j].header_for(&root)
        {
            let fse =
                FallbackSignedEntry::new_signed_positive(scope, root, &self.key, header.clone());
            return ProposalEvidence::FallbackSignedEntry(fse);
        }

        // otherwise, vote negative
        let fse = FallbackSignedEntry::new_signed_negative(scope, &self.key);
        ProposalEvidence::FallbackSignedEntry(fse)
    }

    // the weak qcs for proposer j on a positive entry, as (root, qc)
    fn positive_weak_qcs(&self, j: ProposalIndex) -> Vec<(MerkleRoot, WeakQc<Entry>)> {
        let Some(weak_qc) = self.votes[j].pool().try_form_weak_qc(&self.validator_data) else {
            return vec![];
        };
        let candidates = match weak_qc {
            Either::Left(qc) => [Some(qc), None],
            Either::Right((qc1, qc2)) => [Some(qc1), Some(qc2)],
        };

        let mut positive = Vec::new();
        for qc in candidates.into_iter().flatten() {
            let Entry::Positive(root) = qc.verdict else {
                continue;
            };
            positive.push((root, qc));
        }
        positive
    }

    fn weak_available_root(&self, j: ProposalIndex) -> Option<MerkleRoot> {
        self.positive_weak_qcs(j)
            .into_iter()
            .map(|(root, _)| root)
            .find(|root| self.availability[j].decoded(root))
    }

    // P3 and P4: at the fallback transition, pull the roots that block
    // picking a fallback entry for each proposer without a certificate
    fn recover_at_transition(&mut self) {
        for j in 0..self.proposals.size() {
            if matches!(self.certs[j], LocalCertifiedEntry::Certified(_)) {
                // picked by certificate. P1 pulls it.
                continue;
            }
            for (root, voters) in self.blocking_roots(j) {
                self.request_chunks(ChunkRequestType::YourChunks, j, root, voters);
            }
        }
    }

    // the roots of proposer j that block picking its fallback entry,
    // each with its positive voters: an unresolved root with f+1
    // positive votes (P3), or a root of a claimed equivocation that no
    // held chunk witnesses (P4)
    fn blocking_roots(&self, j: ProposalIndex) -> Vec<(MerkleRoot, Vec<NodeId>)> {
        let avail = &self.availability[j];
        let positive_voters = self.positive_voters(j);
        let equivocation_claimed = positive_voters.len() >= 2;

        let mut weak_roots = Vec::new();
        for (root, _) in self.positive_weak_qcs(j) {
            weak_roots.push(root);
        }

        let mut blocking = Vec::new();
        for (root, voters) in positive_voters {
            let unresolved_weak = weak_roots.contains(&root) && !avail.is_resolved(&root);
            let unwitnessed_claim = equivocation_claimed && avail.header_for(&root).is_none();
            if unresolved_weak || unwitnessed_claim {
                blocking.push((root, voters));
            }
        }
        blocking
    }

    // the voters of every positive root of proposer j, admitted or held
    fn positive_voters(&self, j: ProposalIndex) -> HashMap<MerkleRoot, Vec<NodeId>> {
        let mut positive_voters: HashMap<MerkleRoot, Vec<NodeId>> = HashMap::new();
        for (entry, voters) in self.votes[j].pool().buckets() {
            let Entry::Positive(root) = entry else {
                continue;
            };
            positive_voters
                .entry(*root)
                .or_default()
                .extend(voters.iter().copied());
        }
        for (voter, root) in self.votes[j].held() {
            positive_voters.entry(root).or_default().push(voter);
        }
        positive_voters
    }

    // P1 for a committed slot: pull every committed root we have not
    // resolved from the commit certificate's signers
    pub(crate) fn recover_committed(&mut self, qc: &FastCommitQc) {
        let Some(signers) = qc.sigcol.signers(&self.validator_data) else {
            return;
        };
        let signers: Vec<NodeId> = signers.into_iter().copied().collect();

        for (j, entry) in qc.verdict.entries.as_ref().into_indexed_iter() {
            let Entry::Positive(root) = entry else {
                continue;
            };
            if self.availability[j].is_resolved(root) {
                continue;
            }
            self.request_chunks(ChunkRequestType::YourChunks, j, *root, signers.clone());
        }
    }

    fn try_form_fallback_qc(&mut self, j: ProposalIndex) {
        if self.certs[j].strength() >= EvidenceStrength::FallbackQc {
            // already have a fallback qc or stronger.
            return;
        }

        let weak_qc = match self.fallback_entry_votes[j]
            .pool()
            .try_form_weak_qc(&self.validator_data)
        {
            None => return,
            Some(Either::Left(qc)) => qc,
            Some(Either::Right((qc1, qc2))) => {
                // at most one is positive: two positive entries carry
                // rival headers, whose EquivCert outranks a FallbackQc
                // and returned above
                match (&qc1.verdict.0, &qc2.verdict.0) {
                    (Entry::Positive { .. }, _) => qc1,
                    (_, Entry::Positive { .. }) => qc2,
                    _ => unreachable!("two distinct fallback weak qcs must include a positive"),
                }
            }
        };

        if self.certs[j].try_upgrade(weak_qc) {
            self.recover_certified(j);
        }
    }

    fn try_cast_fast_commit_vote(&mut self) -> Option<FastBlock> {
        if matches!(self.phase, Phase::FastCommit | Phase::TransitionToFallback) {
            // voted for commit or fallback, no-op
            return None;
        }

        let fast_block = self.try_form_fast_block()?;
        self.phase = Phase::FastCommit;
        Some(fast_block)
    }

    fn try_form_fast_block(&self) -> Option<FastBlock> {
        self.certs
            .as_ref()
            .map(LocalCertifiedEntry::fast_qc)
            .try_into_total()
            .map(ProposalMap::into_owned)
            .map(FastBlock)
    }

    fn try_form_fast_qc(&mut self, j: ProposalIndex) {
        if self.certs[j].fast_qc().is_some() {
            // already have a strong qc, no need to try forming again.
            return;
        }

        if let Some(fast_qc) = self.votes[j]
            .pool()
            .try_form_strong_qc(&self.validator_data)
            && self.certs[j].try_upgrade(fast_qc)
        {
            self.recover_certified(j);
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Entry {
    Positive(MerkleRoot),
    Negative,
}

impl IsVote for Entry {
    type Scope = (Slot, ProposalIndex);

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

impl GatingRoot for Entry {
    fn gating_root(&self) -> Option<MerkleRoot> {
        match self {
            Entry::Positive(root) => Some(*root),
            Entry::Negative => None,
        }
    }
}

pub type FastQc = StrongQc<Entry>;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct BatchVoteMsg {
    slot: Slot,
    votes: ProposalMap<(Entry, Signature)>,
    // vote only. fields for chunks & decryption share may be added by
    // other components.
}

impl BatchVoteMsg {
    pub fn split(self) -> Vec<VoteMsg<Entry>> {
        self.votes
            .into_indexed_iter()
            .map(|(j, (entry, sig))| VoteMsg::new((self.slot, j), entry, sig))
            .collect()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct FastCommitVote {
    pub entries: ProposalMap<Entry>,
}

pub(crate) type FastCommitVoteMsg = VoteMsg<FastCommitVote>;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FastBlock(TotalProposalMap<FastQc>);

impl FastBlock {
    pub(crate) fn commit_vote(&self, slot: Slot, key: &KeyPair) -> FastCommitVoteMsg {
        let vote = FastCommitVote::from(self);
        VoteMsg::new_signed(slot, vote, key)
    }
}

impl From<&FastBlock> for FastCommitVote {
    fn from(block: &FastBlock) -> Self {
        let entries = block.0.as_ref().map(|qc| &qc.verdict).into_owned();
        Self { entries }
    }
}

impl IsVote for FastCommitVote {
    type Scope = Slot;

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

pub(crate) type FastCommitQc = StrongQc<FastCommitVote>;

// ============ Fallback ===============

// same as Entry, but signed under a distinct signing domain
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FallbackEntry(pub Entry);

impl IsVote for FallbackEntry {
    type Scope = (Slot, ProposalIndex);

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

impl GatingRoot for FallbackEntry {
    fn gating_root(&self) -> Option<MerkleRoot> {
        self.0.gating_root()
    }
}

pub type FallbackQc = WeakQc<FallbackEntry>;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum EvidenceStrength {
    // From lowest to highest
    Absent,
    FallbackQc,
    EquivCert,
    FastQc,
}

#[derive(Clone, PartialEq, Eq, Hash, derive_more::From, Debug)]
pub enum CertifiedEntry {
    #[from]
    FastQc(FastQc),
    #[from]
    EquivCert(EquivCert),
    #[from]
    FallbackQc(FallbackQc),
}

impl CertifiedEntry {
    fn strength(&self) -> EvidenceStrength {
        match self {
            CertifiedEntry::FastQc(_) => EvidenceStrength::FastQc,
            CertifiedEntry::EquivCert(_) => EvidenceStrength::EquivCert,
            CertifiedEntry::FallbackQc(_) => EvidenceStrength::FallbackQc,
        }
    }

    pub(crate) fn entry(&self) -> Entry {
        match self {
            CertifiedEntry::FastQc(qc) => qc.verdict.clone(),
            CertifiedEntry::FallbackQc(qc) => qc.verdict.0.clone(),
            CertifiedEntry::EquivCert(_) => Entry::Negative,
        }
    }

    // the validators whose votes form the certificate. none for an
    // equivocation certificate.
    fn signers(&self, validator_data: &ValidatorData) -> Vec<NodeId> {
        let sigcol = match self {
            CertifiedEntry::FastQc(qc) => &qc.sigcol,
            CertifiedEntry::FallbackQc(qc) => &qc.sigcol,
            CertifiedEntry::EquivCert(_) => return vec![],
        };
        let Some(signers) = sigcol.signers(validator_data) else {
            return vec![];
        };
        signers.into_iter().copied().collect()
    }

    /// Whether this certificate is well-formed and carries valid
    /// signatures. Authenticity is enforced at message ingress (see the
    /// crate header); we restate it at adoption points to make the trust
    /// boundary explicit and catch protocol-logic bugs.
    pub(crate) fn verify(
        &self,
        scope: (Slot, ProposalIndex),
        header_auth: &HeaderAuth,
        validator_data: &ValidatorData,
    ) -> bool {
        match self {
            CertifiedEntry::FastQc(qc) => qc.verify(validator_data),
            CertifiedEntry::FallbackQc(qc) => qc.verify(validator_data),
            CertifiedEntry::EquivCert(EquivCert(a, b)) => {
                let (s, j) = scope;
                a.root != b.root
                    && header_auth.validate(a, s.get(), j)
                    && header_auth.validate(b, s.get(), j)
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct FallbackSignedEntry {
    entry: FallbackEntry,
    // over (slot, j, self.entry)
    signature: Signature,
    // invariant: header.is_some() iff entry is positive
    // invariant: header.root == entry.root
    header: Option<ProposalHeader>,
}

impl FallbackSignedEntry {
    fn new_signed_positive(
        scope: (Slot, ProposalIndex),
        root: MerkleRoot,
        key: &KeyPair,
        header: ProposalHeader,
    ) -> Self {
        let entry = FallbackEntry(Entry::Positive(root));
        let signature = VoteMsg::new_signed(scope, entry.clone(), key).signature;
        Self {
            entry,
            signature,
            header: Some(header),
        }
    }

    fn new_signed_negative(scope: (Slot, ProposalIndex), key: &KeyPair) -> Self {
        let entry = FallbackEntry(Entry::Negative);
        let signature = VoteMsg::new_signed(scope, entry.clone(), key).signature;
        Self {
            entry,
            signature,
            header: None,
        }
    }

    fn well_formed(&self) -> bool {
        match &self.entry.0 {
            Entry::Positive(root) => self
                .header
                .as_ref()
                .is_some_and(|header| header.root == *root),
            Entry::Negative => self.header.is_none(),
        }
    }

    fn header(&self) -> Option<&ProposalHeader> {
        self.header.as_ref()
    }

    fn into_vote_msg(self, slot: Slot, j: ProposalIndex) -> VoteMsg<FallbackEntry> {
        VoteMsg::new((slot, j), self.entry, self.signature)
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ProposalEvidence {
    Certified(CertifiedEntry),
    FallbackSignedEntry(FallbackSignedEntry),
}

impl From<CertifiedEntry> for ProposalEvidence {
    fn from(value: CertifiedEntry) -> Self {
        ProposalEvidence::Certified(value)
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Default, derive_more::From)]
enum LocalCertifiedEntry {
    #[default]
    Absent,
    #[from]
    Certified(CertifiedEntry),
}

impl LocalCertifiedEntry {
    fn fast_qc(&self) -> Option<&FastQc> {
        match self {
            LocalCertifiedEntry::Absent => None,
            LocalCertifiedEntry::Certified(CertifiedEntry::FastQc(qc)) => Some(qc),
            LocalCertifiedEntry::Certified(_) => None,
        }
    }

    fn strength(&self) -> EvidenceStrength {
        match self {
            LocalCertifiedEntry::Absent => EvidenceStrength::Absent,
            LocalCertifiedEntry::Certified(cert) => cert.strength(),
        }
    }

    // whether the evidence was adopted
    fn try_upgrade(&mut self, new_ev: impl Into<CertifiedEntry>) -> bool {
        let new_ev = new_ev.into();

        match self {
            LocalCertifiedEntry::Absent => {
                *self = LocalCertifiedEntry::Certified(new_ev);
                true
            }
            LocalCertifiedEntry::Certified(ev) if new_ev.strength() > ev.strength() => {
                *ev = new_ev;
                true
            }
            _ => false,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct EnterFallbackVote;

impl IsVote for EnterFallbackVote {
    type Scope = Slot;

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct FallbackVoteMsg {
    enter_fallback_vote: VoteMsg<EnterFallbackVote>,
    evidences: ProposalMap<ProposalEvidence>,
}

// A fallback cert certifies 2f+1 validators agree to enter fallback path
pub type EnterFallbackCert = StrongQc<EnterFallbackVote>;

#[cfg(test)]
mod tests {
    use super::{
        super::types::{Stake, ValidatorData},
        *,
    };
    use crate::{
        env::stub::{D25, EncodingScheme, MerkleHash, ProposalSignature},
        spec::vote::KeyPair as _,
    };

    const SLOT: Slot = Slot(1);

    fn validator_data(n: u64) -> ValidatorData {
        let validators = (0..n).map(NodeId::dummy).collect::<Vec<_>>();
        let valset = validators.iter().map(|id| (*id, Stake::from(1))).collect();
        let mapping = validators
            .iter()
            .map(|id| (*id, id.keypair().pubkey()))
            .collect();

        ValidatorData::new(valset, mapping)
    }

    fn root(byte: u8) -> MerkleRoot {
        MerkleRoot(MerkleHash([byte; 20]))
    }

    // signed by validator 0, the only proposer
    fn header(byte: u8) -> ProposalHeader {
        ProposalHeader {
            slot: crate::stub::types::Slot(SLOT.get()),
            root: root(byte),
            sig: ProposalSignature(0),
            scheme: EncodingScheme::D25(D25 {
                msg_len: 0,
                unix_ts: 0,
            }),
        }
    }

    // the local node is validator 1 among 4, one proposal per slot
    fn fast_path() -> FastPath {
        let header_auth = HeaderAuth::new(|_, signer| (*signer == NodeId::dummy(0)).then_some(0));
        FastPath::new(
            SLOT,
            1,
            Arc::new(NodeId::dummy(1).keypair()),
            Arc::new(validator_data(4)),
            Arc::new(header_auth),
        )
    }

    // the chunk requests among the drained commands, for proposal 0
    fn drain_requests(fast: &mut FastPath) -> Vec<(ChunkRequestType, MerkleRoot, Vec<NodeId>)> {
        let mut requests = Vec::new();
        while let Some(command) = fast.next_da_command() {
            let ChorusDACommand::RecoverChunks {
                j,
                root,
                request_type,
                voters,
            } = command
            else {
                continue;
            };
            assert_eq!(j, 0);
            requests.push((request_type, root, voters));
        }
        requests
    }

    fn positive_fallback_vote(voter: u64, byte: u8) -> (NodeId, FallbackVoteMsg) {
        let voter = NodeId::dummy(voter);
        let key = voter.keypair();
        let entry =
            FallbackSignedEntry::new_signed_positive((SLOT, 0), root(byte), &key, header(byte));
        let msg = FallbackVoteMsg {
            enter_fallback_vote: VoteMsg::new_signed(SLOT, EnterFallbackVote, &key),
            evidences: ProposalMap::new(1, |_| {
                ProposalEvidence::FallbackSignedEntry(entry.clone())
            }),
        };
        (voter, msg)
    }

    // a fast qc on root(byte) signed by validators 0, 2 and 3
    fn fast_block(byte: u8) -> FastBlock {
        let mut pool = VotePool::new((SLOT, 0));
        for id in [0, 2, 3] {
            let voter = NodeId::dummy(id);
            let msg = VoteMsg::new_signed((SLOT, 0), Entry::Positive(root(byte)), &voter.keypair());
            pool.add_vote(voter, msg);
        }
        let qc = pool
            .try_form_strong_qc(&validator_data(4))
            .expect("three of four votes form a fast qc");
        FastBlock(ProposalMap::new(1, |_| qc.clone()))
    }

    #[test]
    fn suspended_fallback_entry_pulls_own_chunks_from_its_signer() {
        let mut fast = fast_path();

        let (voter, msg) = positive_fallback_vote(2, 1);
        fast.handle_fallback_vote(voter, msg);
        let expected = (ChunkRequestType::MyChunks, root(1), vec![voter]);
        assert_eq!(drain_requests(&mut fast), vec![expected]);

        // once our own chunks arrived, positive entries are admitted.
        // the FallbackQc they form pulls the root from its signers (P1),
        // but not our own chunks, which we hold
        let arrived = ProposalDAEvent::ProposerObligationFulfilled(root(1));
        let _ = fast.handle_da_event(ChorusDAEvent {
            j: 0,
            event: arrived,
        });
        let (voter, msg) = positive_fallback_vote(3, 1);
        fast.handle_fallback_vote(voter, msg);
        let signers = vec![NodeId::dummy(2), NodeId::dummy(3)];
        let expected = (ChunkRequestType::YourChunks, root(1), signers);
        assert_eq!(drain_requests(&mut fast), vec![expected]);
    }

    #[test]
    fn certified_root_is_pulled_from_its_signers() {
        let mut fast = fast_path();

        fast.handle_fast_block(fast_block(1));
        let signers = vec![NodeId::dummy(0), NodeId::dummy(2), NodeId::dummy(3)];
        let expected = (ChunkRequestType::YourChunks, root(1), signers);
        assert_eq!(drain_requests(&mut fast), vec![expected]);

        // the same certificate again is no news
        fast.handle_fast_block(fast_block(1));
        assert!(drain_requests(&mut fast).is_empty());
    }

    #[test]
    fn resolved_root_is_not_pulled() {
        let mut fast = fast_path();

        let _ = fast.handle_da_event(ChorusDAEvent {
            j: 0,
            event: ProposalDAEvent::Decoded(root(1)),
        });
        fast.handle_fast_block(fast_block(1));
        assert!(drain_requests(&mut fast).is_empty());
    }

    #[test]
    fn deadline_pins_positive_roots_before_releasing_chunks() {
        let mut fast = fast_path();

        let _ = fast.handle_da_event(ChorusDAEvent {
            j: 0,
            event: ProposalDAEvent::HeaderSeen(header(1)),
        });
        let _ = fast.handle_da_event(ChorusDAEvent {
            j: 0,
            event: ProposalDAEvent::ProposerObligationFulfilled(root(1)),
        });
        let _ = fast.on_deadline();

        let mut commands = Vec::new();
        while let Some(command) = fast.next_da_command() {
            commands.push(command);
        }
        let pin = ChorusDACommand::PinRoot {
            j: 0,
            root: root(1),
        };
        assert_eq!(commands, vec![pin, ChorusDACommand::ReleaseChunks]);
    }

    fn batch_vote(voter: u64, entry: Entry) -> (NodeId, BatchVoteMsg) {
        let voter = NodeId::dummy(voter);
        let key = voter.keypair();
        let votes = ProposalMap::new(1, |j| {
            let signature = VoteMsg::new_signed((SLOT, j), entry.clone(), &key).signature;
            (entry.clone(), signature)
        });
        (voter, BatchVoteMsg { slot: SLOT, votes })
    }

    // the chunks of the voter under the root arrived, admitting its
    // positive vote
    fn owner_fulfilled(fast: &mut FastPath, voter: u64, byte: u8) {
        let event = ProposalDAEvent::OwnerObligationFulfilled {
            owner: NodeId::dummy(voter),
            root: root(byte),
        };
        let _ = fast.handle_da_event(ChorusDAEvent { j: 0, event });
    }

    // cast the votes and pass both deadlines into the fallback transition
    fn transition(fast: &mut FastPath, votes: Vec<(NodeId, BatchVoteMsg)>) {
        let _ = fast.on_deadline();
        for (voter, msg) in votes {
            let _ = fast.handle_batch_vote(voter, msg);
        }
        let outcome = fast.on_commit_vote_deadline();
        assert!(matches!(
            outcome,
            CommitVoteDeadlineOutcome::FallbackVote(_)
        ));
    }

    #[test]
    fn transition_pulls_an_unresolved_weak_root_from_its_voters() {
        let mut fast = fast_path();

        // validators 0 and 3 vote positive on root(1), which we have not
        // decoded. their chunks arrived, so the votes are admitted and
        // form a weak qc
        let _ = fast.handle_da_event(ChorusDAEvent {
            j: 0,
            event: ProposalDAEvent::HeaderSeen(header(1)),
        });
        owner_fulfilled(&mut fast, 0, 1);
        owner_fulfilled(&mut fast, 3, 1);
        let votes = vec![
            batch_vote(0, Entry::Positive(root(1))),
            batch_vote(3, Entry::Positive(root(1))),
            batch_vote(2, Entry::Negative),
            batch_vote(1, Entry::Negative),
        ];
        transition(&mut fast, votes);

        let voters = vec![NodeId::dummy(0), NodeId::dummy(3)];
        let expected = (ChunkRequestType::YourChunks, root(1), voters);
        assert_eq!(drain_requests(&mut fast), vec![expected]);
    }

    #[test]
    fn transition_pulls_the_unwitnessed_root_of_a_claimed_equivocation() {
        let mut fast = fast_path();

        // validator 0 votes positive on root(1), whose header and chunks
        // we hold. validator 2 votes positive on root(2), of which we
        // hold nothing: its vote is held and its claim is unwitnessed
        let _ = fast.handle_da_event(ChorusDAEvent {
            j: 0,
            event: ProposalDAEvent::HeaderSeen(header(1)),
        });
        owner_fulfilled(&mut fast, 0, 1);
        let votes = vec![
            batch_vote(0, Entry::Positive(root(1))),
            batch_vote(2, Entry::Positive(root(2))),
            batch_vote(3, Entry::Negative),
            batch_vote(1, Entry::Negative),
        ];
        transition(&mut fast, votes);

        let expected = (
            ChunkRequestType::YourChunks,
            root(2),
            vec![NodeId::dummy(2)],
        );
        assert_eq!(drain_requests(&mut fast), vec![expected]);
    }

    // a fast commit qc on the entries [root(byte)] signed by validators
    // 0, 2 and 3
    fn fast_commit_qc(byte: u8) -> FastCommitQc {
        let entries = ProposalMap::new(1, |_| Entry::Positive(root(byte)));
        let mut pool = VotePool::new(SLOT);
        for id in [0, 2, 3] {
            let voter = NodeId::dummy(id);
            let vote = FastCommitVote {
                entries: entries.clone(),
            };
            pool.add_vote(voter, VoteMsg::new_signed(SLOT, vote, &voter.keypair()));
        }
        pool.try_form_strong_qc(&validator_data(4))
            .expect("three of four votes form a commit qc")
    }

    #[test]
    fn committed_roots_are_pulled_from_the_commit_signers() {
        let mut fast = fast_path();

        fast.recover_committed(&fast_commit_qc(1));
        let signers = vec![NodeId::dummy(0), NodeId::dummy(2), NodeId::dummy(3)];
        let expected = (ChunkRequestType::YourChunks, root(1), signers);
        assert_eq!(drain_requests(&mut fast), vec![expected]);

        // a resolved root is not pulled
        let _ = fast.handle_da_event(ChorusDAEvent {
            j: 0,
            event: ProposalDAEvent::Decoded(root(1)),
        });
        fast.recover_committed(&fast_commit_qc(1));
        assert!(drain_requests(&mut fast).is_empty());
    }
}
