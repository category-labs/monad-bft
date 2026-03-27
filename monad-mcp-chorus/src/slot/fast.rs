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

use std::sync::Arc;

use itertools::Either;

use super::fallback::{FallbackPath, MVBAInputs};
use crate::types::{
    DAHandle, EquivCert, FetchProposalError, IsVote, KeyPair, MerkleRoot, NodeId, ProposalIndex,
    ProposalMap, ProposalMeta, Signature, Slot, StrongQc, TotalProposalMap, ValidatorData, VoteMsg,
    VotePool, WeakQc,
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

    votes: ProposalMap<VotePool<Entry>>,
    commit_votes: VotePool<FastCommitVote>,

    enter_fallback_votes: VotePool<EnterFallbackVote>,
    fallback_entry_votes: ProposalMap<VotePool<FallbackEntry>>,

    // helper field to construct per-proposal values
    proposals: ProposalMap<ProposalIndex>,

    // using Arc to avoid lifetime issues.
    key: Arc<KeyPair>,
    validator_data: Arc<ValidatorData>,

    // data availability layer handle
    da: Arc<DAHandle>,
}

impl FastPath {
    pub(crate) fn new(
        s: Slot,
        num_proposals: usize,
        key: Arc<KeyPair>,
        validator_data: Arc<ValidatorData>,
        da_handle: Arc<DAHandle>,
    ) -> Self {
        Self {
            slot: s,

            votes: ProposalMap::new(num_proposals, |j| VotePool::new((s, j))),
            certs: ProposalMap::new_default(num_proposals),
            commit_votes: VotePool::new(s),

            enter_fallback_votes: VotePool::new(s),
            fallback_entry_votes: ProposalMap::new(num_proposals, |j| VotePool::new((s, j))),

            phase: Phase::Propose,
            proposals: ProposalMap::new(num_proposals, |j| j),

            key,
            validator_data,
            da: da_handle,
        }
    }

    pub(crate) fn spawn_fallback(&self, input: MVBAInputs) -> FallbackPath {
        FallbackPath::new(
            self.slot,
            self.key.clone(),
            self.validator_data.clone(),
            input,
        )
    }

    pub(crate) fn handle_batch_vote(
        &mut self,
        node_id: NodeId,
        vote_msg: BatchVoteMsg,
    ) -> Option<FastBlock> {
        for vote_msg in vote_msg.split() {
            self.handle_vote(node_id, vote_msg);
        }

        self.try_cast_fast_commit_vote()
    }

    fn handle_vote(&mut self, node_id: NodeId, vote_msg: VoteMsg<Entry>) {
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
        // no phase guard on purpose
        self.commit_votes.add_vote(node_id, vote_msg);
        self.commit_votes.try_form_strong_qc(&self.validator_data)
    }

    pub(crate) fn handle_fast_block(&mut self, fast_block: FastBlock) -> Option<FastBlock> {
        for (j, qc) in fast_block.0.into_indexed_iter() {
            debug_assert!(self.validator_data.verify_strong_qc(&qc));
            self.certs[j].try_upgrade(qc);
        }

        self.try_cast_fast_commit_vote()
    }

    pub(crate) fn handle_fallback_vote(&mut self, node_id: NodeId, vote_msg: FallbackVoteMsg) {
        self.enter_fallback_votes
            .add_vote(node_id, vote_msg.enter_fallback_vote);

        for (j, evidence) in vote_msg.evidences.into_indexed_iter() {
            match evidence {
                ProposalEvidence::FallbackSignedEntry(entry) => {
                    debug_assert!(entry.well_formed());
                    if let Some(meta) = entry.meta() {
                        self.da.observe_proposal(self.slot, j, meta.clone());
                    }
                    let vote = entry.into_vote_msg(self.slot, j);
                    self.fallback_entry_votes[j].add_vote(node_id, vote);
                    self.try_form_fallback_qc(j);
                }

                ProposalEvidence::Certified(cert) => {
                    debug_assert!(cert.verify(&self.validator_data));
                    self.certs[j].try_upgrade(cert);
                }
            }
        }
    }

    // D_s
    #[must_use]
    pub(crate) fn on_propose_deadline(&mut self) -> Option<BatchVoteMsg> {
        if self.phase != Phase::Propose {
            return None; // already voted; no-op
        }

        self.phase = Phase::Vote;

        let votes = self.proposals.as_ref().map(|j| {
            let entry = match self.da.fetch_proposal(self.slot, *j) {
                Ok(proposal) => Entry::Positive {
                    root: proposal.root,
                },
                Err(FetchProposalError::Absent) => Entry::Negative,
                Err(FetchProposalError::Equivocation(equiv_cert)) => {
                    self.certs[*j].try_upgrade(equiv_cert);
                    // upgrade on the equivocation certificate?
                    todo!()
                }
            };

            let vote_msg = VoteMsg::new_signed((self.slot, *j), entry.clone(), &self.key);
            (entry, vote_msg.signature)
        });

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
        let has_enough_votes = self.votes.as_ref().into_iter().all(|pool| {
            let voter_stake = self.validator_data.sum_stake(pool.all_voters());
            self.validator_data.is_supermajority(voter_stake)
        });
        if !has_enough_votes {
            // wait for more votes to arrive.
            return CommitVoteDeadlineOutcome::NotEnoughVotes;
        }

        // cast a fallback vote and enter transition to fallback
        self.phase = Phase::TransitionToFallback;
        let enter_fallback_vote = VoteMsg::new_signed(self.slot, EnterFallbackVote, &self.key);

        let evidences = self.proposals.as_ref().map(|j| self.proposal_evidence(*j));

        let fallback_vote = FallbackVoteMsg {
            enter_fallback_vote,
            evidences,
        };
        CommitVoteDeadlineOutcome::FallbackVote(fallback_vote)
    }

    // D_s + 2Delta
    #[must_use]
    pub(crate) fn on_fallback_deadline(&self) -> Option<MVBAInputs> {
        // Note: fast commit qc is impossible at this point because
        // the possible fast commit qc must have been formed
        // reactively when handling fast commit votes.

        let enter_fallback_cert = self
            .enter_fallback_votes
            .try_form_strong_qc(&self.validator_data)?;
        self.try_build_mvba_inputs(enter_fallback_cert)
    }

    pub(crate) fn try_build_mvba_inputs(
        &self,
        enter_fallback_cert: EnterFallbackCert,
    ) -> Option<MVBAInputs> {
        let block = self
            .certs
            .as_ref()
            .map(|cert| match cert {
                LocalCertifiedEntry::Absent => None,
                LocalCertifiedEntry::Certified(cert) => Some(cert),
            })
            .try_into_total()?
            .into_owned();

        Some(MVBAInputs {
            enter_fallback_cert,
            block,
        })
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
            && let Ok(meta) = self.da.fetch_proposal(self.slot, j)
            && meta.root == root
        {
            let fse = FallbackSignedEntry::new_signed_positive(scope, root, &self.key, meta);
            return ProposalEvidence::FallbackSignedEntry(fse);
        }

        // otherwise, vote negative
        let fse = FallbackSignedEntry::new_signed_negative(scope, &self.key);
        ProposalEvidence::FallbackSignedEntry(fse)
    }

    // find a merkle root for proposer j with f+1 positive votes that is
    // locally decoded, so we can cast a positive fallback entry
    fn weak_available_root(&self, j: ProposalIndex) -> Option<MerkleRoot> {
        let candidates = match self.votes[j].try_form_weak_qc(&self.validator_data)? {
            Either::Left(qc) => [Some(qc), None],
            Either::Right((qc1, qc2)) => [Some(qc1), Some(qc2)],
        };

        candidates
            .into_iter()
            .flatten()
            .filter_map(|qc| match qc.verdict {
                Entry::Positive { root } => Some(root),
                Entry::Negative => None,
            })
            .find(|root| self.da.proposal_decoded(self.slot, j, root))
    }

    fn try_form_fallback_qc(&mut self, j: ProposalIndex) {
        if self.certs[j].strength() >= EvidenceStrength::FallbackQc {
            // already have a fallback qc or stronger.
            return;
        }

        let weak_qc = match self.fallback_entry_votes[j].try_form_weak_qc(&self.validator_data) {
            None => return,
            Some(Either::Left(qc)) => qc,
            Some(Either::Right((qc1, qc2))) => {
                // we already handled equivocation from distinct
                // positive weak qc from observe_proposal that
                // should preceed this call
                match (&qc1.verdict.0, &qc2.verdict.0) {
                    (Entry::Positive { .. }, _) => qc1,
                    (_, Entry::Positive { .. }) => qc2,
                    _ => unreachable!("two distinct fallback weak qcs must include a positive"),
                }
            }
        };

        self.certs[j].try_upgrade(weak_qc);
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

        if let Some(fast_qc) = self.votes[j].try_form_strong_qc(&self.validator_data) {
            self.certs[j].try_upgrade(fast_qc);
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Entry {
    Positive { root: MerkleRoot },
    Negative,
}

impl IsVote for Entry {
    type Scope = (Slot, ProposalIndex);
}

pub(crate) type FastQc = StrongQc<Entry>;

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
}

pub(crate) type FastCommitQc = StrongQc<FastCommitVote>;

// ============ Fallback ===============

// same as Entry, but signed under a distinct signing domain
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct FallbackEntry(pub Entry);

impl IsVote for FallbackEntry {
    type Scope = (Slot, ProposalIndex);
}

pub(crate) type FallbackQc = WeakQc<FallbackEntry>;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum EvidenceStrength {
    // From lowest to highest
    Absent,
    FallbackQc,
    EquivCert,
    FastQc,
}

#[derive(Clone, PartialEq, Eq, Hash, derive_more::From, Debug)]
pub(crate) enum CertifiedEntry {
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

    /// Whether this certificate is well-formed and carries valid
    /// signatures. Authenticity is enforced at message ingress (see the
    /// crate header); we restate it at adoption points to make the trust
    /// boundary explicit and catch protocol-logic bugs.
    fn verify(&self, validator_data: &ValidatorData) -> bool {
        match self {
            CertifiedEntry::FastQc(qc) => validator_data.verify_strong_qc(qc),
            CertifiedEntry::FallbackQc(qc) => validator_data.verify_weak_qc(qc),
            CertifiedEntry::EquivCert(EquivCert(a, b)) => {
                a.root != b.root
                    && a.opaque_header.validate(&a.root, &a.sig)
                    && b.opaque_header.validate(&a.root, &a.sig)
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct FallbackSignedEntry {
    entry: FallbackEntry,
    // over (slot, j, self.entry)
    signature: Signature,
    // invariant: meta.is_some() iff entry is positive
    // invariant: meta.root == entry.root
    meta: Option<ProposalMeta>,
}

impl FallbackSignedEntry {
    fn new_signed_positive(
        scope: (Slot, ProposalIndex),
        root: MerkleRoot,
        key: &KeyPair,
        meta: ProposalMeta,
    ) -> Self {
        let entry = FallbackEntry(Entry::Positive { root });
        let signature = VoteMsg::new_signed(scope, entry.clone(), key).signature;
        Self {
            entry,
            signature,
            meta: Some(meta),
        }
    }

    fn new_signed_negative(scope: (Slot, ProposalIndex), key: &KeyPair) -> Self {
        let entry = FallbackEntry(Entry::Negative);
        let signature = VoteMsg::new_signed(scope, entry.clone(), key).signature;
        Self {
            entry,
            signature,
            meta: None,
        }
    }

    fn well_formed(&self) -> bool {
        match &self.entry.0 {
            Entry::Positive { root } => self.meta.as_ref().is_some_and(|meta| {
                meta.root == *root && meta.opaque_header.validate(&meta.root, &meta.sig)
            }),
            Entry::Negative => self.meta.is_none(),
        }
    }

    fn meta(&self) -> Option<&ProposalMeta> {
        self.meta.as_ref()
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

    fn try_upgrade(&mut self, new_ev: impl Into<CertifiedEntry>) {
        let new_ev = new_ev.into();

        match self {
            LocalCertifiedEntry::Absent => {
                *self = LocalCertifiedEntry::Certified(new_ev);
            }
            LocalCertifiedEntry::Certified(ev) if new_ev.strength() > ev.strength() => {
                *ev = new_ev;
            }
            _ => {}
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct EnterFallbackVote;

impl IsVote for EnterFallbackVote {
    type Scope = Slot;
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct FallbackVoteMsg {
    enter_fallback_vote: VoteMsg<EnterFallbackVote>,
    evidences: ProposalMap<ProposalEvidence>,
}

// A fallback cert certifies 2f+1 validators agree to enter fallback path
pub(crate) type EnterFallbackCert = StrongQc<EnterFallbackVote>;
