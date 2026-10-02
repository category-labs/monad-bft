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

use std::{collections::VecDeque, sync::Arc};

use alloy_rlp::{
    Decodable, Encodable, Header, RlpDecodable, RlpDecodableWrapper, RlpEncodable,
    RlpEncodableWrapper, encode_list, list_length,
};
use arrayvec::ArrayVec;

use super::{
    availability::ProposalAvailability,
    chorus::{ChorusDACommand, ChorusDAEvent, ChunkRequestType},
    fallback::Metablock,
    types::{
        EquivCert, Gate, HeaderAuth, IsVote, KeyPair, MerkleRoot, NodeId, ProposalIndex,
        ProposalMap, ProposalScope, ProposerSet, Signature, SignedProposalHeader, Slot, StrongQc,
        TotalProposalMap, ValidatorData, VoteMsg, VotePool, WeakQc,
    },
};
use crate::spec::{
    Stake as _,
    proposal::{HeaderAuth as _, ProposalHeader as _},
    validator::ValidatorData as _,
    vote::{SignatureCollection as _, SigningDomain, assert_signing_prefix},
};

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum Phase {
    Propose,              // initial phase
    Vote,                 // vote casted
    FastCommit,           // fast commit vote cast
    TransitionToFallback, // fallback vote cast
}

pub enum FallbackTransitionOutcome {
    AlreadyCommitVoted, // already voted reactively
    Waiting,            // not yet 2f+1 votes, or an entry is unresolved
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

    // what we know about each proposal's DA
    availability: ProposalMap<ProposalAvailability>,

    // effects for the DA layer, drained by the slot consensus wrapper
    commands: VecDeque<ChorusDACommand>,

    // who occupies each proposal index in this slot
    proposers: ProposerSet,

    // using Arc to avoid lifetime issues.
    key: Arc<KeyPair>,
    validator_data: Arc<ValidatorData>,
    header_auth: Arc<HeaderAuth>,
}

impl FastPath {
    pub(crate) fn new(
        s: Slot,
        proposers: ProposerSet,
        key: Arc<KeyPair>,
        validator_data: Arc<ValidatorData>,
        header_auth: Arc<HeaderAuth>,
    ) -> Self {
        let num_proposals = proposers.num_indices();
        Self {
            slot: s,

            votes: ProposalMap::new(num_proposals, |j| VotePool::new(ProposalScope::new(s, j))),
            certs: ProposalMap::new_default(num_proposals),
            commit_votes: VotePool::new(s),

            enter_fallback_votes: VotePool::new(s),
            fallback_entry_votes: ProposalMap::new(num_proposals, |j| {
                VotePool::new(ProposalScope::new(s, j))
            }),

            phase: Phase::Propose,
            proposals: ProposalMap::new(num_proposals, |j| j),
            availability: ProposalMap::new_default(num_proposals),
            commands: VecDeque::new(),
            proposers,

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

    #[must_use]
    pub(crate) fn handle_da_event(&mut self, event: ChorusDAEvent) -> Option<FastBlock> {
        let ChorusDAEvent { j, event } = event;

        if let Some(equiv_cert) = self.availability[j].ingest(event) {
            self.certs[j].try_upgrade(equiv_cert);
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
        voter: NodeId,
        vote_msg: BatchVoteMsg,
    ) -> Option<FastBlock> {
        let shape_valid =
            vote_msg.slot == self.slot && vote_msg.votes.size() == self.proposals.size();
        if !shape_valid {
            return None;
        }

        for vote_msg in vote_msg.split() {
            self.handle_vote(voter, vote_msg);
        }

        self.try_cast_fast_commit_vote()
    }

    fn handle_vote(&mut self, voter: NodeId, vote_msg: VoteMsg<Entry>) {
        debug_assert!(self.validator_data.contains(&voter));

        let j = vote_msg.scope.index;
        self.votes[j].add_vote(voter, vote_msg);
        self.try_form_fast_qc(j);
    }

    #[must_use]
    pub(crate) fn handle_commit_vote(
        &mut self,
        voter: NodeId,
        vote_msg: FastCommitVoteMsg,
    ) -> Option<FastCommitQc> {
        debug_assert!(self.validator_data.contains(&voter));

        if vote_msg.scope != self.slot {
            return None;
        }

        // no phase guard on purpose
        self.commit_votes.add_vote(voter, vote_msg);
        self.commit_votes.tally(&self.validator_data).strong_qc()
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

    pub(crate) fn handle_fallback_vote(&mut self, voter: NodeId, vote_msg: FallbackVoteMsg) {
        debug_assert!(self.validator_data.contains(&voter));

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
            .add_vote(voter, vote_msg.enter_fallback_vote);

        for (j, evidence) in vote_msg.evidences.into_indexed_iter() {
            match evidence {
                ProposalEvidence::FallbackSignedEntry(entry) => {
                    self.handle_fallback_signed_entry(voter, j, entry);
                }

                ProposalEvidence::Certified(cert) => {
                    if self.certs[j].try_upgrade(cert) {
                        self.recover_certified(j);
                    }
                }
            }
        }
    }

    fn handle_fallback_signed_entry(
        &mut self,
        voter: NodeId,
        j: ProposalIndex,
        entry: FallbackSignedEntry,
    ) {
        let avail = &mut self.availability[j];

        // 1. record seen header, which may admit first-round votes
        if let Some(header) = entry.header()
            && let Some(equiv_cert) = avail.record_header(header.clone())
        {
            self.certs[j].try_upgrade(equiv_cert);
        }
        self.try_form_fast_qc(j);

        // 2. count the vote
        let vote = entry.into_vote_msg(self.slot, j);
        self.fallback_entry_votes[j].add_vote(voter, vote);

        // 3. form fallback qc from f+1 votes
        self.try_form_fallback_qc(j);
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
            ProposalEvidence::Certified(cert) => cert.verify(
                ProposalScope::new(self.slot, j),
                &self.header_auth,
                &self.validator_data,
            ),
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
            let entry = match self.proposers.proposer(*j) {
                // A vacant index has no proposer (rotation vacancy at a
                // handoff, genesis ramp-up, or fewer staked validators than
                // indices): its proposal is empty by definition.
                None => Entry::Negative,
                Some(_) => match self.availability[*j].fetch_proposal() {
                    Some(proposal) => Entry::Positive(*proposal.root()),
                    None => Entry::Negative,
                },
            };

            let vote_msg =
                VoteMsg::new_signed(ProposalScope::new(self.slot, *j), entry.clone(), &self.key);
            SignedEntry {
                entry,
                signature: vote_msg.signature,
            }
        });

        for (j, SignedEntry { entry, .. }) in votes.as_ref().into_indexed_iter() {
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
    pub(crate) fn try_fallback_transition(&mut self) -> FallbackTransitionOutcome {
        if self.phase != Phase::Vote {
            return FallbackTransitionOutcome::AlreadyCommitVoted;
        }

        let states = self.proposals.as_ref().map(|j| self.proposal_state(*j));

        let mut waiting = false;
        for (j, state) in states.as_ref().into_indexed_iter() {
            match state {
                // todo: deliver headers reliably, since chunk pulls pin their root
                ProposalState::NotEnoughVotes => {}
                ProposalState::Unresolved(weak_qc) => self.recover_unresolved(j, weak_qc),
                ProposalState::Certified(_) | ProposalState::FallbackSignedEntry(_) => continue,
            }
            waiting = true;
        }
        if waiting {
            // wait for more votes and chunks to arrive.
            return FallbackTransitionOutcome::Waiting;
        }

        let evidences = states.map(|state| match state {
            ProposalState::Certified(cert) => ProposalEvidence::Certified(cert),
            ProposalState::FallbackSignedEntry(fse) => ProposalEvidence::FallbackSignedEntry(fse),
            ProposalState::NotEnoughVotes | ProposalState::Unresolved(_) => {
                unreachable!("returned above while waiting")
            }
        });

        // cast a fallback vote and enter transition to fallback
        self.phase = Phase::TransitionToFallback;
        let enter_fallback_vote = VoteMsg::new_signed(self.slot, EnterFallbackVote, &self.key);

        // the roots our positive fallback entries vouch for
        for (j, evidence) in evidences.as_ref().into_indexed_iter() {
            if let ProposalEvidence::FallbackSignedEntry(entry) = evidence
                && let Some(header) = entry.header()
            {
                self.emit(ChorusDACommand::PinRoot {
                    j,
                    root: *header.root(),
                });
            }
        }

        let fallback_vote = FallbackVoteMsg {
            enter_fallback_vote,
            evidences,
        };
        FallbackTransitionOutcome::FallbackVote(fallback_vote)
    }

    // D_s + 2Delta
    #[must_use]
    /// The block this validator can enter the fallback path with, and the
    /// certificate admitting it. A held fast metablock is admissible alone
    /// (the paper's MVBA proposal case 1); any other block needs the
    /// enter-fallback certificate (case 2). The certificate is returned
    /// alongside rather than folded into the block: it admits the path, it is
    /// not part of the value the MVBA agrees on, and the caller has to
    /// disseminate it.
    pub(crate) fn on_fallback_deadline(&self) -> Option<(Option<EnterFallbackCert>, Metablock)> {
        let block = self.try_build_fallback_block()?;
        if block.is_fast() {
            return Some((None, block));
        }

        let enter_fallback_cert = self
            .enter_fallback_votes
            .tally(&self.validator_data)
            .strong_qc()?;

        Some((Some(enter_fallback_cert), block))
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

    // per index: admitted voter count, and the (index, voter) pairs whose
    // positive vote names a root without a seen header
    pub(crate) fn vote_admission_state(&self) -> (Vec<usize>, Vec<(usize, NodeId)>) {
        let mut admitted = Vec::new();
        let mut waiting_on_header = Vec::new();
        for j in 0..self.proposals.size() {
            let header_seen = HeaderSeen(&self.availability[j]);
            let mut count = 0;
            for (entry, voters) in self.votes[j].buckets() {
                if header_seen.admits(entry) {
                    count += voters.len();
                    continue;
                }
                for voter in voters {
                    waiting_on_header.push((j, *voter));
                }
            }
            admitted.push(count);
        }
        (admitted, waiting_on_header)
    }

    pub(crate) fn commit_voter_count(&self) -> usize {
        self.commit_votes.all_voters().count()
    }

    // enter-fallback voters seen and the indices still without a certified entry
    pub(crate) fn fallback_wait_state(&self) -> (Vec<NodeId>, Vec<usize>) {
        let voters: Vec<NodeId> = self.enter_fallback_votes.all_voters().copied().collect();
        let uncertified = (0..self.proposals.size())
            .filter(|j| matches!(self.certs[*j], LocalCertifiedEntry::Absent))
            .collect();
        (voters, uncertified)
    }

    // ------- internal helper methods ---------
    fn proposal_state(&self, j: ProposalIndex) -> ProposalState {
        if let LocalCertifiedEntry::Certified(cert) = &self.certs[j] {
            // case 1: proposal already certified (fast qc, fallback qc, or equiv cert)
            return ProposalState::Certified(cert.clone());
        }

        let avail = &self.availability[j];
        let tally = self.votes[j]
            .tally(&self.validator_data)
            .narrow(HeaderSeen(avail));

        let supermajority = self.validator_data.total_stake().supermajority_threshold();
        if tally.stake() <= supermajority {
            // case 2: lacking 2f+1 votes yet
            return ProposalState::NotEnoughVotes;
        }

        let weak_qcs = tally.weak_qcs();
        let has_neg_weak_qc = weak_qcs
            .iter()
            .any(|qc| matches!(qc.verdict, Entry::Negative));
        let pos_weak_qcs = weak_qcs
            .into_iter()
            .filter_map(|qc| match qc.verdict {
                Entry::Positive(root) => Some((qc, root)),
                _ => None,
            })
            .collect::<ArrayVec<_, 2>>();
        let scope = ProposalScope::new(self.slot, j);

        let fse = match pos_weak_qcs.as_slice() {
            [] => {
                // case 3: <f+1 positive votes, sign a negative fallback entry
                FallbackSignedEntry::new_signed_negative(scope, &self.key)
            }
            [_, _] => {
                unreachable!("equivocation, already handled in case 1")
            }
            [(weak_qc, root)] if !avail.is_resolved(root) && !has_neg_weak_qc => {
                // case 4: f+1 positive votes, but we have yet to
                // decode it. request voters for their chunks.
                return ProposalState::Unresolved(weak_qc.clone());
            }
            [(weak_qc, root)] if !avail.is_resolved(root) && has_neg_weak_qc => {
                // case 5: f+1 positive votes on unresolved root, but
                // we already have f+1 negative votes. we stand with
                // the negative.
                FallbackSignedEntry::new_signed_negative(scope, &self.key)
            }
            [(_weak_qc, root)] if avail.is_resolved(root) && !avail.decoded(root) => {
                // case 6: decoded but proposal is invalid
                // (re-encoding failure), sign a negative fallback
                // entry.

                // todo: >=f+1 byzantine peers detected (voted yes on
                // invalid root), log or report the voter.
                FallbackSignedEntry::new_signed_negative(scope, &self.key)
            }
            [(_weak_qc, root)] => {
                // case 7: decoded, sign a positive fallback entry
                let header = avail
                    .header_for(root)
                    .expect("already narrowed to roots with a seen header");
                FallbackSignedEntry::new_signed_positive(scope, *root, &self.key, header.clone())
            }
            _ => unreachable!("at most 2 weak qcs"),
        };

        ProposalState::FallbackSignedEntry(fse)
    }

    // the signers of the weak qc hold their own shares under its root
    fn recover_unresolved(&mut self, j: ProposalIndex, weak_qc: &WeakQc<Entry>) {
        let Entry::Positive(root) = weak_qc.verdict else {
            unreachable!("an unresolved weak qc is positive");
        };
        let Some(signers) = weak_qc.sigcol.signers(&self.validator_data) else {
            return;
        };
        let voters = signers.into_iter().copied().collect();
        self.request_chunks(ChunkRequestType::YourChunks, j, root, voters);
    }

    // P1 for a committed slot: pull every committed root we have not
    // resolved from the commit certificate's signers
    pub(crate) fn is_resolved(&self, j: ProposalIndex, root: &MerkleRoot) -> bool {
        self.availability[j].is_resolved(root)
    }

    fn try_form_fallback_qc(&mut self, j: ProposalIndex) {
        if self.certs[j].strength() >= EvidenceStrength::FallbackQc {
            // already have a fallback qc or stronger.
            return;
        }

        let mut weak_qcs = self.fallback_entry_votes[j]
            .tally(&self.validator_data)
            .weak_qcs()
            .into_iter();
        let weak_qc = match (weak_qcs.next(), weak_qcs.next()) {
            (None, _) => return,
            (Some(qc), None) => qc,
            // at most one is positive: two positive entries carry
            // rival headers, whose EquivCert outranks a FallbackQc
            // and returned above
            (Some(qc1), Some(qc2)) => match (&qc1.verdict.0, &qc2.verdict.0) {
                (Entry::Positive { .. }, _) => qc1,
                (_, Entry::Positive { .. }) => qc2,
                _ => unreachable!("two distinct fallback weak qcs must include a positive"),
            },
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

        let fast_qc = self.votes[j]
            .tally(&self.validator_data)
            .narrow(HeaderSeen(&self.availability[j]))
            .strong_qc();
        if let Some(fast_qc) = fast_qc
            && self.certs[j].try_upgrade(fast_qc)
        {
            self.recover_certified(j);
        }
    }
}

// the state of a proposal at fallback transition
enum ProposalState {
    // no reaching 2f+1 votes yet
    NotEnoughVotes,

    // out of 2f+1 votes, f+1 positive votes for a root unresolved
    // locally. triggers recovery.
    Unresolved(WeakQc<Entry>),

    // a qc/equiv_cert already formed
    Certified(CertifiedEntry),

    // f+1 positive votes (w/ root resolved), or f+1 negative votes (or invalid decoding).
    FallbackSignedEntry(FallbackSignedEntry),
}

/// The verdict on one proposal index: `Positive` carries the merkle root of
/// the included proposal, `Negative` finalizes the index empty. Part of the
/// finalization data — downstream consumers (ledger sequencing) read it.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Entry {
    Positive(MerkleRoot),
    Negative,
}

pub struct EntryDomain;
const _: () = assert_signing_prefix::<EntryDomain>();

impl SigningDomain for EntryDomain {
    const PREFIX: &'static [u8] = b"\x1Emonad/cadence/proposal-vote/1\n";
}

impl IsVote for Entry {
    type Scope = ProposalScope;
    type SigningDomain = EntryDomain;
}

// admits a positive vote once its root's header is seen
struct HeaderSeen<'a>(&'a ProposalAvailability);

impl Gate<Entry> for HeaderSeen<'_> {
    fn admits(&self, vote: &Entry) -> bool {
        let Entry::Positive(root) = vote else {
            return true;
        };
        self.0.header_for(root).is_some()
    }
}

pub type FastQc = StrongQc<Entry>;

#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
pub(crate) struct BatchVoteMsg {
    slot: Slot,
    votes: ProposalMap<SignedEntry>,
    // vote only. fields for chunks & decryption share may be added by
    // other components.
}

impl BatchVoteMsg {
    // one char per index, + positive / - negative, for logs
    pub(crate) fn shape(&self) -> String {
        self.votes
            .as_ref()
            .into_indexed_iter()
            .map(|(_, signed)| match signed.entry {
                Entry::Positive(_) => '+',
                Entry::Negative => '-',
            })
            .collect()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
struct SignedEntry {
    entry: Entry,
    signature: Signature,
}

impl BatchVoteMsg {
    pub fn split(self) -> Vec<VoteMsg<Entry>> {
        self.votes
            .into_indexed_iter()
            .map(|(j, SignedEntry { entry, signature })| {
                VoteMsg::new(ProposalScope::new(self.slot, j), entry, signature)
            })
            .collect()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FastCommitVote {
    pub entries: ProposalMap<Entry>,
}

pub(crate) type FastCommitVoteMsg = VoteMsg<FastCommitVote>;

#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
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

pub struct FastCommitVoteDomain;
const _: () = assert_signing_prefix::<FastCommitVoteDomain>();

impl SigningDomain for FastCommitVoteDomain {
    const PREFIX: &'static [u8] = b"\x1Cmonad/cadence/fast-commit/1\n";
}

impl IsVote for FastCommitVote {
    type Scope = Slot;
    type SigningDomain = FastCommitVoteDomain;
}

pub type FastCommitQc = StrongQc<FastCommitVote>;

// ============ Fallback ===============

// same as Entry, but signed under a distinct signing domain
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct FallbackEntry(pub Entry);

pub struct FallbackEntryDomain;
const _: () = assert_signing_prefix::<FallbackEntryDomain>();

impl SigningDomain for FallbackEntryDomain {
    const PREFIX: &'static [u8] = b"\x27monad/cadence/fallback-proposal-vote/1\n";
}

impl IsVote for FallbackEntry {
    type Scope = ProposalScope;
    type SigningDomain = FallbackEntryDomain;
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
        scope: ProposalScope,
        header_auth: &HeaderAuth,
        validator_data: &ValidatorData,
    ) -> bool {
        match self {
            CertifiedEntry::FastQc(qc) => qc.verify(validator_data),
            CertifiedEntry::FallbackQc(qc) => qc.verify(validator_data),
            CertifiedEntry::EquivCert(EquivCert(a, b)) => {
                let ProposalScope { slot: s, index: j } = scope;
                a.root() != b.root()
                    && header_auth.validate(a, s.get(), j)
                    && header_auth.validate(b, s.get(), j)
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
struct FallbackSignedEntry {
    entry: FallbackEntry,
    // over (slot, j, self.entry)
    signature: Signature,
    // invariant: header.is_some() iff entry is positive
    // invariant: header.root() == entry.root
    header: Option<SignedProposalHeader>,
}

impl FallbackSignedEntry {
    fn new_signed_positive(
        scope: ProposalScope,
        root: MerkleRoot,
        key: &KeyPair,
        header: SignedProposalHeader,
    ) -> Self {
        let entry = FallbackEntry(Entry::Positive(root));
        let signature = VoteMsg::new_signed(scope, entry.clone(), key).signature;
        Self {
            entry,
            signature,
            header: Some(header),
        }
    }

    fn new_signed_negative(scope: ProposalScope, key: &KeyPair) -> Self {
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
                .is_some_and(|header| header.root() == root),
            Entry::Negative => self.header.is_none(),
        }
    }

    fn header(&self) -> Option<&SignedProposalHeader> {
        self.header.as_ref()
    }

    fn into_vote_msg(self, slot: Slot, j: ProposalIndex) -> VoteMsg<FallbackEntry> {
        VoteMsg::new(ProposalScope::new(slot, j), self.entry, self.signature)
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

#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
pub struct EnterFallbackVote;

pub struct EnterFallbackVoteDomain;
const _: () = assert_signing_prefix::<EnterFallbackVoteDomain>();

impl SigningDomain for EnterFallbackVoteDomain {
    const PREFIX: &'static [u8] = b"\x1Fmonad/cadence/enter-fallback/1\n";
}

impl IsVote for EnterFallbackVote {
    type Scope = Slot;
    type SigningDomain = EnterFallbackVoteDomain;
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
pub(crate) struct FallbackVoteMsg {
    enter_fallback_vote: VoteMsg<EnterFallbackVote>,
    evidences: ProposalMap<ProposalEvidence>,
}

// A fallback cert certifies 2f+1 validators agree to enter fallback path
pub type EnterFallbackCert = StrongQc<EnterFallbackVote>;

impl Encodable for Entry {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            Self::Positive(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Negative => {
                let fields: [&dyn Encodable; 1] = [&2u8];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::Positive(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::Negative => {
                let fields: [&dyn Encodable; 1] = [&2u8];
                list_length::<_, dyn Encodable>(&fields)
            }
        }
    }
}

impl Decodable for Entry {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let result = match <u8 as Decodable>::decode(&mut payload)? {
            1 => Self::Positive(<MerkleRoot as Decodable>::decode(&mut payload)?),
            2 => Self::Negative,
            _ => return Err(alloy_rlp::Error::Custom("unknown Entry tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

impl Encodable for CertifiedEntry {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            Self::FastQc(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::EquivCert(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::FallbackQc(message) => {
                let fields: [&dyn Encodable; 2] = [&3u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::FastQc(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::EquivCert(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::FallbackQc(message) => {
                let fields: [&dyn Encodable; 2] = [&3u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
        }
    }
}

impl Decodable for CertifiedEntry {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let result = match <u8 as Decodable>::decode(&mut payload)? {
            1 => Self::FastQc(<FastQc as Decodable>::decode(&mut payload)?),
            2 => Self::EquivCert(<EquivCert as Decodable>::decode(&mut payload)?),
            3 => Self::FallbackQc(<FallbackQc as Decodable>::decode(&mut payload)?),
            _ => return Err(alloy_rlp::Error::Custom("unknown CertifiedEntry tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

impl Encodable for ProposalEvidence {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            Self::Certified(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::FallbackSignedEntry(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::Certified(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::FallbackSignedEntry(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
        }
    }
}

impl Decodable for ProposalEvidence {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let result = match <u8 as Decodable>::decode(&mut payload)? {
            1 => Self::Certified(<CertifiedEntry as Decodable>::decode(&mut payload)?),
            2 => {
                Self::FallbackSignedEntry(<FallbackSignedEntry as Decodable>::decode(&mut payload)?)
            }
            _ => return Err(alloy_rlp::Error::Custom("unknown ProposalEvidence tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

// Alloy 0.3.12's wrapper decoder derive only constructs tuple newtypes.
// Keep both wrapper codecs manual for this named-field struct.
impl Encodable for FastCommitVote {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        self.entries.encode(out);
    }

    fn length(&self) -> usize {
        self.entries.length()
    }
}

impl Decodable for FastCommitVote {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        Ok(Self {
            entries: <ProposalMap<Entry> as Decodable>::decode(buf)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            super::proposers,
            chorus::ProposalDAEvent,
            types::{FixedProposerSchedule, ProposerSchedule as _, Stake, ValidatorData},
        },
        *,
    };
    use crate::{
        env::stub::{D25, EncodingScheme, MerkleHash, ProposalHeader, ProposalSignature},
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
    fn header(byte: u8) -> SignedProposalHeader {
        SignedProposalHeader {
            header: ProposalHeader {
                root: root(byte),
                scheme: EncodingScheme::D25(D25 {
                    slot: crate::stub::types::Slot(SLOT.get()),
                    msg_len: 1,
                    unix_ts: 0,
                    depth: 3,
                }),
            },
            sig: ProposalSignature {
                signer: NodeId::dummy(0),
                checksum: 0,
            },
        }
    }

    // the local node is validator 1 among 4, one proposal per slot
    fn fast_path() -> FastPath {
        // index 0 is held by validator 0; consensus and header
        // authentication read the one schedule, so they cannot disagree
        let schedule = Arc::new(FixedProposerSchedule::new(vec![NodeId::dummy(0)]));
        let proposers = schedule
            .proposers_at(SLOT)
            .expect("fixed schedule is always available");
        FastPath::new(
            SLOT,
            proposers,
            Arc::new(NodeId::dummy(1).keypair()),
            Arc::new(validator_data(4)),
            Arc::new(proposers::header_auth(schedule)),
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

    // a fast qc on root(byte) signed by validators 0, 2 and 3
    fn fast_block(byte: u8) -> FastBlock {
        let mut pool = VotePool::new(ProposalScope::new(SLOT, 0));
        for id in [0, 2, 3] {
            let voter = NodeId::dummy(id);
            let msg = VoteMsg::new_signed(
                ProposalScope::new(SLOT, 0),
                Entry::Positive(root(byte)),
                &voter.keypair(),
            );
            pool.add_vote(voter, msg);
        }
        let qc = pool
            .tally(&validator_data(4))
            .strong_qc()
            .expect("three of four votes form a fast qc");
        FastBlock(ProposalMap::new(1, |_| qc.clone()))
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
            let signature =
                VoteMsg::new_signed(ProposalScope::new(SLOT, j), entry.clone(), &key).signature;
            SignedEntry {
                entry: entry.clone(),
                signature,
            }
        });
        (voter, BatchVoteMsg { slot: SLOT, votes })
    }

    fn da_event(fast: &mut FastPath, event: ProposalDAEvent) {
        let _ = fast.handle_da_event(ChorusDAEvent { j: 0, event });
    }

    #[test]
    fn a_blocked_transition_pulls_unresolved_roots_from_their_voters() {
        let mut fast = fast_path();
        let _ = fast.on_deadline();
        let votes = [
            batch_vote(0, Entry::Positive(root(1))),
            batch_vote(3, Entry::Positive(root(1))),
            batch_vote(2, Entry::Negative),
        ];
        for (voter, msg) in votes {
            let _ = fast.handle_batch_vote(voter, msg);
        }
        // the voters relay their own shares, so a vote alone pulls nothing
        assert!(drain_requests(&mut fast).is_empty());

        // without the header the votes do not count, and nothing is pulled
        let outcome = fast.try_fallback_transition();
        assert!(matches!(outcome, FallbackTransitionOutcome::Waiting));
        assert!(drain_requests(&mut fast).is_empty());

        // f+1 positive votes on an unresolved root: pull it, again each deadline
        da_event(&mut fast, ProposalDAEvent::HeaderSeen(header(1)));
        let voters = vec![NodeId::dummy(0), NodeId::dummy(3)];
        let expected = (ChunkRequestType::YourChunks, root(1), voters);
        for _ in 0..2 {
            let outcome = fast.try_fallback_transition();
            assert!(matches!(outcome, FallbackTransitionOutcome::Waiting));
            assert_eq!(drain_requests(&mut fast), vec![expected.clone()]);
        }

        da_event(&mut fast, ProposalDAEvent::Decoded(root(1)));
        let outcome = fast.try_fallback_transition();
        assert!(matches!(
            outcome,
            FallbackTransitionOutcome::FallbackVote(_)
        ));
        assert!(drain_requests(&mut fast).is_empty());
    }

    #[test]
    fn a_positive_vote_counts_once_its_header_is_seen() {
        let mut fast = fast_path();
        for voter in [0, 2, 3] {
            let (voter, msg) = batch_vote(voter, Entry::Positive(root(1)));
            let _ = fast.handle_batch_vote(voter, msg);
        }
        assert!(fast.certs[0].fast_qc().is_none());

        da_event(&mut fast, ProposalDAEvent::HeaderSeen(header(1)));
        assert!(fast.certs[0].fast_qc().is_some());
    }

    #[test]
    fn the_transition_waits_for_headers_then_for_resolution() {
        let mut fast = fast_path();
        let _ = fast.on_deadline();
        let votes = [
            batch_vote(0, Entry::Positive(root(1))),
            batch_vote(3, Entry::Positive(root(1))),
            batch_vote(2, Entry::Negative),
        ];
        for (voter, msg) in votes {
            let _ = fast.handle_batch_vote(voter, msg);
        }

        // the positive votes name a root without a header
        let outcome = fast.try_fallback_transition();
        assert!(matches!(outcome, FallbackTransitionOutcome::Waiting));

        // f+1 positive votes on an unresolved root leave the entry undecided
        da_event(&mut fast, ProposalDAEvent::HeaderSeen(header(1)));
        let outcome = fast.try_fallback_transition();
        assert!(matches!(outcome, FallbackTransitionOutcome::Waiting));

        da_event(&mut fast, ProposalDAEvent::Decoded(root(1)));
        let FallbackTransitionOutcome::FallbackVote(msg) = fast.try_fallback_transition() else {
            panic!("the decoded root decides the entry");
        };
        let ProposalEvidence::FallbackSignedEntry(entry) = &msg.evidences[0] else {
            panic!("no certificate formed");
        };
        assert_eq!(entry.entry, FallbackEntry(Entry::Positive(root(1))));
    }

    /// [`FallbackEntry`] wraps [`Entry`] transparently, so the two encode
    /// alike; only the domain keeps a fast-path vote from being read as a
    /// fallback vote on the same proposal.
    #[test]
    fn a_wrapped_entry_signs_under_its_own_domain() {
        let scope = ProposalScope::new(SLOT, 0);
        let entry = Entry::Positive(root(1));
        let fallback = FallbackEntry(entry.clone());
        assert_eq!(alloy_rlp::encode(&entry), alloy_rlp::encode(&fallback));

        let entry_bytes = entry.signing_bytes(&scope);
        let fallback_bytes = fallback.signing_bytes(&scope);
        assert_ne!(entry_bytes, fallback_bytes);
        assert!(entry_bytes.starts_with(EntryDomain::PREFIX));
        assert!(fallback_bytes.starts_with(FallbackEntryDomain::PREFIX));
    }

    /// Past the prefix the signed bytes are exactly the RLP list
    /// `[scope, vote]`.
    #[test]
    fn signing_bytes_are_the_domain_prefix_then_scope_and_vote() {
        let scope = ProposalScope::new(SLOT, 1);
        let entry = Entry::Positive(root(2));

        let bytes = entry.signing_bytes(&scope);
        let mut payload = bytes
            .strip_prefix(EntryDomain::PREFIX)
            .expect("the domain prefix leads the signed bytes");
        let mut list = Header::decode_bytes(&mut payload, true).expect("a two-item list");

        assert_eq!(ProposalScope::decode(&mut list).unwrap(), scope);
        assert_eq!(Entry::decode(&mut list).unwrap(), entry);
        assert!(list.is_empty(), "nothing follows the vote");
        assert!(payload.is_empty(), "nothing follows the list");
    }

    /// A certificate is bound to the scope its votes were signed under: the
    /// same signatures relabelled with another proposal index verify as
    /// neither a strong nor a weak quorum.
    #[test]
    fn a_certificate_does_not_verify_under_a_foreign_scope() {
        let validators = validator_data(4);
        let scope = ProposalScope::new(SLOT, 0);
        let foreign = ProposalScope::new(SLOT, 1);

        let mut pool = VotePool::new(scope);
        for id in [0, 2, 3] {
            let voter = NodeId::dummy(id);
            let msg = VoteMsg::new_signed(scope, Entry::Positive(root(1)), &voter.keypair());
            pool.add_vote(voter, msg);
        }

        let strong = pool
            .tally(&validators)
            .strong_qc()
            .expect("three of four votes form a strong qc");
        assert!(strong.verify(&validators));
        assert!(
            !StrongQc {
                scope: foreign,
                ..strong
            }
            .verify(&validators)
        );

        let mut weak_qcs = pool.tally(&validators).weak_qcs().into_iter();
        let weak = match (weak_qcs.next(), weak_qcs.next()) {
            (Some(qc), None) => qc,
            (None, _) => panic!("three of four votes exceed the honest threshold"),
            (Some(_), Some(_)) => panic!("the voters all cast the same entry"),
        };
        assert!(weak.verify(&validators));
        assert!(
            !WeakQc {
                scope: foreign,
                ..weak
            }
            .verify(&validators)
        );
    }
}

#[cfg(test)]
mod rlp_tests {
    use bytes::Bytes;

    use super::{
        super::{
            super::{
                conductor::{MonadConductor, acs::median::MedianAcs},
                message::CadenceWireMsg,
                test_utils::{assert_roundtrip, assert_serialization_roundtrip},
                types::SlotDeadline,
            },
            chorus::{Chorus, ChorusMessage},
        },
        *,
    };
    use crate::{
        env::stub::{D25, EncodingScheme, MerkleHash, ProposalHeader, ProposalSignature},
        spec::vote::KeyPair as _,
    };

    #[test]
    fn chorus_variants_and_nested_evidence_roundtrip() {
        let slot = Slot(9);
        let key = NodeId::dummy(1).keypair();
        let root = MerkleRoot(MerkleHash([7; 20]));
        let positive = Entry::Positive(root);
        let signature = key.sign(&Bytes::from_static(b"test"));
        // Empty collections suffice for wire tests; validation remains separate.
        let sigcol = alloy_rlp::decode_exact([0xc0]).unwrap();
        let fast_qc = StrongQc {
            scope: ProposalScope::new(slot, 0usize),
            verdict: positive.clone(),
            sigcol,
        };
        let weak_qc = WeakQc {
            scope: ProposalScope::new(slot, 0usize),
            verdict: FallbackEntry(Entry::Negative),
            sigcol: fast_qc.sigcol.clone(),
        };
        let h = SignedProposalHeader {
            header: ProposalHeader {
                root,
                scheme: EncodingScheme::D25(D25 {
                    slot: crate::stub::types::Slot(9),
                    msg_len: 1000,
                    unix_ts: 12345,
                    depth: 4,
                }),
            },
            sig: ProposalSignature {
                signer: NodeId::dummy(1),
                checksum: 12,
            },
        };
        let mut h2 = h.clone();
        h2.header.root = MerkleRoot(MerkleHash([8; 20]));
        // Cover every certificate variant and signed fallback entries with/without a header.
        let evidence = vec![
            ProposalEvidence::Certified(CertifiedEntry::FastQc(fast_qc.clone())),
            ProposalEvidence::Certified(CertifiedEntry::FallbackQc(weak_qc)),
            ProposalEvidence::Certified(CertifiedEntry::EquivCert(EquivCert(h.clone(), h2))),
            ProposalEvidence::FallbackSignedEntry(FallbackSignedEntry::new_signed_positive(
                ProposalScope::new(slot, 0),
                root,
                &key,
                h,
            )),
            ProposalEvidence::FallbackSignedEntry(FallbackSignedEntry::new_signed_negative(
                ProposalScope::new(slot, 1),
                &key,
            )),
        ];
        for value in &evidence {
            assert_roundtrip(value);
        }
        let enter = StrongQc {
            scope: slot,
            verdict: EnterFallbackVote,
            sigcol: fast_qc.sigcol.clone(),
        };
        let fast_commit_vote = FastCommitVote {
            entries: ProposalMap::new(2, |i| {
                if i == 0 {
                    positive.clone()
                } else {
                    Entry::Negative
                }
            }),
        };
        // Cover all six non-MVBA Chorus variants; MVBA messages have their own round-trip test.
        let messages = vec![
            ChorusMessage::BatchVote(BatchVoteMsg {
                slot,
                votes: ProposalMap::new(2, |i| SignedEntry {
                    entry: if i == 0 {
                        positive.clone()
                    } else {
                        Entry::Negative
                    },
                    signature: signature.clone(),
                }),
            }),
            ChorusMessage::FastCommitVote(VoteMsg::new_signed(
                slot,
                fast_commit_vote.clone(),
                &key,
            )),
            ChorusMessage::FastBlock(FastBlock(ProposalMap::new(2, |_| fast_qc.clone()))),
            ChorusMessage::FallbackVote(FallbackVoteMsg {
                enter_fallback_vote: VoteMsg::new_signed(slot, EnterFallbackVote, &key),
                evidences: ProposalMap::new(evidence.len(), |i| evidence[i].clone()),
            }),
            ChorusMessage::FastCommitQc(StrongQc {
                scope: slot,
                verdict: fast_commit_vote,
                sigcol: fast_qc.sigcol,
            }),
            ChorusMessage::EnterFallbackCert(enter),
        ];
        type Wire = CadenceWireMsg<Chorus, MonadConductor<MedianAcs<SlotDeadline>>>;
        // Check both the Chorus RLP payload and its enclosing Cadence byte serialization.
        for message in messages {
            assert_roundtrip(&message);
            assert_serialization_roundtrip(&Wire::Slot(slot, message));
        }
    }

    #[test]
    fn transparent_wrappers_and_unit_votes_have_no_extra_list() {
        fn encode_scope<V: IsVote>(scope: &V::Scope) -> Vec<u8> {
            alloy_rlp::encode(scope)
        }
        let scope = ProposalScope::new(Slot(7), 3);
        assert_eq!(encode_scope::<Entry>(&scope), [0xc2, 7, 3]);
        assert_roundtrip(&scope);
        let root = MerkleRoot(MerkleHash([7; 20]));
        assert_eq!(alloy_rlp::encode(root), alloy_rlp::encode([7u8; 20]));
        let entry = Entry::Negative;
        assert_eq!(alloy_rlp::encode(&entry), [0xc1, 2]);
        assert_eq!(alloy_rlp::encode(FallbackEntry(entry)), [0xc1, 2]);
        assert_eq!(alloy_rlp::encode(EnterFallbackVote), [0xc0]);
        assert_roundtrip(&EnterFallbackVote);
        let entries = ProposalMap::new(0, |_| Entry::Negative);
        assert_eq!(alloy_rlp::encode(FastCommitVote { entries }), [0xc0]);
        let block = FastBlock(ProposalMap::new(0, |_| unreachable!()));
        assert_eq!(alloy_rlp::encode(&block), [0xc0]);
        assert_roundtrip(&block);
        for bad in [&[0xc1, 3][..], &[0xc2, 2, 0][..], &[0x80][..]] {
            assert!(alloy_rlp::decode_exact::<Entry>(bad).is_err());
        }
    }
}
