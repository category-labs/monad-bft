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

use std::{cmp::Reverse, collections::VecDeque, sync::Arc};

use alloy_rlp::{
    Decodable, Encodable, RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper,
};
use arrayvec::ArrayVec;

use super::{
    availability::ProposalAvailability,
    chorus::{ChorusDACommand, ChorusDAEvent, MissingHeaders},
    fallback::Metablock,
    types::{
        EquivCert, Gate, HeaderAuth, IsVote, KeyPair, Lane, Lanes, NodeId, ProposalId,
        ProposalScope, ProposerSet, Signature, SignedProposalHeader, Slot, StrongQc, ValidatorData,
        VoteMsg, VotePool, WeakQc,
    },
};
use crate::spec::{
    proposal::{HeaderAuth as _, ProposalHeader as _},
    validator::ValidatorData as _,
    vote::{SignatureCollection as _, SigningDomain, assert_signing_prefix},
};

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum Phase {
    Initial,        // initial phase
    Vote,           // vote casted
    FastCommitVote, // fast commit vote cast
    FallbackVote,   // fallback vote cast
}

pub enum FallbackTransitionOutcome {
    AlreadyVoted, // already voted for fast commit
    Waiting,      // retry after Delta
    FallbackVote(FallbackVoteMsg),
}

#[derive(Clone)]
pub struct FastPath {
    slot: Slot,

    certs: Lanes<LocalCertifiedEntry>,
    phase: Phase,

    votes: Lanes<VotePool<Entry>>,
    commit_votes: VotePool<FastCommitVote>,

    enter_fallback_votes: VotePool<EnterFallbackVote>,
    fallback_entry_votes: Lanes<VotePool<FallbackEntry>>,

    // helper field to construct per-proposal values
    proposals: Lanes<Lane>,

    // what we know about each proposal's DA
    availability: Lanes<ProposalAvailability>,

    // effects for the DA layer, drained by the slot consensus wrapper
    commands: VecDeque<ChorusDACommand>,

    // who occupies each lane in this slot
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
        let num_lanes = proposers.num_lanes();
        Self {
            slot: s,

            votes: Lanes::new(num_lanes, |j| VotePool::new(ProposalScope::new(s, j))),
            certs: Lanes::new_default(num_lanes),
            commit_votes: VotePool::new(s),

            enter_fallback_votes: VotePool::new(s),
            fallback_entry_votes: Lanes::new(num_lanes, |j| {
                VotePool::new(ProposalScope::new(s, j))
            }),

            phase: Phase::Initial,
            proposals: Lanes::new(num_lanes, |j| j),
            availability: Lanes::new_default(num_lanes),
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
        let in_slot = vote_msg.slot == self.slot;
        let one_vote_per_lane = vote_msg.votes.size() == self.proposals.size();
        if !in_slot || !one_vote_per_lane {
            return None;
        }

        for vote_msg in vote_msg.split() {
            self.handle_vote(voter, vote_msg);
        }

        self.try_cast_fast_commit_vote()
    }

    fn handle_vote(&mut self, voter: NodeId, vote_msg: VoteMsg<Entry>) {
        debug_assert!(self.validator_data.contains(&voter));

        let j = vote_msg.scope.lane;
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

        let in_slot = vote_msg.scope == self.slot;
        let one_entry_per_lane = vote_msg.vote.entries.size() == self.proposals.size();
        if !in_slot || !one_entry_per_lane {
            return None;
        }

        // no phase guard on purpose
        self.commit_votes.add_vote(voter, vote_msg);
        self.commit_votes.tally(&self.validator_data).strong_qc()
    }

    pub(crate) fn handle_fast_block(&mut self, fast_block: FastBlock) -> Option<FastBlock> {
        if !self.fast_block_is_valid(&fast_block) {
            return None;
        }

        for (j, qc) in fast_block.0.into_indexed_iter() {
            self.certs[j].try_upgrade(qc);
        }

        self.try_cast_fast_commit_vote()
    }

    fn fast_block_is_valid(&self, fast_block: &FastBlock) -> bool {
        if fast_block.0.size() != self.proposals.size() {
            return false;
        }
        fast_block.0.as_ref().into_indexed_iter().all(|(j, qc)| {
            let at_its_lane = qc.scope == ProposalScope::new(self.slot, j);
            at_its_lane && qc.verify(&self.validator_data)
        })
    }

    pub(crate) fn fast_commit_qc_is_valid(&self, qc: &FastCommitQc) -> bool {
        qc.scope == self.slot
            && qc.verdict.entries.size() == self.proposals.size()
            && qc.verify(&self.validator_data)
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
                    self.certs[j].try_upgrade(cert);
                }
            }
        }
    }

    fn handle_fallback_signed_entry(&mut self, voter: NodeId, j: Lane, entry: FallbackSignedEntry) {
        // 1. an unseen header goes to DA
        if let Some(header) = entry.header()
            && self.availability[j].header_for(&header.pid()).is_none()
        {
            let header = header.clone();
            self.emit(ChorusDACommand::IngestProposalHeader {
                sender: voter,
                header,
            });
        }

        // 2. count the vote
        let vote = entry.into_vote_msg(self.slot, j);
        self.fallback_entry_votes[j].add_vote(voter, vote);

        // 3. form fallback qc from f+1 votes
        self.try_form_fallback_qc(j);
    }

    fn evidence_valid(&self, j: Lane, evidence: &ProposalEvidence) -> bool {
        match evidence {
            ProposalEvidence::FallbackSignedEntry(entry) => match entry.header() {
                Some(header) => self.header_auth.validate(header, self.slot.get(), j),
                None => true,
            },
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
        // a FastBlock arriving before deadline can skip voting phase
        // (enter FastCommitVote). We will always serve our chunks
        // after deadline regardless of current phase.
        self.emit(ChorusDACommand::ReleaseChunks);

        if self.phase != Phase::Initial {
            return None; // already voted; no-op
        }

        self.phase = Phase::Vote;

        let votes = self.proposals.as_ref().map(|j| {
            let entry = match self.proposers.proposer(*j) {
                // A vacant lane has no proposer (rotation vacancy at a
                // handoff, genesis ramp-up, or fewer staked validators than
                // lanes): its proposal is empty by definition.
                None => Entry::Negative,
                Some(_) => match self.availability[*j].fetch_proposal() {
                    Some(proposal) => Entry::Positive(proposal.pid()),
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

        Some(BatchVoteMsg {
            slot: self.slot,
            votes,
        })
    }

    // D_s + Delta, ..., D_s + N*Delta
    #[must_use]
    pub(crate) fn try_fallback_transition(&mut self) -> FallbackTransitionOutcome {
        match self.phase {
            Phase::Initial => panic!("fallback transition (D_s+Delta) before voting (D_s)"),
            Phase::FastCommitVote => return FallbackTransitionOutcome::AlreadyVoted,
            Phase::FallbackVote => panic!("fallback transition attempt after fallback vote"),
            Phase::Vote => {}
        }

        let states = self.proposals.as_ref().map(|j| self.proposal_state(*j));
        for (j, state) in states.as_ref().into_indexed_iter() {
            if state.is_some() {
                continue;
            }

            // pull the unseen headers when we're undecided
            let missing = self.missing_headers(j);
            if !missing.is_empty() {
                self.emit(ChorusDACommand::PullHeader { j, missing });
            }
        }

        let Some(states) = states.try_into_total::<ProposalState>() else {
            // some proposals are still undecided, wait for more votes/chunks
            return FallbackTransitionOutcome::Waiting;
        };

        // fallback transition decided
        self.phase = Phase::FallbackVote;

        let evidences = states.map_indexed(|j, state| {
            let scope = ProposalScope::new(self.slot, j);
            state.into_evidence(scope, &self.key)
        });

        let enter_fallback_vote = VoteMsg::new_signed(self.slot, EnterFallbackVote, &self.key);
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

    // per lane: admitted voter count, and the (lane, voter) pairs whose
    // positive vote names a proposal without a seen header
    pub(crate) fn vote_admission_state(&self) -> (Vec<usize>, Vec<(Lane, NodeId)>) {
        let mut admitted = Vec::new();
        let mut waiting_on_header = Vec::new();
        for j in self.proposals.lanes() {
            let header_seen = HeaderSeen(&self.availability[j]);
            let mut count = 0;
            for (entry, voters) in self.votes[j].buckets() {
                if header_seen.admits(entry) {
                    count += voters.len();
                    continue;
                }
                // the list only reaches a debug log
                #[allow(clippy::iter_over_hash_type)]
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

    // enter-fallback voters seen and the lanes still without a certified entry
    pub(crate) fn fallback_wait_state(&self) -> (Vec<NodeId>, Vec<Lane>) {
        let voters: Vec<NodeId> = self.enter_fallback_votes.all_voters().copied().collect();
        let uncertified = self
            .proposals
            .lanes()
            .filter(|j| matches!(self.certs[*j], LocalCertifiedEntry::Absent))
            .collect();
        (voters, uncertified)
    }

    // ------- internal helper methods ---------
    fn proposal_state(&self, j: Lane) -> Option<ProposalState> {
        use monad_mcp_common::spec::Stake as _;

        let tally = self.votes[j].tally(&self.validator_data);
        let supermajority = self.validator_data.total_stake().supermajority_threshold();

        if tally.stake() <= supermajority {
            // case 1: lacking 2f+1 votes yet
            return None;
        }

        if let LocalCertifiedEntry::Certified(cert) = &self.certs[j] {
            // case 2: proposal already certified (fast qc, fallback qc, or equiv cert)
            return Some(ProposalState::Certified(Box::new(cert.clone())));
        }

        let avail = &self.availability[j];
        let weak_qcs = tally.weak_qcs();

        if weak_qcs
            .iter()
            .any(|qc| matches!(qc.verdict, Entry::Negative))
        {
            // case 3a: f+1 negative votes, vote fallback negative
            return Some(ProposalState::FallbackNegative);
        }

        let pos_weak_qcs = weak_qcs
            .into_iter()
            .map(|qc| match qc.verdict {
                Entry::Positive(pid) => Some((qc, pid)),
                _ => None,
            })
            .collect::<Option<ArrayVec<_, 2>>>()
            .expect("no negative weak qcs, so all weak qcs are positive");

        for (_qc, pid) in pos_weak_qcs {
            // qc: f+1 positive votes
            let Some(header) = avail.header_for(&pid) else {
                continue;
            };

            if !avail.is_resolved(&pid) {
                // case 5: header seen, not resolved. DA will pull the
                // pinned proposal by itself
                return None;
            }

            if !avail.is_decoded(&pid) {
                // case 3b: f+1 positive votes, but decoding is invalid
                return Some(ProposalState::FallbackNegative);
            }

            // case 4: resolved and decoded, vote fallback positive
            return Some(ProposalState::FallbackPositive(header.clone()));
        }

        // case 6: positive votes on proposals without a header:
        // - f+1 on one of them, or
        // - each <= f
        None
    }

    fn missing_headers(&self, j: Lane) -> MissingHeaders {
        let avail = &self.availability[j];
        let tally = self.votes[j].tally(&self.validator_data);
        let mut missing = MissingHeaders::new();
        for (entry, sigcol) in tally.groups() {
            let Entry::Positive(pid) = entry else {
                continue;
            };
            if avail.header_for(pid).is_some() {
                continue;
            }
            let signers = sigcol
                .signers(&self.validator_data)
                .expect("a tally's collections match the validator set");
            missing.push((*pid, signers.into_iter().copied().collect()));
        }
        // heuristic: the best-backed proposal first
        missing.sort_by_cached_key(|(_, voters)| Reverse(self.validator_data.sum_stake(voters)));
        missing
    }

    fn try_form_fallback_qc(&mut self, j: Lane) {
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
            (Some(qc1), Some(qc2)) => match (&qc1.verdict.0, &qc2.verdict.0) {
                (Entry::Positive { .. }, _) => qc1,
                (_, Entry::Positive { .. }) => qc2,
                _ => unreachable!("two distinct fallback weak qcs must include a positive"),
            },
        };

        self.certs[j].try_upgrade(weak_qc);
    }

    fn try_cast_fast_commit_vote(&mut self) -> Option<FastBlock> {
        if matches!(self.phase, Phase::FastCommitVote | Phase::FallbackVote) {
            // voted for commit or fallback, no-op
            return None;
        }

        let fast_block = self.try_form_fast_block()?;
        self.phase = Phase::FastCommitVote;
        Some(fast_block)
    }

    fn try_form_fast_block(&self) -> Option<FastBlock> {
        self.certs
            .as_ref()
            .map(LocalCertifiedEntry::fast_qc)
            .try_into_total()
            .map(Lanes::into_owned)
            .map(FastBlock)
    }

    fn try_form_fast_qc(&mut self, j: Lane) {
        if self.certs[j].fast_qc().is_some() {
            // already have a strong qc, no need to try forming again.
            return;
        }

        let fast_qc = self.votes[j]
            .tally(&self.validator_data)
            .narrow(HeaderSeen(&self.availability[j]))
            .strong_qc();
        if let Some(fast_qc) = fast_qc {
            self.certs[j].try_upgrade(fast_qc);
        }
    }
}

// a proposal's decided state at fallback transition
enum ProposalState {
    // fast qc, fallback qc, or equiv cert
    Certified(Box<CertifiedEntry>),

    // f+1 negative, or the proposal is invalid
    FallbackNegative,

    // f+1 positive votes, decoded
    FallbackPositive(SignedProposalHeader),
}

impl ProposalState {
    fn into_evidence(self, scope: ProposalScope, key: &KeyPair) -> ProposalEvidence {
        match self {
            ProposalState::Certified(cert) => ProposalEvidence::Certified(*cert),
            ProposalState::FallbackNegative => {
                let fse = FallbackSignedEntry::new_signed_negative(scope, key);
                ProposalEvidence::FallbackSignedEntry(fse)
            }
            ProposalState::FallbackPositive(header) => {
                let fse = FallbackSignedEntry::new_signed_positive(scope, header, key);
                ProposalEvidence::FallbackSignedEntry(fse)
            }
        }
    }
}

/// The verdict on one lane: `Positive` carries the pid of
/// the included proposal, `Negative` finalizes the lane empty. Part of the
/// finalization data — downstream consumers (ledger sequencing) read it.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Entry {
    Positive(ProposalId),
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

// admits a positive vote once its proposal's header is seen
struct HeaderSeen<'a>(&'a ProposalAvailability);

impl Gate<Entry> for HeaderSeen<'_> {
    fn admits(&self, vote: &Entry) -> bool {
        let Entry::Positive(pid) = vote else {
            return true;
        };
        self.0.header_for(pid).is_some()
    }
}

pub type FastQc = StrongQc<Entry>;

#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
pub(crate) struct BatchVoteMsg {
    slot: Slot,
    votes: Lanes<SignedEntry>,
    // vote only. fields for chunks & decryption share may be added by
    // other components.
}

impl BatchVoteMsg {
    // one char per lane, + positive / - negative, for logs
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
    pub entries: Lanes<Entry>,
}

pub(crate) type FastCommitVoteMsg = VoteMsg<FastCommitVote>;

#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct FastBlock(Lanes<FastQc>);

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

impl FallbackQc {
    // a positive signer decoded the proposal
    fn decoded_holders(&self, validator_data: &ValidatorData) -> Option<(ProposalId, Vec<NodeId>)> {
        let Entry::Positive(pid) = self.verdict.0 else {
            return None;
        };
        let signers = self.sigcol.signers(validator_data)?;
        Some((pid, signers.into_iter().copied().collect()))
    }
}

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

    // the proposal and the nodes known to have decoded it
    pub(crate) fn decoded_holders(
        &self,
        validator_data: &ValidatorData,
    ) -> Option<(ProposalId, Vec<NodeId>)> {
        let CertifiedEntry::FallbackQc(qc) = self else {
            return None;
        };
        qc.decoded_holders(validator_data)
    }

    /// Whether this certificate is bound to `scope`, is well-formed and
    /// carries valid signatures. Authenticity is enforced at message ingress
    /// (see the crate header); we restate it at adoption points to make the
    /// trust boundary explicit and catch protocol-logic bugs.
    pub(crate) fn verify(
        &self,
        scope: ProposalScope,
        header_auth: &HeaderAuth,
        validator_data: &ValidatorData,
    ) -> bool {
        match self {
            CertifiedEntry::FastQc(qc) => qc.scope == scope && qc.verify(validator_data),
            CertifiedEntry::FallbackQc(qc) => qc.scope == scope && qc.verify(validator_data),
            CertifiedEntry::EquivCert(EquivCert(a, b)) => {
                let ProposalScope { slot: s, lane: j } = scope;
                let a_authentic = header_auth.validate(a, s.get(), j);
                let b_authentic = header_auth.validate(b, s.get(), j);
                a_authentic && b_authentic
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable)]
#[rlp(trailing)]
struct FallbackSignedEntry {
    entry: FallbackEntry,
    // over (slot, j, self.entry)
    signature: Signature,
    // invariant: header.is_some() iff entry is positive
    // invariant: header.pid() == entry's pid
    header: Option<SignedProposalHeader>,
}

impl Decodable for FallbackSignedEntry {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let entry = FallbackEntry::decode(&mut payload)?;
        let signature = Signature::decode(&mut payload)?;
        let header = match payload.is_empty() {
            true => None,
            false => Some(SignedProposalHeader::decode(&mut payload)?),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        let signed = Self {
            entry,
            signature,
            header,
        };
        if !signed.is_well_formed() {
            return Err(alloy_rlp::Error::Custom(
                "fallback entry and header disagree",
            ));
        }
        Ok(signed)
    }
}

impl FallbackSignedEntry {
    fn new_signed_positive(
        scope: ProposalScope,
        header: SignedProposalHeader,
        key: &KeyPair,
    ) -> Self {
        let entry = FallbackEntry(Entry::Positive(header.pid()));
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

    fn is_well_formed(&self) -> bool {
        match &self.entry.0 {
            Entry::Positive(pid) => self
                .header
                .as_ref()
                .is_some_and(|header| header.pid() == *pid),
            Entry::Negative => self.header.is_none(),
        }
    }

    fn header(&self) -> Option<&SignedProposalHeader> {
        self.header.as_ref()
    }

    fn into_vote_msg(self, slot: Slot, j: Lane) -> VoteMsg<FallbackEntry> {
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

    fn try_upgrade(&mut self, new_ev: impl Into<CertifiedEntry>) {
        let new_ev = new_ev.into();

        match self {
            LocalCertifiedEntry::Absent => *self = LocalCertifiedEntry::Certified(new_ev),
            LocalCertifiedEntry::Certified(ev) if new_ev.strength() > ev.strength() => *ev = new_ev,
            _ => {}
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
    evidences: Lanes<ProposalEvidence>,
}

// A fallback cert certifies 2f+1 validators agree to enter fallback path
pub type EnterFallbackCert = StrongQc<EnterFallbackVote>;

impl Encodable for Entry {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            Self::Positive(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Negative => {
                let fields: [&dyn Encodable; 1] = [&2u8];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::Positive(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
            Self::Negative => {
                let fields: [&dyn Encodable; 1] = [&2u8];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
        }
    }
}

impl Decodable for Entry {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let result = match <u8 as Decodable>::decode(&mut payload)? {
            1 => Self::Positive(<ProposalId as Decodable>::decode(&mut payload)?),
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
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::EquivCert(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::FallbackQc(message) => {
                let fields: [&dyn Encodable; 2] = [&3u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::FastQc(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
            Self::EquivCert(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
            Self::FallbackQc(message) => {
                let fields: [&dyn Encodable; 2] = [&3u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
        }
    }
}

impl Decodable for CertifiedEntry {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
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
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::FallbackSignedEntry(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::Certified(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
            Self::FallbackSignedEntry(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
        }
    }
}

impl Decodable for ProposalEvidence {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
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
            entries: <Lanes<Entry> as Decodable>::decode(buf)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use monad_mcp_common::spec::KeyPair as _;

    use super::{
        super::{
            super::proposers,
            chorus::ProposalDAEvent,
            types::{
                FixedProposerSchedule, MAX_PROPOSER_SET_SIZE, ProposerSchedule as _, Stake,
                ValidatorData,
            },
        },
        *,
    };
    use crate::env::stub::{
        EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader, ProposalSignature,
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

    fn pid(byte: u8) -> ProposalId {
        header(byte).pid()
    }

    // signed by validator 0, the only proposer
    fn header(byte: u8) -> SignedProposalHeader {
        SignedProposalHeader {
            header: ProposalHeader {
                root: root(byte),
                scheme: EncodingScheme::dummy(crate::stub::types::Slot(SLOT.get())),
            },
            sig: ProposalSignature {
                signer: NodeId::dummy(0),
                checksum: 0,
            },
        }
    }

    // the local node is validator 1 among 4, one proposal per slot
    fn fast_path() -> FastPath {
        fast_path_of(vec![NodeId::dummy(0)])
    }

    // the local node is validator 1 among 4, one lane per proposer
    fn fast_path_of(proposers: Vec<NodeId>) -> FastPath {
        // consensus and header authentication read the one schedule, so
        // they cannot disagree
        let schedule = Arc::new(FixedProposerSchedule::new(proposers));
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

    // the pins among the drained commands, for proposal 0
    fn drain_commands(fast: &mut FastPath) -> Vec<ChorusDACommand> {
        let mut commands = Vec::new();
        while let Some(command) = fast.next_da_command() {
            commands.push(command);
        }
        commands
    }

    // a fast qc on header(byte) at `scope`, signed by validators 0, 2 and 3
    fn fast_qc(scope: ProposalScope, byte: u8) -> FastQc {
        let mut pool = VotePool::new(scope);
        for index in [0, 2, 3] {
            let voter = NodeId::dummy(index);
            let msg = VoteMsg::new_signed(scope, Entry::Positive(pid(byte)), &voter.keypair());
            pool.add_vote(voter, msg);
        }
        pool.tally(&validator_data(4))
            .strong_qc()
            .expect("three of four votes form a fast qc")
    }

    fn fast_block(byte: u8) -> FastBlock {
        let qc = fast_qc(ProposalScope::new(SLOT, Lane::new_unchecked(0)), byte);
        FastBlock(Lanes::new(1, |_| qc.clone()))
    }

    // a fast commit qc on `entries`, signed by validators 0, 2 and 3
    fn fast_commit_qc(entries: Lanes<Entry>) -> FastCommitQc {
        let vote = FastCommitVote { entries };
        let mut pool = VotePool::new(SLOT);
        for index in [0, 2, 3] {
            let voter = NodeId::dummy(index);
            let msg = VoteMsg::new_signed(SLOT, vote.clone(), &voter.keypair());
            pool.add_vote(voter, msg);
        }
        pool.tally(&validator_data(4))
            .strong_qc()
            .expect("three of four votes form a fast commit qc")
    }

    #[test]
    fn a_fast_block_with_extra_lanes_is_rejected() {
        let mut fast = fast_path();
        let qc = fast_qc(ProposalScope::new(SLOT, Lane::new_unchecked(0)), 1);
        let padded = FastBlock(Lanes::new(MAX_PROPOSER_SET_SIZE, |_| qc.clone()));

        assert!(fast.handle_fast_block(padded).is_none());
        assert!(fast.certs[Lane::new_unchecked(0)].fast_qc().is_none());
    }

    #[test]
    fn a_fast_block_with_a_qc_from_another_slot_is_rejected() {
        let mut fast = fast_path();
        let other_slot = Slot(SLOT.get() + 1);
        let qc = fast_qc(ProposalScope::new(other_slot, Lane::new_unchecked(0)), 1);
        let replayed = FastBlock(Lanes::new(1, |_| qc.clone()));

        assert!(fast.handle_fast_block(replayed).is_none());
        assert!(fast.certs[Lane::new_unchecked(0)].fast_qc().is_none());
    }

    #[test]
    fn a_fast_block_with_its_qcs_on_swapped_lanes_is_rejected() {
        let mut fast = fast_path_of(vec![NodeId::dummy(0), NodeId::dummy(2)]);
        let lane_0 = fast_qc(ProposalScope::new(SLOT, Lane::new_unchecked(0)), 1);
        let lane_1 = fast_qc(ProposalScope::new(SLOT, Lane::new_unchecked(1)), 2);
        let swapped = FastBlock(Lanes::new(2, |j| match usize::from(j) {
            0 => lane_1.clone(),
            _ => lane_0.clone(),
        }));
        assert!(fast.handle_fast_block(swapped).is_none());

        let ordered = FastBlock(Lanes::new(2, |j| match usize::from(j) {
            0 => lane_0.clone(),
            _ => lane_1.clone(),
        }));
        assert!(fast.handle_fast_block(ordered).is_some());
    }

    #[test]
    fn a_certified_entry_verifies_only_at_its_own_lane() {
        let validator_data = validator_data(4);
        let header_auth = HeaderAuth::new(|_, _| None);
        let lane_0 = fast_qc(ProposalScope::new(SLOT, Lane::new_unchecked(0)), 1);
        let cert = CertifiedEntry::FastQc(lane_0);

        let verify_at = |slot, lane| {
            let scope = ProposalScope::new(slot, Lane::new_unchecked(lane));
            cert.verify(scope, &header_auth, &validator_data)
        };
        assert!(verify_at(SLOT, 0));
        assert!(!verify_at(SLOT, 1));
        assert!(!verify_at(Slot(SLOT.get() + 1), 0));
    }

    #[test]
    fn a_fast_commit_vote_with_extra_lanes_is_not_counted() {
        let mut fast = fast_path();
        let padded = FastCommitVote {
            entries: Lanes::new(2, |_| Entry::Negative),
        };
        for index in [0, 2, 3] {
            let voter = NodeId::dummy(index);
            let msg = VoteMsg::new_signed(SLOT, padded.clone(), &voter.keypair());
            assert!(fast.handle_commit_vote(voter, msg).is_none());
        }
    }

    #[test]
    fn a_fast_commit_qc_is_valid_only_with_one_entry_per_lane() {
        let fast = fast_path();
        let exact = fast_commit_qc(Lanes::new(1, |_| Entry::Negative));
        let padded = fast_commit_qc(Lanes::new(2, |_| Entry::Negative));

        assert!(fast.fast_commit_qc_is_valid(&exact));
        assert!(!fast.fast_commit_qc_is_valid(&padded));
    }

    #[test]
    fn a_fallback_entry_decodes_only_if_it_agrees_with_its_header() {
        let key = NodeId::dummy(0).keypair();
        let scope = ProposalScope::new(SLOT, Lane::new_unchecked(0));
        let positive = FallbackSignedEntry::new_signed_positive(scope, header(1), &key);
        let negative = FallbackSignedEntry::new_signed_negative(scope, &key);
        let decode = |signed: &FallbackSignedEntry| {
            alloy_rlp::decode_exact::<FallbackSignedEntry>(alloy_rlp::encode(signed)).ok()
        };
        assert_eq!(decode(&positive), Some(positive.clone()));
        assert_eq!(decode(&negative), Some(negative.clone()));

        let headerless = FallbackSignedEntry {
            header: None,
            ..positive.clone()
        };
        let mismatched = FallbackSignedEntry {
            header: Some(header(2)),
            ..positive
        };
        let negative_with_header = FallbackSignedEntry {
            header: Some(header(1)),
            ..negative
        };
        for signed in [headerless, mismatched, negative_with_header] {
            assert_eq!(decode(&signed), None);
        }
    }

    #[test]
    fn an_equivocation_over_one_proposal_does_not_decode() {
        let decode =
            |cert: &EquivCert| alloy_rlp::decode_exact::<EquivCert>(alloy_rlp::encode(cert)).ok();
        let distinct = EquivCert(header(1), header(2));
        assert_eq!(decode(&distinct), Some(distinct.clone()));
        assert_eq!(decode(&EquivCert(header(1), header(1))), None);
    }

    #[test]
    fn a_fast_commit_vote_sends_no_da_command() {
        let mut fast = fast_path();

        assert!(fast.handle_fast_block(fast_block(1)).is_some());
        assert!(drain_commands(&mut fast).is_empty());
    }

    #[test]
    fn a_positive_fallback_qc_names_its_signers_as_decoded_holders() {
        let entry = FallbackEntry(Entry::Positive(pid(1)));
        let mut pool = VotePool::new(ProposalScope::new(SLOT, Lane::new_unchecked(0)));
        for id in [0, 2] {
            let voter = NodeId::dummy(id);
            let vote = VoteMsg::new_signed(
                ProposalScope::new(SLOT, Lane::new_unchecked(0)),
                entry.clone(),
                &voter.keypair(),
            );
            pool.add_vote(voter, vote);
        }
        let validator_data = validator_data(4);
        let qc = pool
            .tally(&validator_data)
            .weak_qcs()
            .into_iter()
            .next()
            .expect("two of four votes form a weak qc");
        let cert = CertifiedEntry::FallbackQc(qc);

        let (decoded_id, mut node_ids) = cert
            .decoded_holders(&validator_data)
            .expect("a positive fallback qc");
        node_ids.sort();
        assert_eq!(decoded_id, pid(1));
        assert_eq!(node_ids, [NodeId::dummy(0), NodeId::dummy(2)]);
    }

    #[test]
    fn deadline_releases_chunks_without_pinning() {
        let mut fast = fast_path();

        let _ = fast.handle_da_event(ChorusDAEvent {
            j: Lane::new_unchecked(0),
            event: ProposalDAEvent::HeaderSeen(header(1)),
        });
        let _ = fast.handle_da_event(ChorusDAEvent {
            j: Lane::new_unchecked(0),
            event: ProposalDAEvent::ProposerObligationFulfilled(pid(1)),
        });
        let _ = fast.on_deadline();

        let mut commands = Vec::new();
        while let Some(command) = fast.next_da_command() {
            commands.push(command);
        }
        assert_eq!(commands, vec![ChorusDACommand::ReleaseChunks]);
    }

    #[test]
    fn deadline_releases_chunks_after_an_early_fast_commit_vote() {
        let mut fast = fast_path();
        assert!(fast.handle_fast_block(fast_block(1)).is_some());

        // the fast commit vote replaced our batch vote, not our chunk serving
        assert!(fast.on_deadline().is_none());
        let mut commands = Vec::new();
        while let Some(command) = fast.next_da_command() {
            commands.push(command);
        }
        assert_eq!(commands, vec![ChorusDACommand::ReleaseChunks]);
    }

    fn batch_vote(voter: u64, entry: Entry) -> (NodeId, BatchVoteMsg) {
        let voter = NodeId::dummy(voter);
        let key = voter.keypair();
        let votes = Lanes::new(1, |j| {
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
        let _ = fast.handle_da_event(ChorusDAEvent {
            j: Lane::new_unchecked(0),
            event,
        });
    }

    #[test]
    fn a_blocked_transition_waits_on_an_unresolved_root_without_commands() {
        let mut fast = fast_path();
        let _ = fast.on_deadline();
        let votes = [
            batch_vote(0, Entry::Positive(pid(1))),
            batch_vote(3, Entry::Positive(pid(1))),
            batch_vote(2, Entry::Negative),
        ];
        for (voter, msg) in votes {
            let _ = fast.handle_batch_vote(voter, msg);
        }
        let _ = drain_commands(&mut fast);

        // without the header the votes do not count
        let outcome = fast.try_fallback_transition();
        assert!(matches!(outcome, FallbackTransitionOutcome::Waiting));
        let _ = drain_commands(&mut fast);

        // f+1 positive votes on an unresolved root: wait, DA pulls it by itself
        da_event(&mut fast, ProposalDAEvent::HeaderSeen(header(1)));
        for _ in 0..2 {
            let outcome = fast.try_fallback_transition();
            assert!(matches!(outcome, FallbackTransitionOutcome::Waiting));
            assert!(drain_commands(&mut fast).is_empty());
        }

        da_event(&mut fast, ProposalDAEvent::Decoded(pid(1)));
        let outcome = fast.try_fallback_transition();
        assert!(matches!(
            outcome,
            FallbackTransitionOutcome::FallbackVote(_)
        ));
        assert!(drain_commands(&mut fast).is_empty());
    }

    #[test]
    fn a_missing_header_is_pulled_from_its_voters_on_every_check() {
        let mut fast = fast_path();
        let _ = fast.on_deadline();
        let votes = [
            batch_vote(0, Entry::Positive(pid(1))),
            batch_vote(3, Entry::Positive(pid(1))),
            batch_vote(2, Entry::Negative),
        ];
        for (voter, msg) in votes {
            let _ = fast.handle_batch_vote(voter, msg);
        }
        let _ = drain_commands(&mut fast);

        let voters = BTreeSet::from([NodeId::dummy(0), NodeId::dummy(3)]);
        let pull = ChorusDACommand::PullHeader {
            j: Lane::new_unchecked(0),
            missing: MissingHeaders::from([(pid(1), voters)]),
        };
        for _ in 0..2 {
            let outcome = fast.try_fallback_transition();
            assert!(matches!(outcome, FallbackTransitionOutcome::Waiting));
            assert_eq!(drain_commands(&mut fast), vec![pull.clone()]);
        }

        da_event(&mut fast, ProposalDAEvent::HeaderSeen(header(1)));
        let _ = fast.try_fallback_transition();
        assert!(drain_commands(&mut fast).is_empty());
    }

    #[test]
    fn missing_headers_come_best_backed_first() {
        let mut fast = fast_path();
        let _ = fast.on_deadline();
        let votes = [
            batch_vote(2, Entry::Positive(pid(2))),
            batch_vote(0, Entry::Positive(pid(1))),
            batch_vote(3, Entry::Positive(pid(1))),
        ];
        for (voter, msg) in votes {
            let _ = fast.handle_batch_vote(voter, msg);
        }
        let _ = drain_commands(&mut fast);

        let _ = fast.try_fallback_transition();
        let missing = vec![
            (pid(1), BTreeSet::from([NodeId::dummy(0), NodeId::dummy(3)])),
            (pid(2), BTreeSet::from([NodeId::dummy(2)])),
        ];
        let pull = ChorusDACommand::PullHeader {
            j: Lane::new_unchecked(0),
            missing,
        };
        assert_eq!(drain_commands(&mut fast), vec![pull]);
    }

    #[test]
    fn a_rival_header_is_pulled_while_the_first_root_is_unresolved() {
        let mut fast = fast_path();
        let _ = fast.on_deadline();
        let votes = [
            batch_vote(0, Entry::Positive(pid(1))),
            batch_vote(3, Entry::Positive(pid(1))),
            batch_vote(2, Entry::Positive(pid(2))),
        ];
        for (voter, msg) in votes {
            let _ = fast.handle_batch_vote(voter, msg);
        }
        da_event(&mut fast, ProposalDAEvent::HeaderSeen(header(1)));
        let _ = drain_commands(&mut fast);

        let outcome = fast.try_fallback_transition();
        assert!(matches!(outcome, FallbackTransitionOutcome::Waiting));
        let missing = vec![(pid(2), BTreeSet::from([NodeId::dummy(2)]))];
        let pull = ChorusDACommand::PullHeader {
            j: Lane::new_unchecked(0),
            missing,
        };
        assert_eq!(drain_commands(&mut fast), vec![pull]);
    }

    // a fallback vote whose entry is positive on `header`
    fn fallback_vote(voter: u64, header: SignedProposalHeader) -> (NodeId, FallbackVoteMsg) {
        let voter = NodeId::dummy(voter);
        let key = voter.keypair();
        let scope = ProposalScope::new(SLOT, Lane::new_unchecked(0));
        let entry = FallbackSignedEntry::new_signed_positive(scope, header, &key);
        let msg = FallbackVoteMsg {
            enter_fallback_vote: VoteMsg::new_signed(SLOT, EnterFallbackVote, &key),
            evidences: Lanes::new(1, |_| ProposalEvidence::FallbackSignedEntry(entry.clone())),
        };
        (voter, msg)
    }

    #[test]
    fn an_unseen_fallback_entry_header_goes_to_da() {
        let mut fast = fast_path();

        let (voter, msg) = fallback_vote(2, header(1));
        fast.handle_fallback_vote(voter, msg);
        let ingest = ChorusDACommand::IngestProposalHeader {
            sender: voter,
            header: header(1),
        };
        assert_eq!(drain_commands(&mut fast), vec![ingest]);
        assert!(
            fast.availability[Lane::new_unchecked(0)]
                .header_for(&pid(1))
                .is_none()
        );

        // once DA reports it back, the header is seen and not sent again
        da_event(&mut fast, ProposalDAEvent::HeaderSeen(header(1)));
        let (voter, msg) = fallback_vote(3, header(1));
        fast.handle_fallback_vote(voter, msg);
        assert!(drain_commands(&mut fast).is_empty());
    }

    #[test]
    fn a_positive_vote_counts_once_its_header_is_seen() {
        let mut fast = fast_path();
        for voter in [0, 2, 3] {
            let (voter, msg) = batch_vote(voter, Entry::Positive(pid(1)));
            let _ = fast.handle_batch_vote(voter, msg);
        }
        assert!(fast.certs[Lane::new_unchecked(0)].fast_qc().is_none());

        da_event(&mut fast, ProposalDAEvent::HeaderSeen(header(1)));
        assert!(fast.certs[Lane::new_unchecked(0)].fast_qc().is_some());
    }

    #[test]
    fn the_transition_waits_for_headers_then_for_resolution() {
        let mut fast = fast_path();
        let _ = fast.on_deadline();
        let votes = [
            batch_vote(0, Entry::Positive(pid(1))),
            batch_vote(3, Entry::Positive(pid(1))),
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

        da_event(&mut fast, ProposalDAEvent::Decoded(pid(1)));
        let FallbackTransitionOutcome::FallbackVote(msg) = fast.try_fallback_transition() else {
            panic!("the decoded root decides the entry");
        };
        let ProposalEvidence::FallbackSignedEntry(entry) = &msg.evidences[Lane::new_unchecked(0)]
        else {
            panic!("no certificate formed");
        };
        assert_eq!(entry.entry, FallbackEntry(Entry::Positive(pid(1))));
    }

    /// [`FallbackEntry`] wraps [`Entry`] transparently, so the two encode
    /// alike; only the domain keeps a fast-path vote from being read as a
    /// fallback vote on the same proposal.
    #[test]
    fn a_wrapped_entry_signs_under_its_own_domain() {
        let scope = ProposalScope::new(SLOT, Lane::new_unchecked(0));
        let entry = Entry::Positive(pid(1));
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
        let scope = ProposalScope::new(SLOT, Lane::new_unchecked(1));
        let entry = Entry::Positive(pid(2));

        let bytes = entry.signing_bytes(&scope);
        let mut payload = bytes
            .strip_prefix(EntryDomain::PREFIX)
            .expect("the domain prefix leads the signed bytes");
        let mut list =
            alloy_rlp::Header::decode_bytes(&mut payload, true).expect("a two-item list");

        assert_eq!(ProposalScope::decode(&mut list).unwrap(), scope);
        assert_eq!(Entry::decode(&mut list).unwrap(), entry);
        assert!(list.is_empty(), "nothing follows the vote");
        assert!(payload.is_empty(), "nothing follows the list");
    }

    /// A certificate is bound to the scope its votes were signed under: the
    /// same signatures relabelled with another lane verify as
    /// neither a strong nor a weak quorum.
    #[test]
    fn a_certificate_does_not_verify_under_a_foreign_scope() {
        let validators = validator_data(4);
        let scope = ProposalScope::new(SLOT, Lane::new_unchecked(0));
        let foreign = ProposalScope::new(SLOT, Lane::new_unchecked(1));

        let mut pool = VotePool::new(scope);
        for index in [0, 2, 3] {
            let voter = NodeId::dummy(index);
            let msg = VoteMsg::new_signed(scope, Entry::Positive(pid(1)), &voter.keypair());
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
    use monad_mcp_common::{SlotDeadline, spec::KeyPair as _};

    use super::{
        super::{
            super::{
                conductor::{MonadConductor, acs::median::MedianAcs},
                message::CadenceWireMsg,
                test_utils::{assert_roundtrip, assert_serialization_roundtrip},
            },
            chorus::{Chorus, ChorusMessage},
        },
        *,
    };
    use crate::env::stub::{
        D25, EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader, ProposalSignature,
    };

    #[test]
    fn chorus_variants_and_nested_evidence_roundtrip() {
        let slot = Slot(9);
        let key = NodeId::dummy(1).keypair();
        let root = MerkleRoot(MerkleHash([7; 20]));
        let scheme = EncodingScheme::dummy(slot);
        let positive = Entry::Positive(ProposalHeader { root, scheme }.pid());
        let signature = key.sign(&Bytes::from_static(b"test"));
        // Empty collections suffice for wire tests; validation remains separate.
        let sigcol = alloy_rlp::decode_exact([0xc0]).unwrap();
        let fast_qc = StrongQc {
            scope: ProposalScope::new(slot, Lane::new_unchecked(0)),
            verdict: positive.clone(),
            sigcol,
        };
        let weak_qc = WeakQc {
            scope: ProposalScope::new(slot, Lane::new_unchecked(0)),
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
                ProposalScope::new(slot, Lane::new_unchecked(0)),
                h,
                &key,
            )),
            ProposalEvidence::FallbackSignedEntry(FallbackSignedEntry::new_signed_negative(
                ProposalScope::new(slot, Lane::new_unchecked(1)),
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
            entries: Lanes::new(2, |i| {
                if i == Lane::new_unchecked(0) {
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
                votes: Lanes::new(2, |i| SignedEntry {
                    entry: if i == Lane::new_unchecked(0) {
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
            ChorusMessage::FastBlock(FastBlock(Lanes::new(2, |_| fast_qc.clone()))),
            ChorusMessage::FallbackVote(FallbackVoteMsg {
                enter_fallback_vote: VoteMsg::new_signed(slot, EnterFallbackVote, &key),
                evidences: Lanes::new(evidence.len(), |i| evidence[usize::from(i)].clone()),
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
        let scope = ProposalScope::new(Slot(7), Lane::new_unchecked(3));
        assert_eq!(encode_scope::<Entry>(&scope), [0xc2, 7, 3]);
        assert_roundtrip(&scope);
        let header = ProposalHeader {
            root: MerkleRoot(MerkleHash([7; 20])),
            scheme: EncodingScheme::dummy(Slot(7)),
        };
        let pid = header.pid();
        assert_eq!(alloy_rlp::encode(pid), alloy_rlp::encode(pid.as_bytes()));
        let entry = Entry::Negative;
        assert_eq!(alloy_rlp::encode(&entry), [0xc1, 2]);
        assert_eq!(alloy_rlp::encode(FallbackEntry(entry)), [0xc1, 2]);
        assert_eq!(alloy_rlp::encode(EnterFallbackVote), [0xc0]);
        assert_roundtrip(&EnterFallbackVote);
        let entries = Lanes::new(0, |_| Entry::Negative);
        assert_eq!(alloy_rlp::encode(FastCommitVote { entries }), [0xc0]);
        let block = FastBlock(Lanes::new(0, |_| unreachable!()));
        assert_eq!(alloy_rlp::encode(&block), [0xc0]);
        assert_roundtrip(&block);
        for bad in [&[0xc1, 3][..], &[0xc2, 2, 0][..], &[0x80][..]] {
            assert!(alloy_rlp::decode_exact::<Entry>(bad).is_err());
        }
    }
}
