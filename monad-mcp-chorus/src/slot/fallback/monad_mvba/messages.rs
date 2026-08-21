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

//! Wire messages of the MVBA.
//!
//! Every vote is a distinct type, so each gets its own signing domain, and
//! every one is scoped by `(slot, view)`: no signature can be replayed across
//! views, nor across the fast and fallback paths.

// FIXME: mvba structs should be over generic MVBA input and not the concrete
// type
//
// Response: agreed, and the trait side is already generic -- `Mvba<V: Validate
// + Votable>` names the value type and `Votable::Entries` the projection votes
// range over. What is still concrete is this module: votes wrap
// `ProposalMap<Entry>` and pre-prepares carry a `Metablock` directly, so the
// messages, the certificates over them, the collectors and the phases would all
// have to become generic in `V` together, and `MonadMvba` would need the vote
// newtypes it signs under to come from the value type rather than be declared
// here. That is a module-wide parameterisation rather than a local edit, so it
// is left for you to schedule -- say the word and it is a self-contained pass.

use bytes::Bytes;

use super::{
    super::{
        super::{
            fast::{EnterFallbackCert, Entry},
            types::{
                IsVote, KeyPair, ProposalMap, PubKey, Signature, Slot, ValidatorData, VoteMsg,
                dummy_serialize,
            },
        },
        FallbackView, Metablock,
        block_sync::{BlockRequestMsg, BlockResponseMsg},
    },
    certificates::{FallbackCommitQc, PrepareQc, TimeoutCertificate},
    metablock::entries_of,
};
use crate::spec::vote::{KeyPair as _, Signature as _};

/// Every message of the fallback path's agreement protocol.
#[derive(Clone, PartialEq, Eq, Hash, Debug, derive_more::From)]
pub(crate) enum Message {
    /// The leader's proposal for the view.
    #[from]
    PrePrepare(PrePrepareMsg),
    /// A vote for the entries of the view's accepted proposal.
    #[from]
    Prepare(PrepareVoteMsg),
    /// A vote to commit entries that gathered a prepare certificate.
    #[from]
    Commit(CommitVoteMsg),
    /// Give up on the view, carrying the sender's highest prepare certificate.
    #[from]
    Timeout(TimeoutMsg),
    /// A commit certificate, so a validator that missed the votes it
    /// aggregates can still decide.
    #[from]
    CommitQc(FallbackCommitQc),
    /// Broadcast ask for the block behind entries a certificate has settled.
    #[from]
    BlockRequest(BlockRequestMsg),
    /// Unicast answer to a [`Message::BlockRequest`], carrying the block.
    #[from]
    BlockResponse(BlockResponseMsg),
}

/// `⟨Prepare, slot, v, entries(x)⟩`: a vote for the entries of the proposal
/// accepted in view `v`.
#[derive(Clone, PartialEq, Eq, Hash, Debug, derive_more::From)]
pub(crate) struct PrepareVote(pub ProposalMap<Entry>);

impl IsVote for PrepareVote {
    type Scope = (Slot, FallbackView);

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

pub(crate) type PrepareVoteMsg = VoteMsg<PrepareVote>;

/// `⟨Commit, slot, v, entries(x)⟩`: a vote to commit entries that gathered a
/// prepare certificate in view `v`.
#[derive(Clone, PartialEq, Eq, Hash, Debug, derive_more::From)]
pub(crate) struct FallbackCommitVote(pub ProposalMap<Entry>);

impl IsVote for FallbackCommitVote {
    type Scope = (Slot, FallbackView);

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

pub(crate) type CommitVoteMsg = VoteMsg<FallbackCommitVote>;

/// The signed part of a timeout: which prepare certificate the sender carries,
/// identified by its view alone.
///
/// Keeping the certificate itself out of the signed digest is what lets
/// timeouts aggregate: every validator holding the same lock signs identical
/// bytes, so a timeout certificate needs only one signature collection per
/// distinct `high_prep_view` instead of one signature per sender.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct TimeoutVote {
    /// View of the sender's highest prepare certificate, or    
    /// [`FallbackView::GENESIS`] if it holds none: views are 1-indexed, so view
    /// 0 is not a view any certificate can come from and carries the "no lock"
    /// case without a separate `None`.
    ///
    pub high_prep_view: FallbackView,
}

impl IsVote for TimeoutVote {
    type Scope = (Slot, FallbackView);

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

/// `⟨Timeout, slot, v, PrepQC_i, σ_i⟩`: no value was decided within the view
/// timeout.
///
/// The timeout carries no value at all, free or otherwise: only the prepare
/// certificate, which names the entries it locks. A leader bound by that lock
/// fetches the block behind it through block sync.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct TimeoutMsg {
    pub vote: VoteMsg<TimeoutVote>,
    pub high_prep_qc: Option<PrepareQc>,
}

impl TimeoutMsg {
    pub(crate) fn new_signed(
        slot: Slot,
        view: FallbackView,
        high_prep_qc: Option<PrepareQc>,
        key: &KeyPair,
    ) -> Self {
        let vote = TimeoutVote {
            high_prep_view: high_prep_qc
                .as_ref()
                .map_or(FallbackView::GENESIS, |qc| qc.scope.1),
        };

        Self {
            vote: VoteMsg::new_signed((slot, view), vote, key),
            high_prep_qc,
        }
    }

    pub(crate) fn slot(&self) -> Slot {
        self.vote.scope.0
    }

    pub(crate) fn view(&self) -> FallbackView {
        self.vote.scope.1
    }

    /// Whether the claim in the signed digest is backed by what rides along:
    /// the carried certificate is for this slot, is of exactly the claimed
    /// view, that view is not in the future of the view being abandoned, and
    /// its signatures verify.
    ///
    /// Checking this at ingress is what lets a timeout certificate trust the
    /// claimed `high_prep_view` of each group it aggregates.
    pub(crate) fn is_valid(&self, validator_data: &ValidatorData) -> bool {
        let high_prep_view = self.vote.vote.high_prep_view;

        match &self.high_prep_qc {
            // no certificate claimed and none carried.
            None => high_prep_view == FallbackView::GENESIS,
            Some(qc) => {
                let (qc_slot, qc_view) = qc.scope;
                qc_slot == self.slot()
                    // a certificate of view 0 does not exist, so this also
                    // rejects a certificate carried without being claimed.
                    && qc_view == high_prep_view
                    // a validator may hold a prepare certificate of the very
                    // view it is abandoning, but never of a later one.
                    && qc_view <= self.view()
                    && qc.verify(validator_data)
            }
        }
    }
}

/// `⟨Pre-Prepare, slot, v, x, J, σ_l⟩`: the leader's proposal for view `v`.
///
/// The metablock is carried in full rather than by hash, even though block
/// sync could fetch it: a validator must check a proposal valid before voting
/// to prepare it, so a hash would put a fetch round-trip in front of every
/// vote. Recovering a block whose entries a certificate has already settled is
/// a different problem, and the one block sync solves.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct PrePrepareMsg {
    pub slot: Slot,
    pub view: FallbackView,
    pub metablock: Metablock,
    /// The fallback certificate admitting the slot to the fallback path, which
    /// is what justifies a view-1 proposal. `None` in any later view -- those
    /// are justified by `justification` instead -- and, in view 1, also the
    /// paper's `fbcert = ⊥`. It is not part of the value and not signed over:
    /// the certificate carries its own signatures.
    pub fallback_cert: Option<EnterFallbackCert>,
    /// `J`: `None` in view 1 -- the proposal carries the fallback certificate
    /// instead -- and `TC_{slot, v-1}` in any later view.
    pub justification: Option<TimeoutCertificate>,
    /// The leader's signature over
    /// `⟨Pre-Prepare, slot, v, H(entries(x)), J⟩`.
    pub signature: Signature,
}

/// Domain tag separating pre-prepare signatures from every vote signature.
#[derive(Debug)]
struct PrePrepareDomain;

impl PrePrepareMsg {
    pub(crate) fn new_signed(
        slot: Slot,
        view: FallbackView,
        metablock: Metablock,
        fallback_cert: Option<EnterFallbackCert>,
        justification: Option<TimeoutCertificate>,
        key: &KeyPair,
    ) -> Self {
        let signature = key.sign(&signed_bytes(slot, view, &metablock, &justification));

        Self {
            slot,
            view,
            metablock,
            fallback_cert,
            justification,
            signature,
        }
    }

    /// Whether `σ_l` verifies under the given leader's key.
    pub(crate) fn verify_signature(&self, leader_pubkey: &PubKey) -> bool {
        let data = signed_bytes(self.slot, self.view, &self.metablock, &self.justification);
        self.signature.verify(&data, leader_pubkey)
    }
}

/// The bytes a pre-prepare's signature covers. `H(entries(x))` stands in as
/// the entries themselves under the crate-wide placeholder wire format; the
/// real format hashes them.
///
/// The fallback certificate is deliberately outside the signature: the paper
/// signs `⟨Pre-Prepare, slot, v, H(entries(x)), J⟩`, and a certificate is
/// self-certifying, so covering it would only bind the leader to one of the
/// several aggregates that all say the same thing.
fn signed_bytes(
    slot: Slot,
    view: FallbackView,
    metablock: &Metablock,
    justification: &Option<TimeoutCertificate>,
) -> Bytes {
    dummy_serialize(
        &(entries_of(metablock), justification),
        &(PrePrepareDomain, slot, view),
    )
}
