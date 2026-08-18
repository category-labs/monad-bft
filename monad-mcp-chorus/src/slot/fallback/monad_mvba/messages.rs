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
// `ProposalMap<Entry>` and pre-prepares carry `MVBAInputs` directly, so the
// messages, the certificates over them, the collectors and the phases would all
// have to become generic in `V` together, and `MonadMvba` would need the vote
// newtypes it signs under to come from the value type rather than be declared
// here. That is a module-wide parameterisation rather than a local edit, so it
// is left for you to schedule -- say the word and it is a self-contained pass.

use bytes::Bytes;

use super::{
    super::{
        super::{
            fast::Entry,
            types::{
                IsVote, KeyPair, ProposalMap, PubKey, Signature, Slot, ValidatorData, VoteMsg,
                dummy_serialize,
            },
        },
        FallbackView, MVBAInputs,
    },
    certificates::{CommitQc, PrepareQc, TimeoutCertificate},
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
    /// A vote to commit entries that gathered a prepare certificate, with the
    /// metablock those entries come from.
    #[from]
    Commit(CommitMsg),
    /// Give up on the view, carrying the sender's highest prepare certificate.
    #[from]
    Timeout(TimeoutMsg),
    /// A commit certificate and the metablock it decides, so a validator that
    /// missed the votes it aggregates can still decide.
    #[from]
    CommitQc(CommitQcMsg),
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
pub(crate) struct CommitVote(pub ProposalMap<Entry>);

impl IsVote for CommitVote {
    type Scope = (Slot, FallbackView);

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

pub(crate) type CommitVoteMsg = VoteMsg<CommitVote>;

/// A commit vote together with the metablock it commits.
///
/// The block rides unsigned, as it does on a timeout: the sender signs only
/// `⟨Commit, slot, v, entries(x)⟩`, so commit votes still aggregate into a
/// single certificate. What authenticates the block is its own certificates
/// plus the requirement that its entries are exactly the ones voted for.
///
/// Every commit vote carries it so that a validator which never saw the view's
/// pre-prepare can still decide the moment the certificate forms. The paper
/// answers that case by fetching the block from a signer; this implementation
/// has no fetch protocol, so the block travels with the messages that are sent
/// anyway. It is the largest bandwidth item in the protocol -- one metablock
/// per commit vote, so n per view.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct CommitMsg {
    pub vote: CommitVoteMsg,
    pub block: MVBAInputs,
}

impl CommitMsg {
    pub(crate) fn new_signed(
        slot: Slot,
        view: FallbackView,
        block: MVBAInputs,
        key: &KeyPair,
    ) -> Self {
        let vote = VoteMsg::new_signed((slot, view), CommitVote(block.entries()), key);

        Self { vote, block }
    }

    pub(crate) fn scope(&self) -> (Slot, FallbackView) {
        self.vote.scope
    }

    /// Whether the carried block is the one voted for. A sender that pairs an
    /// unrelated metablock with its vote is discarded rather than trusted.
    pub(crate) fn is_valid(&self) -> bool {
        self.block.entries() == self.vote.vote.0
    }
}

/// A commit certificate and the metablock it decides.
///
/// Same arrangement as [`CommitMsg`]: the certificate is over `entries(x)`
/// alone and the block rides alongside, so a receiver holding neither the
/// proposal nor the votes can decide from this message by itself.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct CommitQcMsg {
    pub qc: CommitQc,
    pub block: MVBAInputs,
}

impl CommitQcMsg {
    pub(crate) fn is_valid(&self) -> bool {
        self.block.entries() == self.qc.verdict.0
    }
}

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
/// The timeout carries no free proposal: the value it forwards is read out of
/// the prepare certificate, so a faulty validator cannot pair an unrelated
/// value with a certificate. The certificate rides along unsigned by the
/// timeout, authenticated by its own aggregated signatures.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct TimeoutMsg {
    pub vote: VoteMsg<TimeoutVote>,
    // FIXME: these should be coupled too, similar to that in TC
    pub high_prepare_qc: Option<PrepareQc>,
    /// The metablock the carried certificate locks, so a later leader can
    /// propose it without fetching it from a signer.
    pub high_block: Option<MVBAInputs>,
}

impl TimeoutMsg {
    pub(crate) fn new_signed(
        slot: Slot,
        view: FallbackView,
        high_prepare_qc: Option<PrepareQc>,
        high_block: Option<MVBAInputs>,
        key: &KeyPair,
    ) -> Self {
        let vote = TimeoutVote {
            high_prep_view: high_prepare_qc
                .as_ref()
                .map_or(FallbackView::GENESIS, |qc| qc.scope.1),
        };

        Self {
            vote: VoteMsg::new_signed((slot, view), vote, key),
            high_prepare_qc,
            high_block,
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
    /// its signatures verify. A carried block must match the certificate.
    ///
    /// Checking this at ingress is what lets a timeout certificate trust the
    /// claimed `high_prep_view` of each group it aggregates.
    pub(crate) fn is_valid(&self, validator_data: &ValidatorData) -> bool {
        let high_prep_view = self.vote.vote.high_prep_view;

        match &self.high_prepare_qc {
            // no certificate claimed and none carried.
            None => high_prep_view == FallbackView::GENESIS && self.high_block.is_none(),
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
                    && self
                        .high_block
                        .as_ref()
                        .is_none_or(|block| block.entries() == qc.verdict.0)
            }
        }
    }
}

/// `⟨Pre-Prepare, slot, v, x, J, σ_l⟩`: the leader's proposal for view `v`.
///
/// The metablock is carried in full rather than by hash: this implementation
/// has no block-fetch protocol, so a proposal is always self-contained. That
/// trades bandwidth for the absence of a fetch round-trip and is a deliberate
/// deviation from the paper, which leaves the fetch to the implementation.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct PrePrepareMsg {
    pub slot: Slot,
    pub view: FallbackView,
    pub metablock: MVBAInputs,
    /// `J`: `None` in view 1 -- the metablock carries its own fallback
    /// certificate -- and `TC_{slot, v-1}` in any later view.
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
        metablock: MVBAInputs,
        justification: Option<TimeoutCertificate>,
        key: &KeyPair,
    ) -> Self {
        let signature = key.sign(&signed_bytes(slot, view, &metablock, &justification));

        Self {
            slot,
            view,
            metablock,
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
fn signed_bytes(
    slot: Slot,
    view: FallbackView,
    metablock: &MVBAInputs,
    justification: &Option<TimeoutCertificate>,
) -> Bytes {
    dummy_serialize(
        &(metablock.entries(), justification),
        &(PrePrepareDomain, slot, view),
    )
}
