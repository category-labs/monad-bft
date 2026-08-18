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
    /// A vote to commit entries that gathered a prepare certificate.
    #[from]
    Commit(CommitVoteMsg),
    /// Give up on the view, carrying the sender's highest prepare certificate.
    #[from]
    Timeout(TimeoutMsg),
    /// A commit certificate, so a validator that missed the votes it
    /// aggregates can still learn the decision.
    #[from]
    CommitQc(CommitQc),
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

/// The signed part of a timeout: which prepare certificate the sender carries,
/// identified by its view alone.
///
/// Keeping the certificate itself out of the signed digest is what lets
/// timeouts aggregate: every validator holding the same lock signs identical
/// bytes, so a timeout certificate needs only one signature collection per
/// distinct `high_prep_view` instead of one signature per sender.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct TimeoutVote {
    /// View of the sender's highest prepare certificate, `None` if it holds
    /// none.
    pub high_prep_view: Option<FallbackView>,
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
            high_prep_view: high_prepare_qc.as_ref().map(|qc| qc.scope.1),
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
        match (&self.vote.vote.high_prep_view, &self.high_prepare_qc) {
            (None, None) => self.high_block.is_none(),
            (Some(high_prep_view), Some(qc)) => {
                let (qc_slot, qc_view) = qc.scope;
                qc_slot == self.slot()
                    && qc_view == *high_prep_view
                    // a validator may hold a prepare certificate of the very
                    // view it is abandoning, but never of a later one.
                    && qc_view <= self.view()
                    && qc.verify(validator_data)
                    && self
                        .high_block
                        .as_ref()
                        .is_none_or(|block| block.entries() == qc.verdict.0)
            }
            // a claim without its certificate, or a certificate not claimed.
            _ => false,
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
