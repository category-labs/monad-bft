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

//! Wire messages of the MVBA. Every vote is a distinct type, so each gets its
//! own signing domain, and every one is scoped by `(slot, view)`

use bytes::Bytes;

use super::{
    super::{
        super::types::{
            IsVote, KeyPair, PubKey, Signature, Slot, ValidatorData, VoteMsg, dummy_serialize,
        },
        FallbackView, FromEntries, ValidateCert, Votable,
    },
    block_store::{BlockRequestMsg, BlockResponseMsg},
    certificates::{FallbackCommitQc, PrepareQc, TimeoutCertificate},
};
use crate::spec::vote::{KeyPair as _, Signature as _};

#[derive(Clone, PartialEq, Eq, Hash, Debug, derive_more::From)]
pub enum MvbaMessage<V: Votable, C: ValidateCert> {
    #[from]
    PrePrepare(PrePrepareMsg<V, C>),
    #[from]
    Prepare(PrepareVoteMsg<V>),
    #[from]
    Commit(CommitVoteMsg<V>),
    #[from]
    Timeout(TimeoutMsg<V>),
    /// So a validator that missed the votes it aggregates can still decide
    #[from]
    CommitQc(FallbackCommitQc<V>),
    #[from]
    BlockRequest(BlockRequestMsg<V>),
    #[from]
    BlockResponse(BlockResponseMsg<V>),
}

/// `⟨Prepare, slot, v, entries(x)⟩`
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct PrepareVote<V: Votable>(pub V::Entries);

impl<V: Votable> FromEntries<V> for PrepareVote<V> {
    fn from_entries(entries: V::Entries) -> Self {
        Self(entries)
    }
}

impl<V: Votable> IsVote for PrepareVote<V> {
    type Scope = (Slot, FallbackView);

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

pub(crate) type PrepareVoteMsg<V> = VoteMsg<PrepareVote<V>>;

/// `⟨Commit, slot, v, entries(x)⟩`
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FallbackCommitVote<V: Votable>(pub(crate) V::Entries);

impl<V: Votable> FromEntries<V> for FallbackCommitVote<V> {
    fn from_entries(entries: V::Entries) -> Self {
        Self(entries)
    }
}

impl<V: Votable> IsVote for FallbackCommitVote<V> {
    type Scope = (Slot, FallbackView);

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

pub(crate) type CommitVoteMsg<V> = VoteMsg<FallbackCommitVote<V>>;

/// The signed part of a timeout: the *view* of the prepare certificate the
/// sender carries, not the certificate, so timeouts holding the same lock sign
/// identical bytes and aggregate
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct TimeoutVote {
    /// [`FallbackView::GENESIS`] when the sender holds none
    pub high_prep_view: FallbackView,
}

impl IsVote for TimeoutVote {
    type Scope = (Slot, FallbackView);

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

/// `⟨Timeout, slot, v, PrepQC_i, σ_i⟩`
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct TimeoutMsg<V: Votable> {
    pub vote: VoteMsg<TimeoutVote>,
    pub high_prep_qc: Option<PrepareQc<V>>,
}

impl<V: Votable> TimeoutMsg<V> {
    pub(crate) fn new_signed(
        slot: Slot,
        view: FallbackView,
        high_prep_qc: Option<PrepareQc<V>>,
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

    /// Whether the claim in the signed digest is backed by what rides along
    pub(crate) fn is_valid(&self, validator_data: &ValidatorData) -> bool {
        let high_prep_view = self.vote.vote.high_prep_view;

        match &self.high_prep_qc {
            None => high_prep_view == FallbackView::GENESIS,
            Some(qc) => {
                let (qc_slot, qc_view) = qc.scope;
                qc_slot == self.slot()
                    // view 0 has no certificate, so this also rejects one
                    // carried unclaimed
                    && qc_view == high_prep_view
                    // a validator may hold a certificate of the view it is
                    // abandoning, but never of a later one
                    && qc_view <= self.view()
                    && qc.verify(validator_data)
            }
        }
    }
}

/// `⟨Pre-Prepare, slot, v, x, J, σ_l⟩`: the leader's proposal for view `v`
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct PrePrepareMsg<V: Votable, C: ValidateCert> {
    pub slot: Slot,
    pub view: FallbackView,
    pub value: V,
    /// `J`: what justifies the proposal for its view
    pub justification: Justification<V, C>,
    /// The leader's signature over `⟨Pre-Prepare, slot, v, H(entries(x)), J⟩`
    pub signature: Signature,
}

/// `J`: view 1 is admitted by a fallback certificate (none if all entries are
/// FastQC), every later view by `TC_{slot, v-1}`
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Justification<V: Votable, C: ValidateCert> {
    FallbackCert(Option<C>),
    Tc(TimeoutCertificate<V>),
}

impl<V: Votable, C: ValidateCert> Justification<V, C> {
    /// The part of `J` the leader's signature covers
    fn signed_part(&self) -> Option<&TimeoutCertificate<V>> {
        match self {
            Justification::FallbackCert(_) => None,
            Justification::Tc(tc) => Some(tc),
        }
    }
}

impl<V: Votable, C: ValidateCert> PrePrepareMsg<V, C> {
    pub(crate) fn new_signed(
        slot: Slot,
        view: FallbackView,
        value: V,
        justification: Justification<V, C>,
        key: &KeyPair,
    ) -> Self {
        let signature = key.sign(&signed_bytes(slot, view, &value, &justification));

        Self {
            slot,
            view,
            value,
            justification,
            signature,
        }
    }

    pub(crate) fn verify_signature(&self, leader_pubkey: &PubKey) -> bool {
        let data = signed_bytes(self.slot, self.view, &self.value, &self.justification);
        self.signature.verify(&data, leader_pubkey)
    }
}

/// Only the timeout-certificate arm of `J` is covered; a fallback certificate
/// is self-certifying
fn signed_bytes<V: Votable, C: ValidateCert>(
    slot: Slot,
    view: FallbackView,
    value: &V,
    justification: &Justification<V, C>,
) -> Bytes {
    dummy_serialize(
        &(value.entries(), justification.signed_part()),
        &(slot, view),
    )
}
