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

use std::{fmt::Debug, hash::Hash};

use alloy_rlp::{
    Decodable, Encodable, Header, RlpDecodable, RlpDecodableWrapper, RlpEncodable,
    RlpEncodableWrapper, encode_list, list_length,
};
use bytes::Bytes;

use super::{
    super::{
        super::types::{
            IsVote, KeyPair, PubKey, Signature, Slot, ValidatorData, VoteMsg, dummy_serialize,
        },
        FallbackView, FromEntries, MvbaScope, ValidateCert, Votable,
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
    Prepare(PrepareVoteMsg<V::Entries>),
    #[from]
    Commit(CommitVoteMsg<V::Entries>),
    #[from]
    Timeout(TimeoutMsg<V::Entries>),
    /// So a validator that missed the votes it aggregates can still decide
    #[from]
    CommitQc(FallbackCommitQc<V::Entries>),
    #[from]
    BlockRequest(BlockRequestMsg<V::Entries>),
    #[from]
    BlockResponse(BlockResponseMsg<V>),
}

/// `⟨Prepare, slot, v, entries(x)⟩`
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
pub(crate) struct PrepareVote<E>(pub E);

impl<V: Votable> FromEntries<V> for PrepareVote<V::Entries> {
    fn from_entries(entries: V::Entries) -> Self {
        Self(entries)
    }
}

impl<E: Clone + Eq + Hash + Debug> IsVote for PrepareVote<E> {
    type Scope = MvbaScope;

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

pub(crate) type PrepareVoteMsg<E> = VoteMsg<PrepareVote<E>, MvbaScope>;

/// `⟨Commit, slot, v, entries(x)⟩`
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct FallbackCommitVote<E>(pub(crate) E);

impl<V: Votable> FromEntries<V> for FallbackCommitVote<V::Entries> {
    fn from_entries(entries: V::Entries) -> Self {
        Self(entries)
    }
}

impl<E: Clone + Eq + Hash + Debug> IsVote for FallbackCommitVote<E> {
    type Scope = MvbaScope;

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

pub(crate) type CommitVoteMsg<E> = VoteMsg<FallbackCommitVote<E>, MvbaScope>;

/// The signed part of a timeout: the *view* of the prepare certificate the
/// sender carries, not the certificate, so timeouts holding the same lock sign
/// identical bytes and aggregate
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct TimeoutVote {
    /// [`FallbackView::GENESIS`] when the sender holds none
    pub high_prep_view: FallbackView,
}

impl IsVote for TimeoutVote {
    type Scope = MvbaScope;

    fn serialize(&self, scope: &Self::Scope) -> Bytes {
        dummy_serialize(self, scope)
    }
}

/// `⟨Timeout, slot, v, PrepQC_i, σ_i⟩`
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub(crate) struct TimeoutMsg<E> {
    pub vote: VoteMsg<TimeoutVote, MvbaScope>,
    pub high_prep_qc: Option<PrepareQc<E>>,
}

impl<E: Clone + Eq + Hash + Debug> TimeoutMsg<E> {
    pub(crate) fn new_signed(
        slot: Slot,
        view: FallbackView,
        high_prep_qc: Option<PrepareQc<E>>,
        key: &KeyPair,
    ) -> Self {
        let vote = TimeoutVote {
            high_prep_view: high_prep_qc
                .as_ref()
                .map_or(FallbackView::GENESIS, |qc| qc.scope.view),
        };

        Self {
            vote: VoteMsg::new_signed(MvbaScope::new(slot, view), vote, key),
            high_prep_qc,
        }
    }

    pub(crate) fn slot(&self) -> Slot {
        self.vote.scope.slot
    }

    pub(crate) fn view(&self) -> FallbackView {
        self.vote.scope.view
    }

    /// Whether the claim in the signed digest is backed by what rides along
    pub(crate) fn is_valid(&self, validator_data: &ValidatorData) -> bool {
        let high_prep_view = self.vote.vote.high_prep_view;

        match &self.high_prep_qc {
            None => high_prep_view == FallbackView::GENESIS,
            Some(qc) => {
                let MvbaScope {
                    slot: qc_slot,
                    view: qc_view,
                } = qc.scope;
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
    Tc(TimeoutCertificate<V::Entries>),
}

impl<V: Votable, C: ValidateCert> Justification<V, C> {
    /// The part of `J` the leader's signature covers
    fn signed_part(&self) -> Option<&TimeoutCertificate<V::Entries>> {
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
        &MvbaScope::new(slot, view),
    )
}

impl<V: Votable, C: ValidateCert> Encodable for MvbaMessage<V, C>
where
    V: Encodable,
    V::Entries: Encodable,
    C: Encodable,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            Self::PrePrepare(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Prepare(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Commit(message) => {
                let fields: [&dyn Encodable; 2] = [&3u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Timeout(message) => {
                let fields: [&dyn Encodable; 2] = [&4u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::CommitQc(message) => {
                let fields: [&dyn Encodable; 2] = [&5u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::BlockRequest(message) => {
                let fields: [&dyn Encodable; 2] = [&6u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::BlockResponse(message) => {
                let fields: [&dyn Encodable; 2] = [&7u8, message];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::PrePrepare(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::Prepare(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::Commit(message) => {
                let fields: [&dyn Encodable; 2] = [&3u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::Timeout(message) => {
                let fields: [&dyn Encodable; 2] = [&4u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::CommitQc(message) => {
                let fields: [&dyn Encodable; 2] = [&5u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::BlockRequest(message) => {
                let fields: [&dyn Encodable; 2] = [&6u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
            Self::BlockResponse(message) => {
                let fields: [&dyn Encodable; 2] = [&7u8, message];
                list_length::<_, dyn Encodable>(&fields)
            }
        }
    }
}

impl<V: Votable, C: ValidateCert> Decodable for MvbaMessage<V, C>
where
    V: Decodable,
    V::Entries: Decodable,
    C: Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let result = match <u8 as Decodable>::decode(&mut payload)? {
            1 => Self::PrePrepare(<PrePrepareMsg<V, C> as Decodable>::decode(&mut payload)?),
            2 => Self::Prepare(<PrepareVoteMsg<V::Entries> as Decodable>::decode(
                &mut payload,
            )?),
            3 => Self::Commit(<CommitVoteMsg<V::Entries> as Decodable>::decode(
                &mut payload,
            )?),
            4 => Self::Timeout(<TimeoutMsg<V::Entries> as Decodable>::decode(&mut payload)?),
            5 => Self::CommitQc(<FallbackCommitQc<V::Entries> as Decodable>::decode(
                &mut payload,
            )?),
            6 => Self::BlockRequest(<BlockRequestMsg<V::Entries> as Decodable>::decode(
                &mut payload,
            )?),
            7 => Self::BlockResponse(<BlockResponseMsg<V> as Decodable>::decode(&mut payload)?),
            _ => return Err(alloy_rlp::Error::Custom("unknown MvbaMessage tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

// Alloy's derives add codec bounds on V and C, but Justification<V, C> also
// needs a codec for V::Entries. A codec bound on V does not imply one on its
// associated entries type, so these manual implementations supply that bound
// without requiring it on the struct definition.
impl<V: Votable, C: ValidateCert> Encodable for PrePrepareMsg<V, C>
where
    V: Encodable,
    V::Entries: Encodable,
    C: Encodable,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let fields: [&dyn Encodable; 5] = [
            &self.slot,
            &self.view,
            &self.value,
            &self.justification,
            &self.signature,
        ];
        encode_list::<_, dyn Encodable>(&fields, out);
    }

    fn length(&self) -> usize {
        let fields: [&dyn Encodable; 5] = [
            &self.slot,
            &self.view,
            &self.value,
            &self.justification,
            &self.signature,
        ];
        list_length::<_, dyn Encodable>(&fields)
    }
}

impl<V: Votable, C: ValidateCert> Decodable for PrePrepareMsg<V, C>
where
    V: Decodable,
    V::Entries: Decodable,
    C: Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let result = Self {
            slot: <Slot as Decodable>::decode(&mut payload)?,
            view: <FallbackView as Decodable>::decode(&mut payload)?,
            value: <V as Decodable>::decode(&mut payload)?,
            justification: <Justification<V, C> as Decodable>::decode(&mut payload)?,
            signature: <Signature as Decodable>::decode(&mut payload)?,
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

impl<V: Votable, C: ValidateCert + Encodable> Encodable for Justification<V, C>
where
    V::Entries: Encodable,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            Self::FallbackCert(None) => encode_list(&[1u8], out),
            Self::FallbackCert(Some(cert)) => {
                encode_list::<_, dyn Encodable>(&[&1u8 as &dyn Encodable, cert], out)
            }
            Self::Tc(tc) => encode_list::<_, dyn Encodable>(&[&2u8 as &dyn Encodable, tc], out),
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::FallbackCert(None) => list_length(&[1u8]),
            Self::FallbackCert(Some(cert)) => {
                list_length::<_, dyn Encodable>(&[&1u8 as &dyn Encodable, cert])
            }
            Self::Tc(tc) => list_length::<_, dyn Encodable>(&[&2u8 as &dyn Encodable, tc]),
        }
    }
}

impl<V: Votable, C: ValidateCert + Decodable> Decodable for Justification<V, C>
where
    V::Entries: Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let result = match u8::decode(&mut payload)? {
            1 => Self::FallbackCert(if payload.is_empty() {
                None
            } else {
                Some(C::decode(&mut payload)?)
            }),
            2 => Self::Tc(TimeoutCertificate::decode(&mut payload)?),
            _ => return Err(alloy_rlp::Error::Custom("unknown Justification tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

// Alloy 0.3.12's wrapper decoder derive only constructs tuple newtypes.
// Keep both wrapper codecs manual for this named-field struct.
impl Encodable for TimeoutVote {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        self.high_prep_view.encode(out);
    }

    fn length(&self) -> usize {
        self.high_prep_view.length()
    }
}

impl Decodable for TimeoutVote {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        Ok(Self {
            high_prep_view: <FallbackView as Decodable>::decode(buf)?,
        })
    }
}

#[cfg(test)]
mod rlp_tests {
    use super::{
        super::{
            super::{
                super::{
                    super::{
                        conductor::{MonadConductor, acs::median::MedianAcs},
                        driver::CadenceDriverMsg,
                        test_utils::{assert_roundtrip, assert_serialization_roundtrip},
                        types::SlotDeadline,
                    },
                    chorus,
                    types::ProposalMap,
                },
                EnterFallbackCert, Entry, Metablock,
            },
            test_helpers as h,
        },
        *,
    };
    type Wire = CadenceDriverMsg<chorus::Chorus, MonadConductor<MedianAcs<SlotDeadline>>>;

    #[test]
    fn every_mvba_variant_and_optional_certificate_roundtrips() {
        type M = MvbaMessage<Metablock, EnterFallbackCert>;
        let validators = h::validator_data();
        let block = h::mixed_evidence_metablock(7, &validators);
        let entries = block.entries();
        let key = h::nodes()[0].keypair();
        let qc = h::prepare_qc(h::view(1), &entries, &validators);
        // Cover proposals with and without a fallback certificate, votes, and block exchange.
        let mut messages = vec![
            h::pre_prepare_with_cert(h::view(1), &block, None).1,
            h::pre_prepare_with_cert(
                h::view(1),
                &block,
                Some(h::enter_fallback_cert(&validators)),
            )
            .1,
            M::Prepare(VoteMsg::new_signed(
                MvbaScope::new(h::SLOT, h::view(1)),
                PrepareVote(entries.clone()),
                &key,
            )),
            M::Commit(VoteMsg::new_signed(
                MvbaScope::new(h::SLOT, h::view(1)),
                FallbackCommitVote(entries.clone()),
                &key,
            )),
            M::CommitQc(h::strong_qc(
                MvbaScope::new(h::SLOT, h::view(1)),
                FallbackCommitVote(entries.clone()),
                &h::quorum(),
                &validators,
            )),
            h::block_request(&entries),
            h::block_response(block.clone()),
        ];
        // Exercise absent/present prepare QCs in timeouts and timeout certificates.
        for lock in [None, Some(qc)] {
            let tc = h::timeout_certificate(h::view(1), lock.clone(), &validators);
            assert!(assert_roundtrip(&tc).verify(&validators));
            messages.push(h::pre_prepare(h::view(2), &block, Some(tc)).1);
            messages.push(M::Timeout(TimeoutMsg::new_signed(
                h::SLOT,
                h::view(1),
                lock,
                &key,
            )));
        }
        // Check every variant's round trip and that signed evidence still verifies.
        for message in messages {
            let decoded = assert_roundtrip(&message);
            match decoded {
                M::PrePrepare(p) => {
                    assert!(p.verify_signature(&h::leader_of(p.view).keypair().pubkey()))
                }
                M::Timeout(t) => assert!(t.is_valid(&validators)),
                M::CommitQc(qc) => assert!(qc.verify(&validators)),
                _ => {}
            }
            // The outer Chorus and Cadence framing also wraps the MVBA arm.
            assert_serialization_roundtrip(&Wire::Slot(
                h::SLOT,
                chorus::ChorusMessage::Fallback(message),
            ));
        }
    }

    #[test]
    fn mvba_wrappers_and_unknown_tags() {
        let scope = MvbaScope::new(Slot(7), h::view(1));
        assert_eq!(alloy_rlp::encode(scope), [0xc2, 7, 1]);
        assert_roundtrip(&scope);
        let entries = ProposalMap::new(0, |_| Entry::Negative);
        assert_eq!(
            alloy_rlp::encode(PrepareVote::<<Metablock as Votable>::Entries>(
                entries.clone()
            )),
            [0xc0]
        );
        assert_eq!(
            alloy_rlp::encode(FallbackCommitVote::<<Metablock as Votable>::Entries>(
                entries
            )),
            [0xc0]
        );
        assert_eq!(
            alloy_rlp::encode(TimeoutVote {
                high_prep_view: h::view(1)
            }),
            [1]
        );
        assert_eq!(
            alloy_rlp::encode(Justification::<Metablock, EnterFallbackCert>::FallbackCert(
                None
            )),
            [0xc1, 1]
        );
        assert!(
            alloy_rlp::decode_exact::<MvbaMessage<Metablock, EnterFallbackCert>>(&[0xc1, 8])
                .is_err()
        );
        assert!(
            alloy_rlp::decode_exact::<Justification<Metablock, EnterFallbackCert>>(&[0xc1, 3])
                .is_err()
        );
    }
}
