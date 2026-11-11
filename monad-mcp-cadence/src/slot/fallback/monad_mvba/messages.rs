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

use alloy_rlp::{Decodable, Encodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};
use bytes::Bytes;
use monad_mcp_common::{
    rlp::{RlpNone, RlpOption},
    spec::KeyPair as _,
};

use super::{
    super::{
        super::types::{IsVote, KeyPair, PubKey, Signature, Slot, ValidatorData, VoteMsg},
        FallbackView, MvbaScope, Votable,
    },
    block_store::{BlockRequestMsg, BlockResponseMsg},
    certificates::{FallbackCommitQc, PrepareQc, TimeoutCertificate},
};
use crate::spec::vote::{SigningDomain, assert_signing_prefix, signing_bytes_of};

#[derive(Clone, PartialEq, Eq, Hash, Debug, derive_more::From)]
pub enum MvbaMessage<V: Votable> {
    #[from]
    PrePrepare(PrePrepareMsg<V>),
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
    /// `TC_{slot, v}` on its own, so a view can be left without a proposal
    #[from]
    Tc(TimeoutCertificate<V::Entries>),
}

impl<V: Votable> MvbaMessage<V> {
    pub fn slot(&self) -> Slot {
        match self {
            Self::PrePrepare(msg) => msg.slot,
            Self::Prepare(msg) => msg.scope.slot,
            Self::Commit(msg) => msg.scope.slot,
            Self::Timeout(msg) => msg.slot(),
            Self::CommitQc(qc) => qc.scope.slot,
            Self::BlockRequest(msg) => msg.slot,
            Self::BlockResponse(msg) => msg.slot,
            Self::Tc(tc) => tc.slot,
        }
    }
}

/// `⟨Prepare, slot, v, entries(x)⟩`
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct PrepareVote<E>(pub E);

pub struct PrepareVoteDomain;
const _: () = assert_signing_prefix::<PrepareVoteDomain>();

impl SigningDomain for PrepareVoteDomain {
    const PREFIX: &'static [u8] = b"\x1Dmonad/cadence/mvba-prepare/1\n";
}

impl<E: Clone + Eq + Hash + Debug + Encodable> IsVote for PrepareVote<E> {
    type Scope = MvbaScope;
    type SigningDomain = PrepareVoteDomain;
}

pub(crate) type PrepareVoteMsg<E> = VoteMsg<PrepareVote<E>>;

/// `⟨Commit, slot, v, entries(x)⟩`
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct FallbackCommitVote<E>(pub(crate) E);

pub struct FallbackCommitVoteDomain;
const _: () = assert_signing_prefix::<FallbackCommitVoteDomain>();

impl SigningDomain for FallbackCommitVoteDomain {
    const PREFIX: &'static [u8] = b"\x1Cmonad/cadence/mvba-commit/1\n";
}

impl<E: Clone + Eq + Hash + Debug + Encodable> IsVote for FallbackCommitVote<E> {
    type Scope = MvbaScope;
    type SigningDomain = FallbackCommitVoteDomain;
}

pub(crate) type CommitVoteMsg<E> = VoteMsg<FallbackCommitVote<E>>;

/// The signed part of a timeout: the *view* of the prepare certificate the
/// sender carries, not the certificate, so timeouts holding the same lock sign
/// identical bytes and aggregate
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TimeoutVote {
    /// [`FallbackView::GENESIS`] when the sender holds none
    pub high_prep_view: FallbackView,
}

pub struct TimeoutVoteDomain;
const _: () = assert_signing_prefix::<TimeoutVoteDomain>();

impl SigningDomain for TimeoutVoteDomain {
    const PREFIX: &'static [u8] = b"\x1Dmonad/cadence/mvba-timeout/1\n";
}

impl IsVote for TimeoutVote {
    type Scope = MvbaScope;
    type SigningDomain = TimeoutVoteDomain;
}

/// `⟨Timeout, slot, v, PrepQC_i, σ_i⟩`
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable)]
#[rlp(trailing)]
pub struct TimeoutMsg<E: Clone + Eq + Hash + Debug + Encodable> {
    pub vote: VoteMsg<TimeoutVote>,
    pub high_prep_qc: Option<PrepareQc<E>>,
}

impl<E: Clone + Eq + Hash + Debug + Encodable> TimeoutMsg<E> {
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

    pub fn view(&self) -> FallbackView {
        self.vote.scope.view
    }

    /// Whether the claim in the signed digest is backed by what rides along
    pub(crate) fn is_well_formed(&self) -> bool {
        let high_prep_view = self.vote.vote.high_prep_view;
        let Some(qc) = &self.high_prep_qc else {
            return high_prep_view == FallbackView::GENESIS;
        };
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
    }

    /// Whether the certificate that rides along verifies
    pub(crate) fn is_valid(&self, validator_data: &ValidatorData) -> bool {
        self.high_prep_qc
            .as_ref()
            .is_none_or(|qc| qc.verify(validator_data))
    }
}

impl<E> Decodable for TimeoutMsg<E>
where
    E: Clone + Eq + Hash + Debug + Encodable + Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let vote = VoteMsg::<TimeoutVote>::decode(&mut payload)?;
        let high_prep_qc = match payload.is_empty() {
            true => None,
            false => Some(PrepareQc::<E>::decode(&mut payload)?),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        let msg = Self { vote, high_prep_qc };
        if !msg.is_well_formed() {
            return Err(alloy_rlp::Error::Custom(
                "timeout claim and certificate disagree",
            ));
        }
        Ok(msg)
    }
}

/// `⟨Pre-Prepare, slot, v, x, J, σ_l⟩`: the leader's proposal for view `v`
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PrePrepareMsg<V: Votable> {
    pub slot: Slot,
    pub view: FallbackView,
    pub value: V,
    /// `J`: `TC_{slot, v-1}`, absent exactly in view 1
    pub tc: Option<TimeoutCertificate<V::Entries>>,
    /// The leader's signature over `⟨Pre-Prepare, slot, v, H(entries(x)), J⟩`

    /// TODO: only the expensive aggregatable signature is available in cadence
    /// now. we need to replace this signature with the identity/integrity one
    pub signature: Signature,
}

impl<V: Votable> PrePrepareMsg<V> {
    pub fn new_signed(
        slot: Slot,
        view: FallbackView,
        value: V,
        tc: Option<TimeoutCertificate<V::Entries>>,
        key: &KeyPair,
    ) -> Self {
        let signature = key.sign(&signed_bytes(slot, view, &value, tc.as_ref()));

        Self {
            slot,
            view,
            value,
            tc,
            signature,
        }
    }

    pub(crate) fn verify_signature(&self, leader_pubkey: &PubKey) -> bool {
        use monad_mcp_common::spec::Signature as _;

        let data = signed_bytes(self.slot, self.view, &self.value, self.tc.as_ref());
        self.signature.verify(&data, leader_pubkey)
    }

    /// `J` fits the view, and the value keeps the lock `J` names, if any
    pub(crate) fn is_well_formed(&self) -> bool {
        let justified = match &self.tc {
            None => self.view == FallbackView::FIRST,
            Some(tc) => {
                self.view != FallbackView::FIRST
                    && tc.slot == self.slot
                    && tc.view.next() == self.view
            }
        };

        let keeps_lock = self
            .tc
            .as_ref()
            .and_then(|tc| tc.lock())
            .is_none_or(|lock| self.value.entries() == *lock);

        justified && keeps_lock
    }
}

struct PrePrepareDomain;
const _: () = assert_signing_prefix::<PrePrepareDomain>();

impl SigningDomain for PrePrepareDomain {
    const PREFIX: &'static [u8] = b"\x21monad/cadence/mvba-pre-prepare/1\n";
}

/// The value is covered by its entries alone, beside the timeout certificate;
/// whatever certificates the value carries are self-certifying
fn signed_bytes<V: Votable>(
    slot: Slot,
    view: FallbackView,
    value: &V,
    tc: Option<&TimeoutCertificate<V::Entries>>,
) -> Bytes {
    let tc: &dyn Encodable = match tc {
        Some(tc) => tc,
        None => &RlpNone,
    };
    signing_bytes_of::<PrePrepareDomain>(&[&slot, &view, &value.entries(), tc])
}

impl<V: Votable> Encodable for MvbaMessage<V>
where
    V: Encodable,
    V::Entries: Encodable,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match self {
            Self::PrePrepare(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Prepare(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Commit(message) => {
                let fields: [&dyn Encodable; 2] = [&3u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Timeout(message) => {
                let fields: [&dyn Encodable; 2] = [&4u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::CommitQc(message) => {
                let fields: [&dyn Encodable; 2] = [&5u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::BlockRequest(message) => {
                let fields: [&dyn Encodable; 2] = [&6u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::BlockResponse(message) => {
                let fields: [&dyn Encodable; 2] = [&7u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
            Self::Tc(message) => {
                let fields: [&dyn Encodable; 2] = [&8u8, message];
                alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
            }
        }
    }

    fn length(&self) -> usize {
        match self {
            Self::PrePrepare(message) => {
                let fields: [&dyn Encodable; 2] = [&1u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
            Self::Prepare(message) => {
                let fields: [&dyn Encodable; 2] = [&2u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
            Self::Commit(message) => {
                let fields: [&dyn Encodable; 2] = [&3u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
            Self::Timeout(message) => {
                let fields: [&dyn Encodable; 2] = [&4u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
            Self::CommitQc(message) => {
                let fields: [&dyn Encodable; 2] = [&5u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
            Self::BlockRequest(message) => {
                let fields: [&dyn Encodable; 2] = [&6u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
            Self::BlockResponse(message) => {
                let fields: [&dyn Encodable; 2] = [&7u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
            Self::Tc(message) => {
                let fields: [&dyn Encodable; 2] = [&8u8, message];
                alloy_rlp::list_length::<_, dyn Encodable>(&fields)
            }
        }
    }
}

impl<V: Votable> Decodable for MvbaMessage<V>
where
    V: Decodable,
    V::Entries: Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let result = match <u8 as Decodable>::decode(&mut payload)? {
            1 => Self::PrePrepare(<PrePrepareMsg<V> as Decodable>::decode(&mut payload)?),
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
            8 => Self::Tc(<TimeoutCertificate<V::Entries> as Decodable>::decode(
                &mut payload,
            )?),
            _ => return Err(alloy_rlp::Error::Custom("unknown MvbaMessage tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

// Alloy's derives add codec bounds on V, but the carried TC also needs a
// codec for V::Entries. A codec bound on V does not imply one on its
// associated entries type, so these manual implementations supply that bound
// without requiring it on the struct definition.
impl<V: Votable> Encodable for PrePrepareMsg<V>
where
    V: Encodable,
    V::Entries: Encodable,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let fields: [&dyn Encodable; 5] = [
            &self.slot,
            &self.view,
            &self.value,
            &RlpOption(self.tc.as_ref()),
            &self.signature,
        ];
        alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
    }

    fn length(&self) -> usize {
        let fields: [&dyn Encodable; 5] = [
            &self.slot,
            &self.view,
            &self.value,
            &RlpOption(self.tc.as_ref()),
            &self.signature,
        ];
        alloy_rlp::list_length::<_, dyn Encodable>(&fields)
    }
}

impl<V: Votable> Decodable for PrePrepareMsg<V>
where
    V: Decodable,
    V::Entries: Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let result = Self {
            slot: <Slot as Decodable>::decode(&mut payload)?,
            view: <FallbackView as Decodable>::decode(&mut payload)?,
            value: <V as Decodable>::decode(&mut payload)?,
            tc: RlpOption::decode(&mut payload)?.0,
            signature: <Signature as Decodable>::decode(&mut payload)?,
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        if !result.is_well_formed() {
            return Err(alloy_rlp::Error::Custom(
                "justification does not fit the view",
            ));
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
mod signing_tests {
    use super::{super::test_helpers as h, *};

    /// The leader's signature covers the timeout certificate that justifies
    /// its view, so another certificate -- valid, but naming a different lock
    /// -- does not carry it
    #[test]
    fn a_pre_prepare_signature_covers_its_justifying_certificate() {
        let validators = h::validator_data();
        let block = h::metablock(1, &validators);
        let lock = h::prepare_qc(h::view(1), &block.entries(), &validators);
        let locked = h::timeout_certificate(h::view(1), Some(lock), &validators);
        let unlocked = h::timeout_certificate(h::view(1), None, &validators);
        let leader = h::leader_of(h::view(2));

        let msg = h::PrePrepareMsg::new_signed(
            h::SLOT,
            h::view(2),
            block,
            Some(locked),
            &leader.keypair(),
        );
        assert!(msg.verify_signature(&leader.keypair().pubkey()));
        assert!(
            signed_bytes(msg.slot, msg.view, &msg.value, msg.tc.as_ref())
                .starts_with(PrePrepareDomain::PREFIX)
        );

        let swapped = h::PrePrepareMsg {
            tc: Some(unlocked),
            ..msg
        };
        assert!(!swapped.verify_signature(&leader.keypair().pubkey()));
    }
}

#[cfg(test)]
mod rlp_tests {
    use super::{
        super::{
            super::{
                super::{super::test_utils::assert_roundtrip, types::Lanes},
                Entry, Metablock,
            },
            test_helpers as h,
        },
        *,
    };

    #[test]
    fn every_mvba_variant_and_optional_certificate_roundtrips() {
        type M = MvbaMessage<Metablock>;
        let validators = h::validator_data();
        let block = h::mixed_evidence_metablock(7, &validators);
        let entries = block.entries();
        let key = h::nodes()[0].keypair();
        let qc = h::prepare_qc(h::view(1), &entries, &validators);
        let fast_block = h::metablock(7, &validators);
        // Cover view-1 proposals, votes, and block exchange.
        let mut messages = vec![
            h::pre_prepare(h::view(1), &fast_block, None).1,
            h::pre_prepare(h::view(1), &block, None).1,
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
            messages.push(h::pre_prepare(h::view(2), &block, Some(tc.clone())).1);
            messages.push(M::Tc(tc));
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
                M::Tc(tc) => assert!(tc.verify(&validators)),
                _ => {}
            }
        }
    }

    #[test]
    fn mvba_wrappers_and_unknown_tags() {
        let scope = MvbaScope::new(Slot(7), h::view(1));
        assert_eq!(alloy_rlp::encode(scope), [0xc2, 7, 1]);
        assert_roundtrip(&scope);
        let entries = Lanes::new(0, |_| Entry::Negative);
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
        assert!(alloy_rlp::decode_exact::<MvbaMessage<Metablock>>(&[0xc1, 9]).is_err());
    }
}
