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

//! The post-quantum signature seam. Signatures serve accountability only
//! (paper §3, Cryptography): the protocol itself never aggregates or
//! forwards them.

use alloy_rlp::{
    Decodable, Encodable, Header, RlpDecodableWrapper, RlpEncodableWrapper, encode_list,
    list_length,
};
use bytes::Bytes;
use monad_mcp_chorus::{
    spec::vote::{SigningDomain, assert_signing_prefix, signing_bytes_of},
    stub::types::{NodeId, Slot},
};
use tiny_keccak::{Hasher, Keccak};

mod ml_dsa;
pub use ml_dsa::{
    ML_DSA_44_PUBLIC_KEY_LEN, ML_DSA_44_SIGNATURE_LEN, ML_DSA_SEED_LEN, MlDsa44PublicKey,
    MlDsa44Signer, MlDsa44Verifier, dev_seed,
};

/// An opaque, variable-length signature.
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct PqSignature(pub Bytes);

pub trait PqSigner: Send + Sync {
    fn sign(&self, msg: &[u8]) -> PqSignature;
}

pub trait PqVerifier: Send + Sync {
    /// Keys are looked up by `signer`; signatures are not recoverable.
    fn verify(&self, signer: NodeId, msg: &[u8], sig: &PqSignature) -> bool;
    fn max_sig_len(&self) -> usize;
}

pub const STUB_SIGNATURE_LEN: usize = 32;

/// Stub scheme: `keccak256(signer || msg)` zero-padded to `len` bytes, so
/// `len` can emulate the sizes of real schemes (ML-DSA-44: 2420).
#[derive(Clone, Copy, Debug)]
pub struct StubPq {
    len: usize,
}

impl Default for StubPq {
    fn default() -> Self {
        Self::new(STUB_SIGNATURE_LEN)
    }
}

impl StubPq {
    /// `len` below the 32-byte digest truncates it.
    pub const fn new(len: usize) -> Self {
        Self { len }
    }

    pub fn signer(&self, id: NodeId) -> StubPqSigner {
        StubPqSigner { id, scheme: *self }
    }

    fn signature(&self, signer: NodeId, msg: &[u8]) -> PqSignature {
        let mut digest = [0u8; 32];
        let mut keccak = Keccak::v256();
        keccak.update(&u64::from(signer).to_be_bytes());
        keccak.update(msg);
        keccak.finalize(&mut digest);

        let mut sig = vec![0u8; self.len];
        let prefix = self.len.min(digest.len());
        sig[..prefix].copy_from_slice(&digest[..prefix]);
        PqSignature(Bytes::from(sig))
    }
}

impl PqVerifier for StubPq {
    fn verify(&self, signer: NodeId, msg: &[u8], sig: &PqSignature) -> bool {
        *sig == self.signature(signer, msg)
    }

    fn max_sig_len(&self) -> usize {
        self.len
    }
}

pub struct StubPqSigner {
    id: NodeId,
    scheme: StubPq,
}

impl PqSigner for StubPqSigner {
    fn sign(&self, msg: &[u8]) -> PqSignature {
        self.scheme.signature(self.id, msg)
    }
}

pub struct VoteDomain;
const _: () = assert_signing_prefix::<VoteDomain>();

impl SigningDomain for VoteDomain {
    const PREFIX: &'static [u8] = b"\x18monad/pq-cadence/vote/1\n";
}

pub struct PathVoteDomain;
const _: () = assert_signing_prefix::<PathVoteDomain>();

impl SigningDomain for PathVoteDomain {
    const PREFIX: &'static [u8] = b"\x1dmonad/pq-cadence/path-vote/1\n";
}

/// A message body signed as a whole under its own domain.
pub trait SignedBody: Encodable {
    type Domain: SigningDomain;
}

/// A body with its signer's signature over
/// `Domain::PREFIX ++ rlp([slot, signer, body])`. The signer is the
/// authenticated channel sender, which the signed bytes bind.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Signed<B> {
    pub body: B,
    pub sig: PqSignature,
}

impl<B: SignedBody> Signed<B> {
    pub fn signing_bytes(slot: Slot, signer: NodeId, body: &B) -> Bytes {
        signing_bytes_of::<B::Domain>(&[&slot, &signer, body])
    }

    pub fn sign(slot: Slot, signer: NodeId, body: B, key: &dyn PqSigner) -> Self {
        let sig = key.sign(&Self::signing_bytes(slot, signer, &body));
        Self { body, sig }
    }

    pub fn verify(&self, slot: Slot, signer: NodeId, verifier: &dyn PqVerifier) -> bool {
        self.sig.0.len() <= verifier.max_sig_len()
            && verifier.verify(
                signer,
                &Self::signing_bytes(slot, signer, &self.body),
                &self.sig,
            )
    }
}

impl<B: Encodable> Encodable for Signed<B> {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let fields: [&dyn Encodable; 2] = [&self.body, &self.sig];
        encode_list::<_, dyn Encodable>(&fields, out);
    }

    fn length(&self) -> usize {
        let fields: [&dyn Encodable; 2] = [&self.body, &self.sig];
        list_length::<_, dyn Encodable>(&fields)
    }
}

impl<B: Decodable> Decodable for Signed<B> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let body = B::decode(&mut payload)?;
        let sig = PqSignature::decode(&mut payload)?;
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(Self { body, sig })
    }
}

#[cfg(test)]
mod tests {
    use alloy_rlp::{RlpDecodable, RlpEncodable};

    use super::*;

    #[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
    struct Body {
        value: u64,
    }

    impl SignedBody for Body {
        type Domain = VoteDomain;
    }

    #[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
    struct OtherBody {
        value: u64,
    }

    impl SignedBody for OtherBody {
        type Domain = PathVoteDomain;
    }

    fn signed(slot: u64, signer: u64, value: u64) -> Signed<Body> {
        let scheme = StubPq::default();
        let signer = NodeId::dummy(signer);
        Signed::sign(Slot(slot), signer, Body { value }, &scheme.signer(signer))
    }

    #[test]
    fn stub_verifies_own_signature() {
        let msg = signed(3, 1, 7);
        assert!(msg.verify(Slot(3), NodeId::dummy(1), &StubPq::default()));
        assert_eq!(msg.sig.0.len(), STUB_SIGNATURE_LEN);
    }

    #[test]
    fn stub_rejects_wrong_signer_slot_or_body() {
        let verifier = StubPq::default();
        let msg = signed(3, 1, 7);

        assert!(!msg.verify(Slot(3), NodeId::dummy(2), &verifier));
        assert!(!msg.verify(Slot(4), NodeId::dummy(1), &verifier));

        let mut tampered = msg;
        tampered.body.value = 8;
        assert!(!tampered.verify(Slot(3), NodeId::dummy(1), &verifier));
    }

    #[test]
    fn domains_separate_signatures() {
        let scheme = StubPq::default();
        let signer = NodeId::dummy(1);
        let vote = Signed::<Body>::signing_bytes(Slot(1), signer, &Body { value: 5 });
        let path = Signed::<OtherBody>::signing_bytes(Slot(1), signer, &OtherBody { value: 5 });
        assert!(vote.starts_with(VoteDomain::PREFIX));
        assert!(path.starts_with(PathVoteDomain::PREFIX));
        assert_ne!(
            scheme.signer(signer).sign(&vote),
            scheme.signer(signer).sign(&path)
        );
    }

    #[test]
    fn padded_length_and_oversize_rejection() {
        let long = StubPq::new(2420);
        let signer = NodeId::dummy(4);
        let msg = Signed::sign(Slot(1), signer, Body { value: 1 }, &long.signer(signer));
        assert_eq!(msg.sig.0.len(), 2420);
        assert!(msg.verify(Slot(1), signer, &long));
        // a verifier for shorter signatures rejects before hashing
        assert!(!msg.verify(Slot(1), signer, &StubPq::default()));
    }

    #[test]
    fn signed_rlp_roundtrip() {
        let msg = signed(9, 2, 11);
        let bytes = alloy_rlp::encode(&msg);
        assert_eq!(bytes.len(), msg.length());
        let decoded: Signed<Body> = alloy_rlp::decode_exact(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }
}
