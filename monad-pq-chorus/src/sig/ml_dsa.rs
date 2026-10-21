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

//! ML-DSA-44 (FIPS 204) behind the signature seam: deterministic signing
//! with the empty context string, keys looked up by `NodeId`.

use std::{collections::HashMap, fmt};

use bytes::Bytes;
use ml_dsa::{
    EncodedVerifyingKey, Keypair as _, MlDsa44, Seed, Signature, Signer as _, SigningKey,
    Verifier as _, VerifyingKey,
};
use monad_mcp_chorus::stub::types::NodeId;
use tiny_keccak::{Hasher, Keccak};

use super::{PqSignature, PqSigner, PqVerifier};

pub const ML_DSA_44_SIGNATURE_LEN: usize = 2420;
pub const ML_DSA_44_PUBLIC_KEY_LEN: usize = 1312;
pub const ML_DSA_SEED_LEN: usize = 32;

const DEV_SEED_DOMAIN: &[u8] = b"monad/pq-cadence/ml-dsa-44/dev-seed/1";

/// A validator's ML-DSA-44 signing key, kept as its FIPS 204 seed.
pub struct MlDsa44Signer {
    key: SigningKey<MlDsa44>,
}

impl MlDsa44Signer {
    pub fn from_seed(seed: &[u8; ML_DSA_SEED_LEN]) -> Self {
        Self {
            key: SigningKey::from_seed(&Seed::from(*seed)),
        }
    }

    /// Test networks only: anyone can derive this key from the node id.
    pub fn dev(id: NodeId) -> Self {
        Self::from_seed(&dev_seed(id))
    }

    pub fn public_key(&self) -> MlDsa44PublicKey {
        MlDsa44PublicKey(self.key.verifying_key())
    }
}

impl fmt::Debug for MlDsa44Signer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MlDsa44Signer").finish_non_exhaustive()
    }
}

impl PqSigner for MlDsa44Signer {
    fn sign(&self, msg: &[u8]) -> PqSignature {
        let sig: Signature<MlDsa44> = self.key.sign(msg);
        PqSignature(Bytes::copy_from_slice(&sig.encode()))
    }
}

/// `keccak256(domain || id)`: public, so never for a real network.
pub fn dev_seed(id: NodeId) -> [u8; ML_DSA_SEED_LEN] {
    let mut seed = [0u8; ML_DSA_SEED_LEN];
    let mut keccak = Keccak::v256();
    keccak.update(DEV_SEED_DOMAIN);
    keccak.update(&u64::from(id).to_be_bytes());
    keccak.finalize(&mut seed);
    seed
}

#[derive(Clone, PartialEq)]
pub struct MlDsa44PublicKey(VerifyingKey<MlDsa44>);

impl MlDsa44PublicKey {
    /// `None` unless `bytes` is an encoded key of the right length.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let encoded = EncodedVerifyingKey::<MlDsa44>::try_from(bytes).ok()?;
        Some(Self(VerifyingKey::decode(&encoded)))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.encode().to_vec()
    }
}

impl fmt::Debug for MlDsa44PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.to_bytes();
        write!(f, "MlDsa44PublicKey({:02x?}..)", &bytes[..4])
    }
}

/// The validator set's public keys.
#[derive(Clone, Debug, Default)]
pub struct MlDsa44Verifier {
    keys: HashMap<NodeId, MlDsa44PublicKey>,
}

impl MlDsa44Verifier {
    pub fn new(keys: impl IntoIterator<Item = (NodeId, MlDsa44PublicKey)>) -> Self {
        Self {
            keys: keys.into_iter().collect(),
        }
    }

    /// The keys of `dev` signers, for test networks.
    pub fn dev(validators: impl IntoIterator<Item = NodeId>) -> Self {
        Self::new(
            validators
                .into_iter()
                .map(|id| (id, MlDsa44Signer::dev(id).public_key())),
        )
    }

    pub fn public_key(&self, id: &NodeId) -> Option<&MlDsa44PublicKey> {
        self.keys.get(id)
    }
}

impl PqVerifier for MlDsa44Verifier {
    fn verify(&self, signer: NodeId, msg: &[u8], sig: &PqSignature) -> bool {
        let Some(key) = self.keys.get(&signer) else {
            return false;
        };
        let Ok(sig) = Signature::<MlDsa44>::try_from(sig.0.as_ref()) else {
            return false;
        };
        key.0.verify(msg, &sig).is_ok()
    }

    fn max_sig_len(&self) -> usize {
        ML_DSA_44_SIGNATURE_LEN
    }
}

#[cfg(test)]
mod tests {
    use alloy_rlp::{RlpDecodable, RlpEncodable};
    use monad_mcp_chorus::stub::types::Slot;

    use super::*;
    use crate::sig::{PathVoteDomain, Signed, SignedBody, VoteDomain};

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

    fn verifier() -> MlDsa44Verifier {
        MlDsa44Verifier::dev((0..4).map(NodeId::dummy))
    }

    fn signed(slot: u64, signer: u64, value: u64) -> Signed<Body> {
        let signer = NodeId::dummy(signer);
        Signed::sign(
            Slot(slot),
            signer,
            Body { value },
            &MlDsa44Signer::dev(signer),
        )
    }

    #[test]
    fn verifies_own_signature_at_the_fips_204_sizes() {
        let msg = signed(3, 1, 7);
        assert_eq!(msg.sig.0.len(), ML_DSA_44_SIGNATURE_LEN);
        assert_eq!(
            MlDsa44Signer::dev(NodeId::dummy(1))
                .public_key()
                .to_bytes()
                .len(),
            ML_DSA_44_PUBLIC_KEY_LEN
        );
        assert!(msg.verify(Slot(3), NodeId::dummy(1), &verifier()));
    }

    #[test]
    fn signing_is_deterministic() {
        assert_eq!(signed(3, 1, 7), signed(3, 1, 7));
    }

    #[test]
    fn rejects_wrong_signer_slot_or_body() {
        let verifier = verifier();
        let msg = signed(3, 1, 7);

        assert!(!msg.verify(Slot(3), NodeId::dummy(2), &verifier));
        assert!(!msg.verify(Slot(4), NodeId::dummy(1), &verifier));

        let mut tampered = msg;
        tampered.body.value = 8;
        assert!(!tampered.verify(Slot(3), NodeId::dummy(1), &verifier));
    }

    #[test]
    fn rejects_a_key_outside_the_set_and_a_wrong_key() {
        let msg = signed(3, 9, 7);
        assert!(!msg.verify(Slot(3), NodeId::dummy(9), &verifier()));

        // node 1's slot in the set holds node 2's key
        let swapped = MlDsa44Verifier::new([(
            NodeId::dummy(1),
            MlDsa44Signer::dev(NodeId::dummy(2)).public_key(),
        )]);
        assert!(!signed(3, 1, 7).verify(Slot(3), NodeId::dummy(1), &swapped));
    }

    #[test]
    fn rejects_malformed_signatures() {
        let verifier = verifier();
        let signer = NodeId::dummy(1);
        let msg = signed(3, 1, 7);
        let bytes = msg.sig.0.to_vec();

        let short = PqSignature(Bytes::copy_from_slice(&bytes[..bytes.len() - 1]));
        let mut long = bytes.clone();
        long.push(0);
        let mut flipped = bytes;
        flipped[100] ^= 1;
        let zeros = PqSignature(Bytes::from(vec![0u8; ML_DSA_44_SIGNATURE_LEN]));
        let ones = PqSignature(Bytes::from(vec![0xffu8; ML_DSA_44_SIGNATURE_LEN]));

        let signing = Signed::<Body>::signing_bytes(Slot(3), signer, &msg.body);
        for sig in [
            PqSignature(Bytes::new()),
            short,
            PqSignature(Bytes::from(long)),
            PqSignature(Bytes::from(flipped)),
            zeros,
            ones,
        ] {
            assert!(!verifier.verify(signer, &signing, &sig));
        }
    }

    #[test]
    fn domains_separate_signatures() {
        let id = NodeId::dummy(1);
        let key = MlDsa44Signer::dev(id);
        let vote = Signed::sign(Slot(1), id, Body { value: 5 }, &key);
        let path = Signed::sign(Slot(1), id, OtherBody { value: 5 }, &key);
        assert_ne!(vote.sig, path.sig);
        let crossed = Signed {
            body: OtherBody { value: 5 },
            sig: vote.sig,
        };
        assert!(!crossed.verify(Slot(1), id, &verifier()));
    }

    #[test]
    fn keys_derive_from_the_seed() {
        let seed = [7u8; ML_DSA_SEED_LEN];
        assert_eq!(
            MlDsa44Signer::from_seed(&seed).public_key(),
            MlDsa44Signer::from_seed(&seed).public_key()
        );
        assert_ne!(
            dev_seed(NodeId::dummy(0)),
            dev_seed(NodeId::dummy(1)),
            "dev seeds differ per node"
        );
        assert_ne!(
            MlDsa44Signer::dev(NodeId::dummy(0)).public_key(),
            MlDsa44Signer::dev(NodeId::dummy(1)).public_key()
        );
    }

    #[test]
    fn public_keys_roundtrip_and_reject_bad_lengths() {
        let key = MlDsa44Signer::dev(NodeId::dummy(3)).public_key();
        let bytes = key.to_bytes();
        assert_eq!(MlDsa44PublicKey::from_bytes(&bytes), Some(key));
        assert_eq!(MlDsa44PublicKey::from_bytes(&bytes[1..]), None);
        assert_eq!(MlDsa44PublicKey::from_bytes(&[]), None);
    }

    #[test]
    fn an_ml_dsa_signature_fails_the_stub_and_back() {
        let id = NodeId::dummy(1);
        let msg = signed(3, 1, 7);
        assert!(!msg.verify(Slot(3), id, &crate::sig::StubPq::new(2420)));
        let stub = Signed::sign(
            Slot(3),
            id,
            Body { value: 7 },
            &crate::sig::StubPq::new(2420).signer(id),
        );
        assert!(!stub.verify(Slot(3), id, &verifier()));
    }
}
