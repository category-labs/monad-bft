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

use alloy_rlp::{Decodable, Encodable};
use k256::{
    elliptic_curve::hash2curve::{ExpandMsgXmd, GroupDigest},
    Secp256k1 as K256Secp256k1,
};
use monad_crypto::{
    hasher::{Hasher, HasherType},
    signing_domain::SigningDomain,
};
use secp256k1::Secp256k1;
use sha2::Sha256;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// secp256k1 public key
#[derive(Copy, Clone, PartialOrd, Ord)]
pub struct PubKey(secp256k1::PublicKey);
/// secp256k1 keypair
pub struct KeyPair(secp256k1::Keypair);

#[derive(ZeroizeOnDrop)]
pub struct PrivKeyView(Vec<u8>);

impl std::fmt::Display for PrivKeyView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

/// secp256k1 ecdsa recoverable signature
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct SecpSignature(secp256k1::ecdsa::RecoverableSignature);

/// wrapped secp256k1 library errors
#[derive(Debug, Clone)]
pub struct Error(secp256k1::Error);

impl From<secp256k1::Error> for Error {
    fn from(value: secp256k1::Error) -> Self {
        Error(value)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for Error {}

impl std::fmt::Debug for PubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let ser = self.bytes_compressed();
        for byte in ser {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl std::cmp::PartialEq for PubKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_fast_unstable(&other.0)
    }
}

impl std::cmp::Eq for PubKey {}

impl std::hash::Hash for PubKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let slice = unsafe { std::mem::transmute::<Self, [u8; 64]>(*self) };
        slice.hash(state)
    }
}

fn msg_hash<SD: SigningDomain>(msg: &[u8]) -> secp256k1::Message {
    let mut hasher = HasherType::new();
    hasher.update(SD::PREFIX);
    hasher.update(msg);
    let hash = hasher.hash();

    secp256k1::Message::from_digest(hash.0)
}

#[cfg(test)]
fn msg_hash_sha256<SD: SigningDomain>(msg: &[u8]) -> secp256k1::Message {
    use monad_crypto::hasher::{Hasher, Sha256Hash};
    let mut hasher = Sha256Hash::new();
    hasher.update(SD::PREFIX);
    hasher.update(msg);
    let hash = hasher.hash();

    secp256k1::Message::from_digest(hash.0)
}

impl KeyPair {
    pub fn generate<R: secp256k1::rand::Rng + secp256k1::rand::CryptoRng>(rng: &mut R) -> Self {
        let keypair = secp256k1::Keypair::new(secp256k1::SECP256K1, rng);
        Self(keypair)
    }

    /// Create a keypair from a secret key slice. The secret is zero-ized after
    /// use. The secret must be 32 bytes.
    pub fn from_bytes(secret: &mut [u8]) -> Result<Self, Error> {
        let secret_array: [u8; 32] = secret
            .try_into()
            .map_err(|_| Error(secp256k1::Error::InvalidSecretKey))?;
        let keypair =
            secp256k1::Keypair::from_seckey_byte_array(secp256k1::SECP256K1, secret_array)
                .map(Self)
                .map_err(Error);
        secret.zeroize();
        keypair
    }

    pub fn from_ikm(ikm: &mut [u8]) -> Result<Self, Error> {
        let dst = b"monad-ecdsa-keygen";
        let scalar =
            <K256Secp256k1 as GroupDigest>::hash_to_scalar::<ExpandMsgXmd<Sha256>>(&[ikm], &[dst])
                .map_err(|_| Error(secp256k1::Error::InvalidSecretKey))?;
        ikm.zeroize();
        let mut scalar_bytes = scalar.to_bytes();
        let result = Self::from_bytes(scalar_bytes.as_mut_slice());
        scalar_bytes.zeroize();
        result
    }

    /// Create a SecpSignature over Hash(msg)
    pub fn sign<SD: SigningDomain>(&self, msg: &[u8]) -> SecpSignature {
        SecpSignature(Secp256k1::sign_ecdsa_recoverable(
            secp256k1::SECP256K1,
            msg_hash::<SD>(msg),
            &self.0.secret_key(),
        ))
    }

    pub fn privkey_view(&self) -> PrivKeyView {
        PrivKeyView(self.0.secret_bytes().into())
    }

    /// Get the pubkey
    pub fn pubkey(&self) -> PubKey {
        PubKey(self.0.public_key())
    }

    pub fn ecdh(&self, public_key: &PubKey) -> [u8; 32] {
        let shared_secret = secp256k1::ecdh::SharedSecret::new(&public_key.0, &self.0.secret_key());
        shared_secret.secret_bytes()
    }
}

impl AsRef<KeyPair> for KeyPair {
    fn as_ref(&self) -> &KeyPair {
        self
    }
}

impl PubKey {
    /// Deserialize public key from bytes
    /// Can be compressed OR uncompressed pubkey
    pub fn from_slice(pubkey: &[u8]) -> Result<Self, Error> {
        secp256k1::PublicKey::from_slice(pubkey)
            .map(Self)
            .map_err(Error)
    }

    /// Serialize public key
    pub fn bytes(&self) -> [u8; 65] {
        self.0.serialize_uncompressed()
    }

    pub fn bytes_compressed(&self) -> [u8; 33] {
        self.0.serialize()
    }

    /// Verify that the message is correctly signed
    pub fn verify<SD: SigningDomain>(
        &self,
        msg: &[u8],
        signature: &SecpSignature,
    ) -> Result<(), Error> {
        Secp256k1::verify_ecdsa(
            secp256k1::SECP256K1,
            msg_hash::<SD>(msg),
            &signature.0.to_standard(),
            &self.0,
        )
        .map_err(Error)
    }

    /// Verify that the message is correctly signed using SHA256 hash
    #[cfg(test)]
    pub fn verify_sha256<SD: SigningDomain>(
        &self,
        msg: &[u8],
        signature: &SecpSignature,
    ) -> Result<(), Error> {
        Secp256k1::verify_ecdsa(
            secp256k1::SECP256K1,
            msg_hash_sha256::<SD>(msg),
            &signature.0.to_standard(),
            &self.0,
        )
        .map_err(Error)
    }
}

impl SecpSignature {
    /// Recover the pubkey from signature given the message
    pub fn recover_pubkey<SD: SigningDomain>(&self, msg: &[u8]) -> Result<PubKey, Error> {
        Secp256k1::recover_ecdsa(secp256k1::SECP256K1, msg_hash::<SD>(msg), &self.0)
            .map(PubKey)
            .map_err(Error)
    }

    /// Serialize the signature. The signature itself is 64 bytes. An extra byte
    /// is used to store the RecoveryId to recover the pubkey
    pub fn serialize(&self) -> [u8; secp256k1::constants::COMPACT_SIGNATURE_SIZE + 1] {
        // recid is 0..3, fit in a single byte (see secp256k1 https://docs.rs/secp256k1/0.27.0/src/secp256k1/ecdsa/recovery.rs.html#39)
        let (recid, sig) = self.0.serialize_compact();
        let recid_byte = recid as u8;
        assert!((0..=3).contains(&recid_byte));
        let mut sig_vec = sig.to_vec();
        sig_vec.push(recid_byte);
        sig_vec.try_into().unwrap()
    }

    /// Deserialize the signature
    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
        if data.len() != secp256k1::constants::COMPACT_SIGNATURE_SIZE + 1 {
            return Err(Error(secp256k1::Error::InvalidSignature));
        }
        let sig_data = &data[..secp256k1::constants::COMPACT_SIGNATURE_SIZE];
        let recid = secp256k1::ecdsa::RecoveryId::try_from(
            data[secp256k1::constants::COMPACT_SIGNATURE_SIZE] as i32,
        )
        .map_err(Error)?;
        Ok(SecpSignature(
            secp256k1::ecdsa::RecoverableSignature::from_compact(sig_data, recid).map_err(Error)?,
        ))
    }
}

impl Encodable for SecpSignature {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        self.serialize().encode(out);
    }
}

impl Decodable for SecpSignature {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let raw_bytes = <[u8; 65]>::decode(buf)?;

        match SecpSignature::deserialize(&raw_bytes) {
            Ok(sig) => Ok(sig),
            Err(_) => Err(alloy_rlp::Error::Custom("invalid secp signature")),
        }
    }
}

impl Drop for KeyPair {
    fn drop(&mut self) {
        self.0.non_secure_erase();
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::{signing_domain, signing_domain::SigningDomain};
    use proptest::prelude::*;
    use sha2::{Digest, Sha256};
    use tiny_keccak::Hasher;
    use wycheproof::{
        ecdsa::{TestName::EcdsaSecp256k1Sha256, TestSet},
        TestResult,
    };

    use super::{KeyPair, PubKey, SecpSignature};

    type SigningDomainType = signing_domain::Vote;

    #[test]
    fn test_pubkey_roundtrip() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let pubkey_bytes = keypair.pubkey().bytes();
        assert_eq!(
            pubkey_bytes,
            PubKey::from_slice(&pubkey_bytes).unwrap().bytes()
        );
        let pubkey_compressed_bytes = keypair.pubkey().bytes_compressed();
        assert_eq!(
            pubkey_bytes,
            PubKey::from_slice(&pubkey_compressed_bytes)
                .unwrap()
                .bytes()
        );
    }

    #[test]
    fn test_eth_address() {
        let mut privkey =
            hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530")
                .unwrap();
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let mut hasher = tiny_keccak::Keccak::v256();
        // pubkey() returns 65 bytes, ignore first one
        hasher.update(&keypair.pubkey().bytes()[1..]);
        let mut output = [0u8; 32];
        hasher.finalize(&mut output);

        let generated_eth_address = output[12..].to_vec();

        let expected_eth_address = hex::decode("ff7F1B7DbaaF35259dDa7cb42564CB7507C1D88d").unwrap();
        assert_eq!(generated_eth_address, expected_eth_address);
    }

    #[test]
    fn test_verify() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign::<SigningDomainType>(msg);

        assert!(keypair
            .pubkey()
            .verify::<SigningDomainType>(msg, &signature)
            .is_ok());
        assert!(keypair
            .pubkey()
            .verify::<SigningDomainType>(b"bye world", &signature)
            .is_err());
    }

    #[test]
    fn test_domain_separation() {
        struct AnotherDomain;
        impl SigningDomain for AnotherDomain {
            const PREFIX: &'static [u8] = b"another_domain";
        }

        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign::<SigningDomainType>(msg);

        assert!(keypair
            .pubkey()
            .verify::<SigningDomainType>(msg, &signature)
            .is_ok());
        assert!(keypair
            .pubkey()
            .verify::<AnotherDomain>(msg, &signature)
            .is_err());
    }

    #[test]
    fn test_recovery() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign::<SigningDomainType>(msg);

        let recovered_key = signature.recover_pubkey::<SigningDomainType>(msg).unwrap();

        assert!(keypair.pubkey().bytes() == recovered_key.bytes());
    }

    #[test]
    fn test_signature_serde() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign::<SigningDomainType>(msg);

        let ser = signature.serialize();
        let deser = SecpSignature::deserialize(&ser);
        assert_eq!(signature, deser.unwrap());
    }

    #[test]
    fn test_signature_rlp() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"hello world";
        let signature = keypair.sign::<SigningDomainType>(msg);

        let rlp = alloy_rlp::encode(signature);
        let x: SecpSignature = alloy_rlp::decode_exact(rlp).unwrap();

        assert_eq!(signature, x);
    }

    #[test]
    fn test_from_ikm() {
        let ikm = b"test input keying material 32byt";
        let keypair = KeyPair::from_ikm(ikm).unwrap();

        let msg = b"test message";
        let signature = keypair.sign::<SigningDomainType>(msg);
        assert!(keypair
            .pubkey()
            .verify::<SigningDomainType>(msg, &signature)
            .is_ok());

        let keypair2 = KeyPair::from_ikm(ikm).unwrap();
        assert_eq!(keypair.pubkey().bytes(), keypair2.pubkey().bytes());
    }

    #[test]
    fn test_secp256k1_out_of_range_key_fails() {
        let mut zero_key = [0u8; 32];
        let result = KeyPair::from_bytes(&mut zero_key);
        assert!(result.is_err());

        let mut curve_order =
            hex::decode("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141")
                .unwrap();
        let result = KeyPair::from_bytes(&mut curve_order);
        assert!(result.is_err());

        let mut above_order =
            hex::decode("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364142")
                .unwrap();
        let result = KeyPair::from_bytes(&mut above_order);
        assert!(result.is_err());

        let mut max_value = [0xFFu8; 32];
        let result = KeyPair::from_bytes(&mut max_value);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_ikm_deterministic_profiles() {
        #[derive(Debug)]
        struct IkmProfile {
            #[allow(dead_code)]
            ikm_hex: String,
            #[allow(dead_code)]
            private_key_hex: String,
            #[allow(dead_code)]
            pubkey_hex: String,
        }

        let test_ikm_hexes = [
            "0000000000000000000000000000000000000000000000000000000000000001",
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "c0c1c2c3c4c5c6c7c8c9cacbcccdcecfc0c1c2c3c4c5c6c7c8c9cacbcccdcecf",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "2222222222222222222222222222222222222222222222222222222222222222",
            "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
        ];

        let profiles: Vec<_> = test_ikm_hexes
            .into_iter()
            .map(|ikm_hex| {
                let ikm = hex::decode(ikm_hex).unwrap();
                let keypair = KeyPair::from_ikm(&ikm).unwrap();
                let pubkey_bytes = keypair.pubkey().bytes();

                IkmProfile {
                    ikm_hex: ikm_hex.to_string(),
                    private_key_hex: keypair.privkey_view().to_string(),
                    pubkey_hex: hex::encode(pubkey_bytes),
                }
            })
            .collect();

        insta::assert_debug_snapshot!(profiles);
    }

    fn map_wycheproof_result(result: TestResult) -> TestResult {
        match result {
            TestResult::Valid => TestResult::Valid,
            TestResult::Invalid => TestResult::Invalid,
            TestResult::Acceptable => TestResult::Valid,
        }
    }

    #[test]
    fn test_secp_wycheproof_conformance() {
        // Define a special signing domain for Wycheproof that uses raw SHA256
        struct WycheproofRawSha256;
        impl SigningDomain for WycheproofRawSha256 {
            const PREFIX: &'static [u8] = b"";
        }

        let test_set = TestSet::load(EcdsaSecp256k1Sha256).unwrap();

        for test_group in test_set.test_groups {
            let pk = PubKey::from_slice(&test_group.key.key).unwrap();

            for test in test_group.tests {
                let Ok(mut sig_der) = secp256k1::ecdsa::Signature::from_der(&test.sig) else {
                    assert_eq!(
                        map_wycheproof_result(test.result),
                        TestResult::Invalid,
                        "failed to decode signature in test {}: {}",
                        test.tc_id,
                        test.comment
                    );
                    continue;
                };
                // We need to normalize the signature because rust-secp256k1 (via libsecp256k1)
                // treats signatures with s in the upper half of the curve order as invalid (“low-S only”)
                // to eliminate ECDSA’s malleability.
                sig_der.normalize_s();

                let sig_bytes = sig_der.serialize_compact();

                // Wycheproof tests do not provide a recovery id, iterate over all possible ones to verify.
                let mut n_bytes = [0u8; 65];
                n_bytes[..64].copy_from_slice(&sig_bytes);

                let mut res = TestResult::Invalid;
                for i in 0..4 {
                    n_bytes[64] = i;
                    let sig = SecpSignature::deserialize(&n_bytes).unwrap();

                    if pk
                        .verify_sha256::<WycheproofRawSha256>(&test.msg, &sig)
                        .is_ok()
                    {
                        res = TestResult::Valid;
                        break;
                    } else {
                        continue;
                    }
                }
                assert_eq!(
                    map_wycheproof_result(test.result),
                    res,
                    "test {} failed: {}",
                    test.tc_id,
                    test.comment
                );
            }
        }
    }

    #[test]
    fn test_original_secp256k1_wycheproof_conformance() {
        let test_set = TestSet::load(EcdsaSecp256k1Sha256).unwrap();

        for test_group in test_set.test_groups {
            let pk = secp256k1::PublicKey::from_slice(&test_group.key.key).unwrap();

            for test in test_group.tests {
                let Ok(mut sig_der) = secp256k1::ecdsa::Signature::from_der(&test.sig) else {
                    assert_eq!(
                        map_wycheproof_result(test.result),
                        TestResult::Invalid,
                        "failed to decode signature in test {}: {}",
                        test.tc_id,
                        test.comment
                    );
                    continue;
                };
                // We need to normalize the signature because rust-secp256k1 (via libsecp256k1)
                // treats signatures with s in the upper half of the curve order as invalid (“low-S only”)
                // to eliminate ECDSA’s malleability.
                sig_der.normalize_s();

                let msg = secp256k1::Message::from_digest(Sha256::digest(&test.msg).into());
                let res =
                    if secp256k1::Secp256k1::verify_ecdsa(secp256k1::SECP256K1, msg, &sig_der, &pk)
                        .is_ok()
                    {
                        TestResult::Valid
                    } else {
                        TestResult::Invalid
                    };
                assert_eq!(
                    map_wycheproof_result(test.result),
                    res,
                    "test {} failed: {}",
                    test.tc_id,
                    test.comment
                );
            }
        }
    }

    #[test]
    fn test_pubkey_operations() {
        use std::{
            cmp::Ordering,
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };
        fn hash_pk(pk: &PubKey) -> u64 {
            let mut h = DefaultHasher::new();
            pk.hash(&mut h);
            h.finish()
        }

        // Deterministic key material for reproducibility
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
        let pubkey1 = keypair.pubkey();

        // Roundtrip via canonical encodings
        let pubkey2 = PubKey::from_slice(&pubkey1.bytes()).unwrap();
        let pubkey3 = PubKey::from_slice(&pubkey1.bytes_compressed()).unwrap();

        // Equality must hold
        assert_eq!(
            pubkey1, pubkey2,
            "PubKey equality broke after uncompressed roundtrip"
        );
        assert_eq!(
            pubkey1, pubkey3,
            "PubKey equality broke after compressed roundtrip"
        );
        assert_eq!(
            pubkey1.cmp(&pubkey2),
            Ordering::Equal,
            "Ord/Eq contract violated for uncompressed roundtrip"
        );
        assert_eq!(
            pubkey1.cmp(&pubkey3),
            Ordering::Equal,
            "Ord/Eq contract violated for compressed roundtrip"
        );

        let h1 = hash_pk(&pubkey1);
        let h2 = hash_pk(&pubkey2);
        let h3 = hash_pk(&pubkey3);
        assert_eq!(
            h1, h2,
            "Hash/Eq contract violated for uncompressed roundtrip"
        );
        assert_eq!(h1, h3, "Hash/Eq contract violated for compressed roundtrip");
        assert_eq!(h1, hash_pk(&pubkey1), "Hash not stable within a run");
    }

    #[test]
    fn test_special_private_keys() {
        let mut zero =
            hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                .unwrap();
        assert!(KeyPair::from_bytes(&mut zero).is_err());

        let mut one =
            hex::decode("0000000000000000000000000000000000000000000000000000000000000001")
                .unwrap();
        assert!(KeyPair::from_bytes(&mut one).is_ok());

        let mut order_minus_one =
            hex::decode("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364140")
                .unwrap();
        assert!(KeyPair::from_bytes(&mut order_minus_one).is_ok());

        let mut order =
            hex::decode("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141")
                .unwrap();
        assert!(KeyPair::from_bytes(&mut order).is_err());
    }

    #[test]
    fn test_non_malleability() {
        // Big-endian subtraction: a - b (assuming a >= b)
        fn sub_be_32(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
            let mut out = [0u8; 32];
            let mut borrow: u16 = 0;
            for i in (0..32).rev() {
                let ai = a[i] as u16;
                let bi = b[i] as u16;
                let tmp = ai.wrapping_sub(bi).wrapping_sub(borrow);
                out[i] = (tmp & 0xFF) as u8;
                borrow = if ai < bi + borrow { 1 } else { 0 };
            }
            out
        }

        // 1) Generate a key and sign a message
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
        let pubkey = keypair.pubkey();

        let msg = b"malleability test message";
        let sig = keypair.sign::<SigningDomainType>(msg);

        assert!(pubkey.verify::<SigningDomainType>(msg, &sig).is_ok());

        // 2) Assert the produced signature is already "low-S"
        // (normalize_s() should not change it)
        let std_sig = sig.0.to_standard();
        let mut std_sig_norm = std_sig;
        std_sig_norm.normalize_s();
        assert_eq!(
            std_sig.serialize_compact(),
            std_sig_norm.serialize_compact(),
            "Signer produced a high-S signature; expected low-S"
        );

        // 3) Construct malleable signature: (r, s') with s' = n - s
        let comp = std_sig.serialize_compact(); // 64 bytes = r||s
        let mut r = [0u8; 32];
        let mut s = [0u8; 32];
        r.copy_from_slice(&comp[..32]);
        s.copy_from_slice(&comp[32..]);

        assert_ne!(s, [0u8; 32], "unexpected s=0 (should never happen)");

        let n: [u8; 32] = secp256k1::constants::CURVE_ORDER;
        let s_malleable = sub_be_32(&n, &s);

        // Build 65-byte wrapper encoding: r||s'||recid
        let mut mal_bytes = [0u8; 65];
        mal_bytes[..32].copy_from_slice(&r);
        mal_bytes[32..64].copy_from_slice(&s_malleable);
        mal_bytes[64] = sig.serialize()[64];

        // 4) The malleable signature must be rejected:
        let mal_sig = SecpSignature::deserialize(&mal_bytes).unwrap();
        assert!(
            pubkey.verify::<SigningDomainType>(msg, &mal_sig).is_err(),
            "High-S malleable signature successfully verified; signature is malleable"
        );
    }

    #[test]
    fn test_signature_deserialize_rejects_bad_lengths_and_recid() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
        let msg = b"negative signature parsing";
        let sig = keypair.sign::<SigningDomainType>(msg);
        let good = sig.serialize();

        // Bad lengths (DoS hardening: must return Err, never panic)
        for len in [0usize, 1, 10, 64, 66, 100] {
            let buf = vec![0u8; len];
            assert!(
                SecpSignature::deserialize(&buf).is_err(),
                "expected error for length {}",
                len
            );
        }

        // Bad recid values must be rejected (last byte is RecoveryId)
        for bad_recid in 4u8..=255u8 {
            let mut b = good;
            b[64] = bad_recid;
            assert!(
                SecpSignature::deserialize(&b).is_err(),
                "expected error for bad recid {}",
                bad_recid
            );
        }
    }

    #[test]
    fn test_signature_bitflip_corruption_is_rejected_or_fails_verify() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
        let pk = keypair.pubkey();

        let msg = b"bitflip corruption test";
        let sig = keypair.sign::<SigningDomainType>(msg);
        let good = sig.serialize();

        // Flip one bit in r/s bytes and require: either parse fails or verify fails.
        for i in [0usize, 5, 16, 31, 32, 40, 63] {
            let mut b = good;
            b[i] ^= 0x01;

            if let Ok(corrupted) = SecpSignature::deserialize(&b) {
                assert!(
                    pk.verify::<SigningDomainType>(msg, &corrupted).is_err(),
                    "corrupted signature unexpectedly verified (index {})",
                    i
                );
            }
        }

        // Flip the recid bit: verification ignores recid, but recovery should differ or fail.
        for bit in [0x01u8, 0x02u8, 0x04u8, 0x08u8] {
            let mut b = good;
            b[64] ^= bit;

            if let Ok(corrupted) = SecpSignature::deserialize(&b) {
                let recovered = corrupted.recover_pubkey::<SigningDomainType>(msg);
                // For a different (valid) recid, recovery should not yield the original key.
                if let Ok(rpk) = recovered {
                    assert_ne!(
                        rpk.bytes(),
                        pk.bytes(),
                        "mutating recid should not recover original pubkey"
                    );
                }
            }
        }
    }

    #[test]
    fn test_pubkey_from_slice_rejects_invalid_encodings() {
        // Wrong lengths (must be 33 or 65).
        // This is only a necessary condition, not sufficient.
        for len in [0usize, 1, 10, 32, 34, 64, 66, 100, 128, 255] {
            let buf = vec![0u8; len];
            assert!(
                PubKey::from_slice(&buf).is_err(),
                "expected error for pubkey length {}",
                len
            );
        }

        // Generator G (compressed, 33 bytes)
        {
            let pk_bytes =
                hex::decode("0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798")
                    .unwrap();
            let pk = PubKey::from_slice(&pk_bytes).expect("valid compressed pubkey should parse");
            assert_eq!(pk.bytes_compressed(), pk_bytes.as_slice());
        }

        // Generator G (uncompressed, 65 bytes)
        {
            let pk_bytes = hex::decode(
                "0479be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8",
            ).unwrap();
            let pk = PubKey::from_slice(&pk_bytes).expect("valid uncompressed pubkey should parse");
            assert_eq!(pk.bytes(), pk_bytes.as_slice());
        }

        // Invalid prefix for compressed (must be 0x02 or 0x03)
        {
            let pk =
                hex::decode("0479be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798")
                    .unwrap();
            assert!(
                PubKey::from_slice(&pk).is_err(),
                "bad compressed prefix accepted"
            );
        }

        // Invalid prefix for uncompressed (must be 0x04)
        {
            let pk = hex::decode(
                "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8",
            )
            .unwrap();
            assert!(
                PubKey::from_slice(&pk).is_err(),
                "bad uncompressed prefix accepted"
            );
        }

        // Uncompressed point not on curve: (x=1, y=1) is not on secp256k1
        {
            let mut pk = [0u8; 65];
            pk[0] = 0x04;
            pk[32] = 0x01; // x = 1
            pk[64] = 0x01; // y = 1
            assert!(
                PubKey::from_slice(&pk).is_err(),
                "non-curve point (1,1) accepted"
            );
        }

        // Compressed x out of field range: x = p (field prime) must be rejected (x must be < p)
        {
            // secp256k1 field prime p:
            // FFFFFFFF FFFFFFFF FFFFFFFF FFFFFFFF FFFFFFFF FFFFFFFF FFFFFFFE FFFFFC2F
            let pk =
                hex::decode("02FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F")
                    .unwrap();
            assert!(
                PubKey::from_slice(&pk).is_err(),
                "compressed x=p (out of range) accepted"
            );
        }
    }

    #[test]
    fn test_signing_is_deterministic_for_same_key_domain_and_message() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();

        let msg = b"determinism check";
        let sig1 = keypair.sign::<SigningDomainType>(msg).serialize();
        let sig2 = keypair.sign::<SigningDomainType>(msg).serialize();

        assert_eq!(
            sig1, sig2,
            "signing is not deterministic; nonce behavior changed (or RNG introduced)"
        );
    }

    #[test]
    fn test_recovery_misuse() {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
        let pk = keypair.pubkey();

        let msg1 = b"recovery misuse msg1";
        let msg2 = b"recovery misuse msg2";

        let sig = keypair.sign::<SigningDomainType>(msg1);

        // Correct recovery should return the signing pubkey
        let recovered1 = sig.recover_pubkey::<SigningDomainType>(msg1).unwrap();
        assert_eq!(recovered1.bytes(), pk.bytes());

        // Wrong-message recovery should not yield the original pubkey
        let recovered2 = sig.recover_pubkey::<SigningDomainType>(msg2).unwrap();
        assert_ne!(
            recovered2.bytes(),
            pk.bytes(),
            "wrong-message recovery unexpectedly produced original pubkey"
        );

        // Verification is message-bound
        assert!(pk.verify::<SigningDomainType>(msg1, &sig).is_ok());
        assert!(pk.verify::<SigningDomainType>(msg2, &sig).is_err());
    }

    proptest! {
        #[test]
        fn proptest_from_ikm(ikm: [u8; 32]) {
            let keypair_result = KeyPair::from_ikm(&ikm);

            match keypair_result {
                Ok(keypair) => {
                    let msg = b"test message for proptest";
                    let signature = keypair.sign::<SigningDomainType>(msg);
                    prop_assert!(keypair.pubkey().verify::<SigningDomainType>(msg, &signature).is_ok());
                }
                Err(e) => {
                    panic!("key should be valid: {:?}", e);
                }
            }
        }

        #[test]
        fn proptest_no_panic_signature_deserialize(_case in any::<[u8; 65]>()) {
            // Must never panic
            let _ = SecpSignature::deserialize(&_case);
        }

        #[test]
        fn proptest_no_panic_pubkey_from_slice(bytes in proptest::collection::vec(any::<u8>(), 0..100)) {
            // Must never panic
            let _ = PubKey::from_slice(&bytes);
        }

        #[test]
        fn proptest_no_panic_rlp_decode(bytes in proptest::collection::vec(any::<u8>(), 0..200)) {
            // Must never panic
            let _ = alloy_rlp::decode_exact::<SecpSignature>(&bytes);
        }
    }
}
