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

use std::cmp::Ordering;

use alloy_rlp::{Decodable, Encodable};
use blst::BLST_ERROR::BLST_BAD_ENCODING;
use monad_crypto::signing_domain::SigningDomain;
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop};

/// The cipher suite
///
/// POP (proof of possession) uses a separate pubkey validation step to defend
/// against rogue key attack. It enables fast verification for signatures over
/// the same message
/// https://datatracker.ietf.org/doc/html/draft-irtf-cfrg-bls-signature-05#name-proof-of-possession
#[allow(dead_code)]
const MIN_PK_DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";
#[allow(dead_code)]
const MIN_SIG_DST: &[u8] = b"BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_";

/// The two groups under BLS12-381 are different in size. G1 is smaller than G2.
/// Using the smaller group for pubkey makes signature verification cheaper, but
/// signing and signature aggregation more expensive.
const G1_BYTE_LEN: usize = 96;
const G1_COMPRESSED_LEN: usize = 48;
const G1_INFINITY: [u8; G1_COMPRESSED_LEN] = [
    0xc0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
const G2_BYTE_LEN: usize = 192;
const G2_COMPRESSED_LEN: usize = 96;
const G2_INFINITY: [u8; G2_COMPRESSED_LEN] = [
    0xc0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0,
];

/// The macro assigns the right value to the constants when different groups are
/// used for pubkey/sig
macro_rules! set_curve_constants {
    (minpk) => {
        use blst::min_pk as blst_core;

        const DST: &[u8] = MIN_PK_DST;

        pub const SIGNATURE_BYTE_LEN: usize = G2_BYTE_LEN;
        const SIGNATURE_COMPRESSED_LEN: usize = G2_COMPRESSED_LEN;
        const INFINITY_SIGNATURE: [u8; SIGNATURE_COMPRESSED_LEN] = G2_INFINITY;

        const PUBKEY_BYTE_LEN: usize = G1_BYTE_LEN;
        const PUBKEY_COMPRESSED_LEN: usize = G1_COMPRESSED_LEN;
        const INFINITY_PUBKEY: [u8; PUBKEY_COMPRESSED_LEN] = G1_INFINITY;
    };
    (minsig) => {
        use blst::min_sig as blst_core;

        const DST: &[u8] = MIN_SIG_DST;

        pub const SIGNATURE_BYTE_LEN: usize = G1_BYTE_LEN;
        const SIGNATURE_COMPRESSED_LEN: usize = G1_COMPRESSED_LEN;
        const INFINITY_SIGNATURE: [u8; SIGNATURE_COMPRESSED_LEN] = G1_INFINITY;

        const PUBKEY_BYTE_LEN: usize = G2_BYTE_LEN;
        const PUBKEY_COMPRESSED_LEN: usize = G2_COMPRESSED_LEN;
        const INFINITY_PUBKEY: [u8; PUBKEY_COMPRESSED_LEN] = G2_INFINITY;
    };
}

set_curve_constants!(minpk);

#[derive(Debug, PartialEq, Eq)]
pub struct BlsError(blst::BLST_ERROR);

impl std::fmt::Display for BlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for BlsError {}

/// As [blst::BLST_ERROR::BLST_SUCCESS] is one of the error enums, we use this
/// function to map [blst::BLST_ERROR] to our own Result type
fn map_err_to_result(bls_error: blst::BLST_ERROR) -> Result<(), BlsError> {
    match bls_error {
        blst::BLST_ERROR::BLST_SUCCESS => Ok(()),
        err => Err(BlsError(err)),
    }
}

/// `BlsAggregatePubKey` and `BlsPubKey` are different representations of points
/// in the group. There's a 1-to-1 mapping between the two representations,
/// hence the conversion functions like `as_pubkey` and `from_pubkey`.
///
/// `BlsAggregatePubkey` is a faster representation for aggregation
#[derive(Debug, Clone, Copy)]
pub struct BlsAggregatePubKey(blst_core::AggregatePublicKey);

#[derive(Clone, Copy)]
pub struct BlsPubKey(blst_core::PublicKey);

impl std::fmt::Debug for BlsPubKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.compress()))
    }
}

impl From<blst_core::PublicKey> for BlsPubKey {
    fn from(value: blst_core::PublicKey) -> Self {
        Self(value)
    }
}

/// `transmute` the memory contents is faster than serializing. The memory
/// layout is stable if locked to an implementation version. The same for all
/// other hash implementations
impl std::hash::Hash for BlsPubKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe {
            let slice = std::mem::transmute::<blst_core::PublicKey, [u8; PUBKEY_BYTE_LEN]>(self.0);
            slice.hash(state);
        }
    }
}

impl PartialEq for BlsPubKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for BlsPubKey {}

impl PartialOrd for BlsPubKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlsPubKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.serialize().cmp(&other.serialize())
    }
}

impl BlsPubKey {
    pub fn infinity() -> Self {
        blst_core::PublicKey::deserialize(INFINITY_PUBKEY.as_slice())
            .expect("Infinity BLS pubkey")
            .into()
    }

    /// Validate that the pubkey is a point on the curve. Used to guard against
    /// the subgroup attack
    pub fn validate(&self) -> Result<(), BlsError> {
        self.0.validate().map_err(BlsError)
    }

    pub fn serialize(&self) -> Vec<u8> {
        self.0.serialize().to_vec()
    }

    pub fn deserialize(msg: &[u8]) -> Result<Self, BlsError> {
        if msg.len() != PUBKEY_BYTE_LEN {
            return Err(BlsError(BLST_BAD_ENCODING));
        }
        let pk = blst_core::PublicKey::deserialize(msg)
            .map(Self)
            .map_err(BlsError)?;
        pk.validate()?;
        Ok(pk)
    }

    pub fn compress(&self) -> [u8; PUBKEY_COMPRESSED_LEN] {
        self.0.compress()
    }

    pub fn uncompress(msg: &[u8]) -> Result<Self, BlsError> {
        if msg.len() != PUBKEY_COMPRESSED_LEN {
            return Err(BlsError(BLST_BAD_ENCODING));
        }
        let pk = blst_core::PublicKey::uncompress(msg)
            .map(Self)
            .map_err(BlsError)?;
        pk.validate()?;
        Ok(pk)
    }
}

impl std::hash::Hash for BlsAggregatePubKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_pubkey().hash(state)
    }
}

impl PartialEq for BlsAggregatePubKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_pubkey().eq(&other.as_pubkey())
    }
}

impl Eq for BlsAggregatePubKey {}

impl BlsAggregatePubKey {
    /// The infinity point is the identity element in the group.
    /// Aggregating/adding infinity to anything is the identity function
    pub fn infinity() -> Self {
        Self::from_pubkey(&BlsPubKey::infinity())
    }

    fn as_pubkey(&self) -> BlsPubKey {
        BlsPubKey(self.0.to_public_key())
    }

    fn from_pubkey(pubkey: &BlsPubKey) -> Self {
        Self(blst_core::AggregatePublicKey::from_public_key(&pubkey.0))
    }

    /// Validate that the point is on the curve. Used to guard against the subgroup
    /// attack
    pub fn validate(&self) -> Result<(), BlsError> {
        self.as_pubkey().validate()
    }

    /// Create an AggregatePubKey from an slice of PubKeys
    pub fn aggregate(pks: &[&BlsPubKey]) -> Result<Self, BlsError> {
        let pks = pks.iter().map(|p| &p.0).collect::<Vec<_>>();
        blst_core::AggregatePublicKey::aggregate(pks.as_ref(), false)
            .map(Self)
            .map_err(BlsError)
    }

    /// Aggregate a Pubkey to self
    pub fn add_assign(&mut self, other: &BlsPubKey) {
        self.0
            .add_public_key(&other.0, false)
            // Passing `pk_validate = false` makes this method never produce an error.
            .expect("pubkey aggregation always succeeds")
    }

    /// Aggregate a AggregatePubKey to self
    pub fn add_assign_aggregate(&mut self, other: &Self) {
        self.0.add_aggregate(&other.0)
    }

    pub fn serialize(&self) -> Vec<u8> {
        self.as_pubkey().serialize()
    }

    pub fn deserialize(message: &[u8]) -> Result<Self, BlsError> {
        let pubkey = BlsPubKey::deserialize(message)?;
        Ok(Self::from_pubkey(&pubkey))
    }

    pub fn compress(&self) -> [u8; PUBKEY_COMPRESSED_LEN] {
        self.as_pubkey().compress()
    }

    pub fn uncompress(msg: &[u8]) -> Result<Self, BlsError> {
        let pk = BlsPubKey::uncompress(msg)?;
        Ok(Self::from_pubkey(&pk))
    }
}

#[derive(ZeroizeOnDrop)]
struct BlsSecretKey(blst_core::SecretKey);

#[derive(ZeroizeOnDrop)]
pub struct BlsSecretKeyView(Vec<u8>);

impl std::fmt::Display for BlsSecretKeyView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

/// BLS keypair
pub struct BlsKeyPair {
    pubkey: BlsPubKey,
    secretkey: BlsSecretKey,
}

impl BlsSecretKey {
    fn key_gen(ikm: &mut [u8], key_info: &[u8]) -> Result<Self, BlsError> {
        let blst_key = blst_core::SecretKey::key_gen(ikm, key_info);
        ikm.zeroize();
        blst_key.map(Self).map_err(BlsError)
    }

    fn sk_view(&self) -> BlsSecretKeyView {
        BlsSecretKeyView(self.0.to_bytes().into())
    }

    fn sk_to_pk(&self) -> BlsPubKey {
        self.0.sk_to_pk().into()
    }
}

impl BlsKeyPair {
    /// https://datatracker.ietf.org/doc/html/draft-irtf-cfrg-bls-signature#section-2.3
    /// secret MUST be at least 32 bytes
    pub fn from_bytes(mut secret: impl AsMut<[u8]>) -> Result<Self, BlsError> {
        let secret_mut = secret.as_mut();
        let sk = BlsSecretKey::key_gen(secret_mut, &[])?;
        let keypair = Self {
            pubkey: sk.sk_to_pk(),
            secretkey: sk,
        };
        Ok(keypair)
    }

    /// Import an already-derived secret scalar bytes.
    #[cfg(test)]
    fn from_secret_key_bytes(mut secret_key: impl AsMut<[u8]>) -> Result<Self, BlsError> {
        let b = secret_key.as_mut();
        // blst validates the scalar (non-zero, < r).
        let sk = BlsSecretKey(blst_core::SecretKey::from_bytes(b).map_err(BlsError)?);
        b.zeroize();
        let keypair = Self {
            pubkey: sk.sk_to_pk(),
            secretkey: sk,
        };
        Ok(keypair)
    }

    pub fn from_ikm(mut ikm: impl AsMut<[u8]>) -> Result<Self, BlsError> {
        let dst = b"monad-bls-keygen";
        let ikm_mut = ikm.as_mut();
        let sk = BlsSecretKey::key_gen(ikm_mut, dst)?;
        let keypair = Self {
            pubkey: sk.sk_to_pk(),
            secretkey: sk,
        };
        Ok(keypair)
    }

    pub fn sign<SD: SigningDomain>(&self, msg: &[u8]) -> BlsSignature {
        let msg = [SD::PREFIX, msg].concat();
        self.secretkey.0.sign(&msg, DST, &[]).into()
    }

    pub fn privkey_view(&self) -> BlsSecretKeyView {
        self.secretkey.sk_view()
    }

    pub fn pubkey(&self) -> BlsPubKey {
        self.pubkey
    }
}

/// Similar to [BlsAggregatePubKey] and [BlsPubKey]
#[derive(Clone, Copy)]
pub struct BlsAggregateSignature(blst_core::AggregateSignature);

/// Output the signature serialized bytes in hex string
impl std::fmt::Debug for BlsAggregateSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BlsAggregateSignature")
            .field(&hex::encode(self.serialize()))
            .finish()
    }
}

#[derive(Clone, Copy)]
pub struct BlsSignature(blst_core::Signature);

/// Output the signature serialized bytes in hex string
impl std::fmt::Debug for BlsSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BlsSignature")
            .field(&hex::encode(self.serialize()))
            .finish()
    }
}

impl From<blst_core::Signature> for BlsSignature {
    fn from(value: blst_core::Signature) -> Self {
        Self(value)
    }
}

impl std::hash::Hash for BlsSignature {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe {
            let slice =
                std::mem::transmute::<blst_core::Signature, [u8; SIGNATURE_BYTE_LEN]>(self.0);
            slice.hash(state);
        }
    }
}

impl PartialEq for BlsSignature {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for BlsSignature {}

impl BlsSignature {
    /// Sign the message with `keypair`. `msg` is first hashed to a point on the
    /// curve then signed
    pub fn sign<SD: SigningDomain>(msg: &[u8], keypair: &BlsKeyPair) -> Self {
        keypair.sign::<SD>(msg)
    }

    /// Validate the signature and verify
    pub fn verify<SD: SigningDomain>(
        &self,
        msg: &[u8],
        pubkey: &BlsPubKey,
    ) -> Result<(), BlsError> {
        let msg = [SD::PREFIX, msg].concat();
        let err = self.0.verify(true, &msg, DST, &[], &pubkey.0, true);
        map_err_to_result(err)
    }

    /// Validate that the signature point is on the curve
    pub fn validate(&self, sig_infcheck: bool) -> Result<(), BlsError> {
        self.0.validate(sig_infcheck).map_err(BlsError)
    }

    fn aggregate_verify(
        &self,
        sig_groupcheck: bool,
        msgs: &[&[u8]],
        dst: &[u8],
        pks: &[&BlsPubKey],
        pks_validate: bool,
    ) -> blst::BLST_ERROR {
        let pks = pks.iter().map(|pk| &pk.0).collect::<Vec<_>>();
        self.0
            .aggregate_verify(sig_groupcheck, msgs, dst, pks.as_ref(), pks_validate)
    }

    fn fast_aggregate_verify_pre_aggregated(
        &self,
        sig_groupcheck: bool,
        msg: &[u8],
        dst: &[u8],
        pk: &BlsAggregatePubKey,
    ) -> blst::BLST_ERROR {
        self.0
            .fast_aggregate_verify_pre_aggregated(sig_groupcheck, msg, dst, &pk.as_pubkey().0)
    }

    pub fn serialize(&self) -> Vec<u8> {
        self.compress()
    }

    /// Deserializes a signature from bytes without performing subgroup checks.
    /// The subgroup check is performed in verify() by calling underlying BLST's verify function with `sig_groupcheck`
    /// parameter to true.
    pub fn deserialize(message: &[u8]) -> Result<Self, BlsError> {
        Self::uncompress(message)
    }

    pub fn compress(&self) -> Vec<u8> {
        self.0.compress().to_vec()
    }

    /// Uncompresses a signature from compressed bytes without performing subgroup checks.
    /// The subgroup check is performed in verify() by calling underlying BLST's verify function with `sig_groupcheck`
    /// parameter to true.
    pub fn uncompress(message: &[u8]) -> Result<Self, BlsError> {
        blst_core::Signature::uncompress(message)
            .map(Self)
            .map_err(BlsError)
    }
}

impl Encodable for BlsSignature {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let x: [u8; SIGNATURE_COMPRESSED_LEN] = self
            .compress()
            .try_into()
            .expect("bls signature expected to be 96 bytes");
        x.encode(out);
    }
}

impl Decodable for BlsSignature {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let raw_bytes = <[u8; SIGNATURE_COMPRESSED_LEN]>::decode(buf)?;

        match Self::uncompress(&raw_bytes) {
            Ok(sig) => Ok(sig),
            Err(_) => Err(alloy_rlp::Error::Custom("invalid bls signature")),
        }
    }
}

impl Serialize for BlsSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let hex_str = "0x".to_string() + &hex::encode(self.serialize());
        serializer.serialize_str(&hex_str)
    }
}

impl<'de> Deserialize<'de> for BlsSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = <String as Deserialize>::deserialize(deserializer)?;

        let hex_str = match buf.strip_prefix("0x") {
            Some(hex_str) => hex_str,
            None => &buf,
        };

        let bytes = hex::decode(hex_str).map_err(<D::Error as serde::de::Error>::custom)?;

        Self::deserialize(bytes.as_ref()).map_err(<D::Error as serde::de::Error>::custom)
    }
}

impl From<blst_core::AggregateSignature> for BlsAggregateSignature {
    fn from(value: blst_core::AggregateSignature) -> Self {
        Self(value)
    }
}

impl BlsAggregateSignature {
    /// The infinity point is the identity element in the group.
    /// Aggregating/adding infinity to anything is the identity function
    pub fn infinity() -> Self {
        Self::deserialize(&INFINITY_SIGNATURE).expect("Infinity BLS signature")
    }

    /// Validate that the signature is in the correct subgroup
    pub fn validate(&self) -> Result<(), BlsError> {
        self.as_signature().validate(true)
    }

    /// Aggregate a signature to self
    pub fn add_assign(&mut self, other: &BlsSignature) -> Result<(), BlsError> {
        self.0.add_signature(&other.0, false).map_err(BlsError)
    }

    /// Aggregate an aggregated signature to self
    pub fn add_assign_aggregate(&mut self, other: &Self) {
        self.0.add_aggregate(&other.0)
    }

    /// Verify the aggregate signature created over the same message. It only requires 2 pairing function calls, hence the name fast
    pub fn fast_verify<SD: SigningDomain>(
        &self,
        msg: &[u8],
        pubkey: &BlsAggregatePubKey,
    ) -> Result<(), BlsError> {
        let msg = [SD::PREFIX, msg].concat();
        let err = self
            .as_signature()
            .fast_aggregate_verify_pre_aggregated(true, &msg, DST, pubkey);
        map_err_to_result(err)
    }

    /// Verify the aggregate signature created over different messages. It
    /// requires `n+1`` pairing function calls. It is better than verifying the
    /// `n` signatures independently as it would otherwise incur `2n` pairing
    /// calls. (`n == msgs.len() == pubkeys.len()`)
    pub fn verify<SD: SigningDomain>(
        &self,
        msgs: &[&[u8]],
        pubkeys: &[&BlsAggregatePubKey],
    ) -> Result<(), BlsError> {
        let msgs_prefixed: Vec<Vec<u8>> =
            msgs.iter().map(|msg| [SD::PREFIX, msg].concat()).collect();
        let msgs: Vec<&[u8]> = msgs_prefixed.iter().map(|msg| msg.as_slice()).collect();
        let pks = pubkeys.iter().map(|pk| pk.as_pubkey()).collect::<Vec<_>>();
        let pks: Vec<&BlsPubKey> = pks.iter().collect();

        let err = self
            .as_signature()
            .aggregate_verify(true, &msgs, DST, pks.as_ref(), false);
        map_err_to_result(err)
    }

    pub fn as_signature(&self) -> BlsSignature {
        self.0.to_signature().into()
    }

    fn from_signature(sig: &BlsSignature) -> Self {
        blst_core::AggregateSignature::from_signature(&sig.0).into()
    }

    pub fn serialize(&self) -> Vec<u8> {
        self.as_signature().serialize()
    }

    pub fn deserialize(message: &[u8]) -> Result<Self, BlsError> {
        let sig = BlsSignature::deserialize(message)?;
        Ok(Self::from_signature(&sig))
    }

    pub fn compress(&self) -> Vec<u8> {
        self.as_signature().compress()
    }

    pub fn uncompress(message: &[u8]) -> Result<Self, BlsError> {
        let sig = BlsSignature::uncompress(message)?;
        Ok(Self::from_signature(&sig))
    }
}

impl PartialEq for BlsAggregateSignature {
    fn eq(&self, other: &Self) -> bool {
        self.as_signature() == other.as_signature()
    }
}

impl Eq for BlsAggregateSignature {}

impl std::hash::Hash for BlsAggregateSignature {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.as_signature(), state)
    }
}

impl Encodable for BlsAggregateSignature {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let x: [u8; SIGNATURE_COMPRESSED_LEN] = self
            .compress()
            .try_into()
            .expect("bls aggregate signature expected to be 96 bytes");
        x.encode(out);
    }
}

impl Decodable for BlsAggregateSignature {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let raw_bytes = <[u8; SIGNATURE_COMPRESSED_LEN]>::decode(buf)?;

        match Self::uncompress(&raw_bytes) {
            Ok(sig) => Ok(sig),
            Err(_) => Err(alloy_rlp::Error::Custom("invalid bls aggregate signature")),
        }
    }
}

impl Serialize for BlsAggregateSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let hex_str = "0x".to_string() + &hex::encode(self.serialize());
        serializer.serialize_str(&hex_str)
    }
}

impl<'de> Deserialize<'de> for BlsAggregateSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = <String as Deserialize>::deserialize(deserializer)?;

        let hex_str = match buf.strip_prefix("0x") {
            Some(hex_str) => hex_str,
            None => &buf,
        };

        let bytes = hex::decode(hex_str).map_err(<D::Error as serde::de::Error>::custom)?;

        Self::deserialize(bytes.as_ref()).map_err(<D::Error as serde::de::Error>::custom)
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashSet,
        panic::{catch_unwind, AssertUnwindSafe},
    };

    use monad_crypto::{signing_domain, signing_domain::SigningDomain};

    use super::{
        blst_core, BlsAggregatePubKey, BlsAggregateSignature, BlsError, BlsKeyPair, BlsPubKey,
        BlsSecretKey, BlsSignature, BLST_BAD_ENCODING, INFINITY_PUBKEY, INFINITY_SIGNATURE,
        SIGNATURE_COMPRESSED_LEN,
    };

    type SigningDomainType = signing_domain::Vote;

    fn keygen(secret: u8) -> BlsKeyPair {
        let mut secret = [secret; 32];
        BlsKeyPair::from_bytes(&mut secret).unwrap()
    }

    fn gen_keypairs(len: usize) -> Vec<BlsKeyPair> {
        assert!(len < 255);

        let mut vec = Vec::new();
        for i in 1..=len {
            let mut secret = [i as u8; 32];
            vec.push(BlsKeyPair::from_bytes(&mut secret).unwrap());
        }
        vec
    }

    // aggregate neighboring pubkeys into an aggregate pubkey
    // e.g. [1,2,3,4,5] -> [[1,2], [3,4], [5]]
    fn aggregate_pubkey_by_2<'a, T>(mut iter: T) -> Vec<BlsAggregatePubKey>
    where
        T: Iterator<Item = &'a BlsPubKey>,
    {
        let mut aggpks = Vec::new();
        while let Some(pk0) = iter.next() {
            let mut aggpk = BlsAggregatePubKey::infinity();
            aggpk.add_assign(pk0);
            if let Some(pk1) = iter.next() {
                aggpk.add_assign(pk1);
            }
            aggpks.push(aggpk);
        }
        aggpks
    }

    // same as aggregate_pubkey_by_2, but on signatures
    fn aggregate_signature_by_2<'a, T>(mut iter: T) -> Vec<BlsAggregateSignature>
    where
        T: Iterator<Item = &'a BlsSignature>,
    {
        let mut aggsigs = Vec::new();
        while let Some(sig0) = iter.next() {
            let mut aggsig = BlsAggregateSignature::infinity();
            aggsig.add_assign(sig0).unwrap();
            if let Some(sig1) = iter.next() {
                aggsig.add_assign(sig1).unwrap();
            }
            aggsigs.push(aggsig);
        }
        aggsigs
    }

    #[test]
    fn test_bad_public_key() {
        let pubkey = BlsPubKey::uncompress([0u8; 4].as_ref());
        assert_eq!(pubkey, Err(BlsError(BLST_BAD_ENCODING)));

        let pubkey = BlsPubKey::deserialize([0u8; 4].as_ref());
        assert_eq!(pubkey, Err(BlsError(BLST_BAD_ENCODING)));
    }

    #[test]
    fn test_compressed_public_key_inf() {
        let pubkey = BlsPubKey::uncompress(&INFINITY_PUBKEY);
        assert_eq!(pubkey, Err(BlsError(blst::BLST_ERROR::BLST_PK_IS_INFINITY)));
    }

    #[test]
    fn test_uncompressed_public_key_inf() {
        let infinity_pubkey_uncompressed = blst_core::PublicKey::uncompress(&INFINITY_PUBKEY)
            .unwrap()
            .serialize();
        let pubkey = BlsPubKey::deserialize(infinity_pubkey_uncompressed.as_slice());
        assert_eq!(pubkey, Err(BlsError(blst::BLST_ERROR::BLST_PK_IS_INFINITY)));
    }

    #[test]
    fn test_keygen_zeroize() {
        let mut secret = [127; 64];
        let _ = BlsSecretKey::key_gen(secret.as_mut_slice(), &[]).unwrap();
        // secret is zeroized
        assert_eq!(secret, [0_u8; 64])
    }

    #[test]
    fn test_keypair_from_bytes_zeroize() {
        let mut secret = [127; 64];
        let _ = BlsKeyPair::from_bytes(&mut secret).unwrap();
        // secret is zeroized
        assert_eq!(secret, [0_u8; 64])
    }

    #[test]
    fn test_privkey_reproducible() {
        let secret = [127; 64];
        let mut secret1 = secret;
        let mut secret2 = secret;

        let keypair1 = BlsKeyPair::from_bytes(&mut secret1).unwrap();
        let keypair2 = BlsKeyPair::from_bytes(&mut secret2).unwrap();

        assert_eq!(keypair1.pubkey(), keypair2.pubkey())
    }

    #[test]
    fn test_pubkey_roundtrip() {
        let keypair = keygen(7);
        let pubkey = keypair.pubkey();

        let pubkey_bytes = pubkey.serialize();

        assert_eq!(
            pubkey_bytes,
            BlsPubKey::deserialize(pubkey_bytes.as_ref())
                .unwrap()
                .serialize()
        )
    }

    #[test]
    fn test_pubkey_roundtrip_compressed() {
        let keypair = keygen(7);
        let pubkey = keypair.pubkey();

        let pubkey_compressed = pubkey.compress();

        assert_eq!(
            pubkey_compressed,
            BlsPubKey::uncompress(pubkey_compressed.as_ref())
                .unwrap()
                .compress()
        )
    }

    #[test]
    fn test_signature_serdes_roundtrip() {
        let keypair = keygen(7);
        let msg = keypair.pubkey().serialize();
        let sig = BlsSignature::sign::<SigningDomainType>(msg.as_ref(), &keypair);

        let sig_rlp = alloy_rlp::encode(sig);
        let x: BlsSignature = alloy_rlp::decode_exact(sig_rlp).unwrap();

        assert_eq!(sig, x);
    }

    #[test]
    fn test_aggregate_pubkey_roundtrip() {
        let keypair = keygen(7);
        let pubkey = keypair.pubkey();
        let agg_pk = BlsAggregatePubKey::from_pubkey(&pubkey);

        let agg_pk_compressed = agg_pk.serialize();
        assert_eq!(
            agg_pk_compressed,
            BlsAggregatePubKey::deserialize(agg_pk_compressed.as_ref())
                .unwrap()
                .serialize()
        )
    }

    #[test]
    fn test_aggregate_pubkey_roundtrip_compressed() {
        let keypair = keygen(7);
        let pubkey = keypair.pubkey();
        let agg_pk = BlsAggregatePubKey::from_pubkey(&pubkey);

        let agg_pk_compressed = agg_pk.compress();
        assert_eq!(
            agg_pk_compressed,
            BlsAggregatePubKey::uncompress(agg_pk_compressed.as_ref())
                .unwrap()
                .compress()
        )
    }

    #[test]
    fn test_signature_group_check() {
        let not_in_subgroup_bytes: [u8; 96] = [
            0xac, 0xb0, 0x12, 0x4c, 0x75, 0x74, 0xf2, 0x81, 0xa2, 0x93, 0xf4, 0x18, 0x5c, 0xad,
            0x3c, 0xb2, 0x26, 0x81, 0xd5, 0x20, 0x91, 0x7c, 0xe4, 0x66, 0x65, 0x24, 0x3e, 0xac,
            0xb0, 0x51, 0x00, 0x0d, 0x8b, 0xac, 0xf7, 0x5e, 0x14, 0x51, 0x87, 0x0c, 0xa6, 0xb3,
            0xb9, 0xe6, 0xc9, 0xd4, 0x1a, 0x7b, 0x02, 0xea, 0xd2, 0x68, 0x5a, 0x84, 0x18, 0x8a,
            0x4f, 0xaf, 0xd3, 0x82, 0x5d, 0xaf, 0x6a, 0x98, 0x96, 0x25, 0xd7, 0x19, 0xcc, 0xd2,
            0xd8, 0x3a, 0x40, 0x10, 0x1f, 0x4a, 0x45, 0x3f, 0xca, 0x62, 0x87, 0x8c, 0x89, 0x0e,
            0xca, 0x62, 0x23, 0x63, 0xf9, 0xdd, 0xb8, 0xf3, 0x67, 0xa9, 0x1e, 0x84,
        ];

        let sig = BlsSignature::uncompress(&not_in_subgroup_bytes).unwrap();
        assert_eq!(
            BlsError(blst::BLST_ERROR::BLST_POINT_NOT_IN_GROUP),
            sig.validate(false).unwrap_err()
        );
    }

    #[test]
    fn test_signature_roundtrip() {
        let keypair = keygen(7);
        let msg = keypair.pubkey().serialize();

        let sig = BlsSignature::sign::<SigningDomainType>(msg.as_ref(), &keypair);

        let sig_bytes = sig.serialize();
        assert_eq!(
            sig_bytes,
            BlsSignature::deserialize(sig_bytes.as_ref())
                .unwrap()
                .serialize()
        );
    }

    #[test]
    fn test_signature_roundtrip_compressed() {
        let keypair = keygen(7);
        let msg = keypair.pubkey().serialize();

        let sig = BlsSignature::sign::<SigningDomainType>(msg.as_ref(), &keypair);

        let sig_bytes = sig.compress();
        assert_eq!(
            sig_bytes,
            BlsSignature::uncompress(sig_bytes.as_ref())
                .unwrap()
                .compress()
        );
    }

    #[test]
    fn test_aggregate_signature_roundtrip() {
        let keypairs = gen_keypairs(2);
        let msg = b"hello world";
        let mut aggsig = BlsAggregateSignature::infinity();
        for kp in keypairs.iter() {
            aggsig
                .add_assign(&kp.sign::<SigningDomainType>(msg))
                .unwrap();
        }

        let aggsig_bytes = aggsig.serialize();

        assert_eq!(
            aggsig_bytes,
            BlsAggregateSignature::deserialize(aggsig_bytes.as_ref())
                .unwrap()
                .serialize()
        )
    }

    #[test]
    fn test_aggregate_signature_serdes_roundtrip() {
        let keypairs = gen_keypairs(2);
        let msg = b"hello world";
        let mut aggsig = BlsAggregateSignature::infinity();
        for kp in keypairs.iter() {
            aggsig
                .add_assign(&kp.sign::<SigningDomainType>(msg))
                .unwrap();
        }

        let aggsig_rlp = alloy_rlp::encode(aggsig);
        let x: BlsAggregateSignature = alloy_rlp::decode_exact(aggsig_rlp).unwrap();

        assert_eq!(aggsig, x);
    }

    #[test]
    fn test_aggregate_signature_roundtrip_compressed() {
        let keypairs = gen_keypairs(2);
        let msg = b"hello world";
        let mut aggsig = BlsAggregateSignature::infinity();
        for kp in keypairs.iter() {
            aggsig
                .add_assign(&kp.sign::<SigningDomainType>(msg))
                .unwrap();
        }

        let aggsig_bytes = aggsig.compress();

        assert_eq!(
            aggsig_bytes,
            BlsAggregateSignature::uncompress(aggsig_bytes.as_ref())
                .unwrap()
                .compress()
        )
    }

    #[test]
    fn test_hashing() {
        let mut pkhs = HashSet::new();
        let mut sighs = HashSet::new();

        let keypair = gen_keypairs(10);
        let pks = keypair.iter().map(|kp| kp.pubkey()).collect::<Vec<_>>();
        let pks_ref = pks.iter().collect::<Vec<_>>();
        let aggpk = BlsAggregatePubKey::aggregate(&pks_ref).unwrap();
        assert!(pkhs.insert(aggpk));
        assert!(!pkhs.insert(aggpk));

        let sigs = keypair
            .iter()
            .map(|kp| kp.sign::<SigningDomainType>(&kp.pubkey().serialize()))
            .collect::<Vec<_>>();
        let mut aggsig = BlsAggregateSignature::infinity();
        for sig in sigs.iter() {
            aggsig.add_assign(sig).unwrap();
        }
        assert!(sighs.insert(aggsig));
        assert!(!sighs.insert(aggsig));
    }

    #[test]
    fn test_infinity_aggpk() {
        let aggpk = BlsAggregatePubKey::infinity();

        let result = aggpk.validate();

        assert_eq!(result, Err(BlsError(blst::BLST_ERROR::BLST_PK_IS_INFINITY)));
    }

    #[test]
    fn test_aggpk_aggregate_commutative() {
        let keypairs = gen_keypairs(3);

        let pks: Vec<_> = keypairs.into_iter().map(|kp| kp.pubkey()).collect();
        let pks1: Vec<_> = pks.iter().collect();

        let agg1 = BlsAggregatePubKey::aggregate(&pks1);

        let pks2: Vec<_> = pks.iter().rev().collect();
        let agg2 = BlsAggregatePubKey::aggregate(&pks2);

        assert_eq!(agg1, agg2)
    }

    #[test]
    fn test_aggpk_add_assign_commutative() {
        let keypairs = gen_keypairs(3);

        let pks: Vec<_> = keypairs.into_iter().map(|kp| kp.pubkey()).collect();
        let mut agg1 = BlsAggregatePubKey::infinity();
        for pk in pks.iter() {
            agg1.add_assign(pk);
        }

        let mut agg2 = BlsAggregatePubKey::infinity();
        for pk in pks.iter().rev() {
            agg2.add_assign(pk);
        }

        assert_eq!(agg1, agg2)
    }

    #[test]
    fn test_aggpk_add_assign_aggregate_commutative() {
        let keypairs = gen_keypairs(7);

        let pks: Vec<_> = keypairs.into_iter().map(|kp| kp.pubkey()).collect();
        let aggv1 = aggregate_pubkey_by_2(pks.iter());

        let mut aggpk1 = BlsAggregatePubKey::infinity();
        for pk in aggv1.iter() {
            aggpk1.add_assign_aggregate(pk);
        }

        let aggv2 = aggregate_pubkey_by_2(pks.iter().rev());

        let mut aggpk2 = BlsAggregatePubKey::infinity();
        for pk in aggv2.iter() {
            aggpk2.add_assign_aggregate(pk);
        }

        assert_ne!(aggv1, aggv2);
        assert_eq!(aggpk1, aggpk2);
    }

    #[test]
    fn test_aggpk_aggregation_methods_equivalent() {
        let keypairs = gen_keypairs(4);
        let pks: Vec<_> = keypairs.into_iter().map(|kp| kp.pubkey()).collect();
        let pks_ref: Vec<_> = pks.iter().collect();

        // aggregate
        let pk_agg = BlsAggregatePubKey::aggregate(&pks_ref).unwrap();

        // add_assign
        let mut pk_add_assign = BlsAggregatePubKey::infinity();
        for pk in pks_ref.iter() {
            pk_add_assign.add_assign(pk);
        }

        // add_assign_aggregate
        let mut pk_add_assign_agg = BlsAggregatePubKey::infinity();
        let aggv = aggregate_pubkey_by_2(pks_ref.into_iter());

        for pk in aggv.iter() {
            pk_add_assign_agg.add_assign_aggregate(pk);
        }

        assert_eq!(pk_agg, pk_add_assign);
        assert_eq!(pk_agg, pk_add_assign_agg);
    }

    #[test]
    fn test_sig_verify() {
        let keypair = keygen(7);
        let pubkey = keypair.pubkey();

        let msg = b"hello world";

        let sig = keypair.sign::<SigningDomainType>(msg);
        assert!(sig.verify::<SigningDomainType>(msg, &pubkey).is_ok());
    }

    #[test]
    fn test_infinity_aggsig() {
        let signature = BlsAggregateSignature::infinity();
        let validate_result = signature.validate();

        assert_eq!(
            validate_result,
            Err(BlsError(blst::BLST_ERROR::BLST_PK_IS_INFINITY))
        );
    }

    #[test]
    fn test_aggsig_single_msg_verify() {
        let keypairs = gen_keypairs(3);
        let pks: Vec<_> = keypairs.iter().map(|kp| kp.pubkey()).collect();
        let pks_ref: Vec<_> = pks.iter().collect();

        let agg_pk = BlsAggregatePubKey::aggregate(&pks_ref).unwrap();

        let msg = b"hello world";
        let mut sig = BlsAggregateSignature::infinity();

        for kp in keypairs.iter() {
            sig.add_assign(&kp.sign::<SigningDomainType>(msg)).unwrap();
        }

        assert!(sig.fast_verify::<SigningDomainType>(msg, &agg_pk).is_ok())
    }

    #[test]
    fn test_aggsig_single_msg_verify_fail() {
        let keypairs = gen_keypairs(3);
        let pks: Vec<_> = keypairs.iter().map(|kp| kp.pubkey()).collect();
        let pks_ref: Vec<_> = pks.iter().collect();

        let agg_pk = BlsAggregatePubKey::aggregate(&pks_ref).unwrap();

        let msg = b"hello world";
        let mut sig = BlsAggregateSignature::infinity();

        for kp in keypairs[0..=1].iter() {
            sig.add_assign(&kp.sign::<SigningDomainType>(msg)).unwrap();
        }

        let msg2 = b"bye world";
        sig.add_assign(&keypairs[2].sign::<SigningDomainType>(msg2))
            .unwrap();

        assert_eq!(
            sig.fast_verify::<SigningDomainType>(msg, &agg_pk),
            Err(BlsError(blst::BLST_ERROR::BLST_VERIFY_FAIL))
        )
    }

    #[test]
    fn test_aggsig_multi_msg_verify() {
        let keypairs = gen_keypairs(3);
        let pks: Vec<_> = keypairs
            .iter()
            .map(|kp| BlsAggregatePubKey::from_pubkey(&kp.pubkey()))
            .collect();
        let pks_ref: Vec<_> = pks.iter().collect();

        let msgs: Vec<_> = pks.iter().map(|pk| pk.serialize()).collect();
        let msgs_ref: Vec<&[u8]> = msgs.iter().map(|m| m.as_ref()).collect();

        let mut aggsig = BlsAggregateSignature::infinity();
        for kp in keypairs.iter() {
            let msg = kp.pubkey().serialize();
            let sig = kp.sign::<SigningDomainType>(&msg);
            aggsig.add_assign(&sig).unwrap();
        }

        assert!(aggsig
            .verify::<SigningDomainType>(&msgs_ref, &pks_ref)
            .is_ok());
    }

    #[test]
    fn test_aggsig_multi_msg_verify_fail() {
        let keypairs = gen_keypairs(3);
        let pks: Vec<_> = keypairs
            .iter()
            .map(|kp| BlsAggregatePubKey::from_pubkey(&kp.pubkey()))
            .collect();
        let pks_ref: Vec<_> = pks.iter().collect();

        let msgs: Vec<_> = pks.iter().map(|pk| pk.serialize()).collect();
        let msgs_ref: Vec<&[u8]> = msgs.iter().map(|m| m.as_ref()).collect();

        let mut aggsig = BlsAggregateSignature::infinity();
        for kp in keypairs.iter() {
            let mut msg = kp.pubkey().serialize();
            // change msg to sign
            msg[0] = 0xff;
            let sig = kp.sign::<SigningDomainType>(&msg);
            aggsig.add_assign(&sig).unwrap();
        }

        assert_eq!(
            aggsig.verify::<SigningDomainType>(&msgs_ref, &pks_ref),
            Err(BlsError(blst::BLST_ERROR::BLST_VERIFY_FAIL))
        );
    }

    #[test]
    fn test_aggsig_add_assign_commutative() {
        let keypairs = gen_keypairs(7);
        let pks: Vec<_> = keypairs.iter().map(|kp| kp.pubkey()).collect();
        let pks_ref: Vec<_> = pks.iter().collect();
        let agg_pk = BlsAggregatePubKey::aggregate(&pks_ref).unwrap();

        let msg = b"hello world";
        let mut sig1 = BlsAggregateSignature::infinity();
        for kp in keypairs.iter() {
            sig1.add_assign(&kp.sign::<SigningDomainType>(msg)).unwrap();
        }

        let mut sig2 = BlsAggregateSignature::infinity();
        for kp in keypairs.iter().rev() {
            sig2.add_assign(&kp.sign::<SigningDomainType>(msg)).unwrap();
        }

        assert!(sig1.fast_verify::<SigningDomainType>(msg, &agg_pk).is_ok());
        assert!(sig2.fast_verify::<SigningDomainType>(msg, &agg_pk).is_ok());
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_aggsig_add_assign_aggregate_commutative() {
        let keypairs = gen_keypairs(7);
        let pks: Vec<_> = keypairs.iter().map(|kp| kp.pubkey()).collect();
        let pks_ref: Vec<_> = pks.iter().collect();
        let agg_pk = BlsAggregatePubKey::aggregate(&pks_ref).unwrap();

        let msg = b"hello world";
        let mut sig1 = BlsAggregateSignature::infinity();
        let mut sig2 = BlsAggregateSignature::infinity();
        let mut sigs = Vec::new();

        for kp in keypairs.iter() {
            sigs.push(kp.sign::<SigningDomainType>(msg));
        }

        let aggsigv1 = aggregate_signature_by_2(sigs.iter());
        for aggsig in aggsigv1.iter() {
            sig1.add_assign_aggregate(aggsig);
        }

        let aggsigv2 = aggregate_signature_by_2(sigs.iter().rev());
        for aggsig in aggsigv2.iter() {
            sig2.add_assign_aggregate(aggsig);
        }

        assert!(sig1.fast_verify::<SigningDomainType>(msg, &agg_pk).is_ok());
        assert!(sig2.fast_verify::<SigningDomainType>(msg, &agg_pk).is_ok());
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_aggsig_aggregation_methods_equivalent() {
        let keypairs = gen_keypairs(7);
        let pks: Vec<_> = keypairs.iter().map(|kp| kp.pubkey()).collect();
        let pks_ref: Vec<_> = pks.iter().collect();
        let agg_pk = BlsAggregatePubKey::aggregate(&pks_ref).unwrap();

        let msg = b"hello world";
        let mut sig1 = BlsAggregateSignature::infinity();
        let mut sig2 = BlsAggregateSignature::infinity();
        let mut sigs = Vec::new();

        for kp in keypairs.iter() {
            sigs.push(kp.sign::<SigningDomainType>(msg));
        }

        for sig in sigs.iter() {
            sig1.add_assign(sig).unwrap();
        }

        let aggsigv = aggregate_signature_by_2(sigs.iter());
        for aggsig in aggsigv.iter() {
            sig2.add_assign_aggregate(aggsig);
        }

        assert!(sig1.fast_verify::<SigningDomainType>(msg, &agg_pk).is_ok());
        assert!(sig2.fast_verify::<SigningDomainType>(msg, &agg_pk).is_ok());
        assert_eq!(sig1, sig2);
    }

    struct DomainA;
    impl SigningDomain for DomainA {
        const PREFIX: &'static [u8] = b"test-domain-a:";
    }

    struct DomainB;
    impl SigningDomain for DomainB {
        const PREFIX: &'static [u8] = b"test-domain-b:";
    }

    #[test]
    fn test_sign_is_deterministic_for_same_key_and_message() {
        let kp = keygen(7);
        let msg = b"determinism";

        let s1 = kp.sign::<SigningDomainType>(msg).serialize();
        let s2 = kp.sign::<SigningDomainType>(msg).serialize();

        assert_eq!(s1, s2);
    }

    #[test]
    fn test_sig_verify_fails_with_wrong_pubkey() {
        let kp1 = keygen(7);
        let kp2 = keygen(8);

        let msg = b"hello world";
        let sig = kp1.sign::<SigningDomainType>(msg);

        assert_eq!(
            sig.verify::<SigningDomainType>(msg, &kp2.pubkey()),
            Err(BlsError(blst::BLST_ERROR::BLST_VERIFY_FAIL))
        );
    }

    #[test]
    fn test_sig_verify_fails_with_wrong_message() {
        let kp = keygen(7);
        let msg1 = b"msg1";
        let msg2 = b"msg2";

        let sig = kp.sign::<SigningDomainType>(msg1);

        assert_eq!(
            sig.verify::<SigningDomainType>(msg2, &kp.pubkey()),
            Err(BlsError(blst::BLST_ERROR::BLST_VERIFY_FAIL))
        );
    }

    #[test]
    fn test_domain_separation_changes_signature_and_breaks_cross_domain_verify() {
        let kp = keygen(7);
        let msg = b"same message";

        let sig_a = kp.sign::<DomainA>(msg);
        let sig_b = kp.sign::<DomainB>(msg);

        // Same key+msg but different domain => different signature bytes.
        assert_ne!(sig_a.serialize(), sig_b.serialize());

        // Verify must be domain-consistent.
        assert!(sig_a.verify::<DomainA>(msg, &kp.pubkey()).is_ok());
        assert_eq!(
            sig_a.verify::<DomainB>(msg, &kp.pubkey()),
            Err(BlsError(blst::BLST_ERROR::BLST_VERIFY_FAIL))
        );
    }

    #[test]
    fn test_keygen_error_path_still_zeroizes_ikm() {
        // Spec requires >= 32 bytes IKM; ensure we error AND still zeroize.
        let mut short_ikm = [0xABu8; 31];
        let res = BlsSecretKey::key_gen(short_ikm.as_mut_slice(), &[]);
        assert!(res.is_err());
        assert_eq!(short_ikm, [0u8; 31]);
    }

    #[test]
    fn test_signature_deserialize_rejects_wrong_lengths() {
        let too_short = vec![0u8; SIGNATURE_COMPRESSED_LEN - 1];
        assert_eq!(
            BlsSignature::deserialize(&too_short),
            Err(BlsError(BLST_BAD_ENCODING))
        );

        let too_long = vec![0u8; SIGNATURE_COMPRESSED_LEN + 1];
        assert_eq!(
            BlsSignature::deserialize(&too_long),
            Err(BlsError(BLST_BAD_ENCODING))
        );
    }

    #[test]
    fn test_aggregate_signature_deserialize_rejects_wrong_lengths() {
        let too_short = vec![0u8; SIGNATURE_COMPRESSED_LEN - 1];
        assert_eq!(
            BlsAggregateSignature::deserialize(&too_short),
            Err(BlsError(BLST_BAD_ENCODING))
        );

        let too_long = vec![0u8; SIGNATURE_COMPRESSED_LEN + 1];
        assert_eq!(
            BlsAggregateSignature::deserialize(&too_long),
            Err(BlsError(BLST_BAD_ENCODING))
        );
    }

    #[test]
    fn test_aggsig_verify_length_mismatch_does_not_panic() {
        // Robustness: calling verify() with mismatched slice lengths should not unwind.
        let keypairs = gen_keypairs(3);

        let pks: Vec<_> = keypairs
            .iter()
            .map(|kp| BlsAggregatePubKey::from_pubkey(&kp.pubkey()))
            .collect();
        let pks_ref: Vec<_> = pks.iter().collect();

        let msgs: Vec<_> = keypairs.iter().map(|kp| kp.pubkey().serialize()).collect();
        let msgs_ref: Vec<&[u8]> = msgs.iter().map(|m| m.as_slice()).collect();

        let mut aggsig = BlsAggregateSignature::infinity();
        for (kp, msg) in keypairs.iter().zip(msgs_ref.iter()) {
            aggsig
                .add_assign(&kp.sign::<SigningDomainType>(msg))
                .unwrap();
        }

        let res = catch_unwind(AssertUnwindSafe(|| {
            aggsig.verify::<SigningDomainType>(&msgs_ref[..2], &pks_ref[..3])
        }));

        assert!(res.is_ok());
        assert!(res.unwrap().is_err());
    }

    #[test]
    fn test_signature_infinity_is_rejected() {
        let sig = BlsSignature::uncompress(&INFINITY_SIGNATURE).unwrap();
        assert!(sig.validate(true).is_err());
    }

    #[test]
    fn test_ethereum_bls12_381_minpk() {
        // A test suite from https://github.com/ethereum/bls12-381-tests used in Ethereum 2.0 BLS Signature APIs,
        // as well as common extensions such as signature-sets (batch aggregate verification) and serialization.

        run_ethereum_bls12_381_aggregate();
        run_ethereum_bls12_381_aggregate_verify();
        run_ethereum_bls12_381_batch_verify();
        run_ethereum_bls12_381_deserialization_g1();
        run_ethereum_bls12_381_deserialization_g2();
        run_ethereum_bls12_381_fast_aggregate_verify();
        run_ethereum_bls12_381_sign();
        run_ethereum_bls12_381_verify()
    }

    fn run_ethereum_bls12_381_aggregate() {
        // Test: aggregate_0x0000000000000000000000000000000000000000000000000000000000000000.json
        {
            let sigs_bytes = vec![
                hex::decode("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380b55285a55").unwrap(),
                hex::decode("b23c46be3a001c63ca711f87a005c200cc550b9429d5f4eb38d74322144f1b63926da3388979e5321012fb1a0526bcd100b5ef5fe72628ce4cd5e904aeaa3279527843fae5ca9ca675f4f51ed8f83bbf7155da9ecc9663100a885d5dc6df96d9").unwrap(),
                hex::decode("948a7cb99f76d616c2c564ce9bf4a519f1bea6b0a624a02276443c245854219fabb8d4ce061d255af5330b078d5380681751aa7053da2c98bae898edc218c75f07e24d8802a17cd1f6833b71e58f5eb5b94208b4d0bb3848cecb075ea21be115").unwrap(),
            ];

            let mut agg_sig = BlsAggregateSignature::infinity();
            for sig_bytes in &sigs_bytes {
                let sig = BlsSignature::uncompress(sig_bytes).unwrap();
                agg_sig.add_assign(&sig).unwrap();
            }

            let expected_bytes = hex::decode("9683b3e6701f9a4b706709577963110043af78a5b41991b998475a3d3fd62abf35ce03b33908418efc95a058494a8ae504354b9f626231f6b3f3c849dfdeaf5017c4780e2aee1850ceaf4b4d9ce70971a3d2cfcd97b7e5ecf6759f8da5f76d31").unwrap();
            let expected_sig = BlsAggregateSignature::deserialize(&expected_bytes).unwrap();
            assert_eq!(agg_sig, expected_sig);
        }

        // Test: aggregate_0x5656565656565656565656565656565656565656565656565656565656565656.json
        {
            let sigs_bytes = vec![
                hex::decode("882730e5d03f6b42c3abc26d3372625034e1d871b65a8a6b900a56dae22da98abbe1b68f85e49fe7652a55ec3d0591c20767677e33e5cbb1207315c41a9ac03be39c2e7668edc043d6cb1d9fd93033caa8a1c5b0e84bedaeb6c64972503a43eb").unwrap(),
                hex::decode("af1390c3c47acdb37131a51216da683c509fce0e954328a59f93aebda7e4ff974ba208d9a4a2a2389f892a9d418d618418dd7f7a6bc7aa0da999a9d3a5b815bc085e14fd001f6a1948768a3f4afefc8b8240dda329f984cb345c6363272ba4fe").unwrap(),
                hex::decode("a4efa926610b8bd1c8330c918b7a5e9bf374e53435ef8b7ec186abf62e1b1f65aeaaeb365677ac1d1172a1f5b44b4e6d022c252c58486c0a759fbdc7de15a756acc4d343064035667a594b4c2a6f0b0b421975977f297dba63ee2f63ffe47bb6").unwrap(),
            ];

            let mut agg_sig = BlsAggregateSignature::infinity();
            for sig_bytes in &sigs_bytes {
                let sig = BlsSignature::uncompress(sig_bytes).unwrap();
                agg_sig.add_assign(&sig).unwrap();
            }

            let expected_bytes = hex::decode("ad38fc73846583b08d110d16ab1d026c6ea77ac2071e8ae832f56ac0cbcdeb9f5678ba5ce42bd8dce334cc47b5abcba40a58f7f1f80ab304193eb98836cc14d8183ec14cc77de0f80c4ffd49e168927a968b5cdaa4cf46b9805be84ad7efa77b").unwrap();
            let expected_agg_sig = BlsAggregateSignature::deserialize(&expected_bytes).unwrap();
            assert_eq!(agg_sig, expected_agg_sig);
        }

        // Test: aggregate_0xabababababababababababababababababababababababababababababababab.json
        {
            let sigs_bytes = vec![
                hex::decode("91347bccf740d859038fcdcaf233eeceb2a436bcaaee9b2aa3bfb70efe29dfb2677562ccbea1c8e061fb9971b0753c240622fab78489ce96768259fc01360346da5b9f579e5da0d941e4c6ba18a0e64906082375394f337fa1af2b7127b0d121").unwrap(),
                hex::decode("9674e2228034527f4c083206032b020310face156d4a4685e2fcaec2f6f3665aa635d90347b6ce124eb879266b1e801d185de36a0a289b85e9039662634f2eea1e02e670bc7ab849d006a70b2f93b84597558a05b879c8d445f387a5d5b653df").unwrap(),
                hex::decode("ae82747ddeefe4fd64cf9cedb9b04ae3e8a43420cd255e3c7cd06a8d88b7c7f8638543719981c5d16fa3527c468c25f0026704a6951bde891360c7e8d12ddee0559004ccdbe6046b55bae1b257ee97f7cdb955773d7cf29adf3ccbb9975e4eb9").unwrap(),
            ];

            let mut agg_sig = BlsAggregateSignature::infinity();
            for sig_bytes in &sigs_bytes {
                let sig = BlsSignature::uncompress(sig_bytes).unwrap();
                agg_sig.add_assign(&sig).unwrap();
            }

            let expected_bytes = hex::decode("9712c3edd73a209c742b8250759db12549b3eaf43b5ca61376d9f30e2747dbcf842d8b2ac0901d2a093713e20284a7670fcf6954e9ab93de991bb9b313e664785a075fc285806fa5224c82bde146561b446ccfc706a64b8579513cfc4ff1d930").unwrap();
            let expected_agg_sig = BlsAggregateSignature::deserialize(&expected_bytes).unwrap();
            assert_eq!(agg_sig, expected_agg_sig);
        }

        // Test: aggregate_infinity_signature.json
        {
            let sigs_bytes = vec![
                hex::decode("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap(),
            ];

            let mut agg_sig = BlsAggregateSignature::infinity();
            for sig_bytes in &sigs_bytes {
                let sig = BlsSignature::uncompress(sig_bytes).unwrap();
                agg_sig.add_assign(&sig).unwrap();
            }

            let expected_bytes = hex::decode("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected_agg_sig = BlsAggregateSignature::deserialize(&expected_bytes).unwrap();
            assert_eq!(agg_sig, expected_agg_sig);
        }

        // Test: aggregate_na_signatures.json (empty array)
        {
            let sigs_bytes: Vec<Vec<u8>> = vec![];
            // When input is empty, we expect output to be null, so we skip aggregation
            assert!(sigs_bytes.is_empty());
        }

        // Test: aggregate_single_signature.json
        {
            let sigs_bytes = vec![
                hex::decode("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380b55285a55").unwrap(),
            ];

            let mut agg_sig = BlsAggregateSignature::infinity();
            for sig_bytes in &sigs_bytes {
                let sig = BlsSignature::uncompress(sig_bytes).unwrap();
                agg_sig.add_assign(&sig).unwrap();
            }

            let expected_bytes = hex::decode("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380b55285a55").unwrap();
            let expected_agg_sig = BlsAggregateSignature::deserialize(&expected_bytes).unwrap();
            assert_eq!(agg_sig, expected_agg_sig);
        }
    }

    fn run_ethereum_bls12_381_verify() {
        struct NoPrefix;
        impl SigningDomain for NoPrefix {
            const PREFIX: &'static [u8] = b"";
        }

        // Test: verify_valid_case_195246ee3bd3b6ec.json
        {
            let pubkey = hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap();
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("ae82747ddeefe4fd64cf9cedb9b04ae3e8a43420cd255e3c7cd06a8d88b7c7f8638543719981c5d16fa3527c468c25f0026704a6951bde891360c7e8d12ddee0559004ccdbe6046b55bae1b257ee97f7cdb955773d7cf29adf3ccbb9975e4eb9").unwrap();
            let expected = true;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_valid_case_2ea479adf8c40300.json
        {
            let pubkey = hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap();
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let signature = hex::decode("882730e5d03f6b42c3abc26d3372625034e1d871b65a8a6b900a56dae22da98abbe1b68f85e49fe7652a55ec3d0591c20767677e33e5cbb1207315c41a9ac03be39c2e7668edc043d6cb1d9fd93033caa8a1c5b0e84bedaeb6c64972503a43eb").unwrap();
            let expected = true;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_valid_case_2f09d443ab8a3ac2.json
        {
            let pubkey = hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap();
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let signature = hex::decode("b23c46be3a001c63ca711f87a005c200cc550b9429d5f4eb38d74322144f1b63926da3388979e5321012fb1a0526bcd100b5ef5fe72628ce4cd5e904aeaa3279527843fae5ca9ca675f4f51ed8f83bbf7155da9ecc9663100a885d5dc6df96d9").unwrap();
            let expected = true;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_valid_case_3208262581c8fc09.json
        {
            let pubkey = hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap();
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let signature = hex::decode("af1390c3c47acdb37131a51216da683c509fce0e954328a59f93aebda7e4ff974ba208d9a4a2a2389f892a9d418d618418dd7f7a6bc7aa0da999a9d3a5b815bc085e14fd001f6a1948768a3f4afefc8b8240dda329f984cb345c6363272ba4fe").unwrap();
            let expected = true;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_valid_case_6b3b17f6962a490c.json
        {
            let pubkey = hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap();
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let signature = hex::decode("a4efa926610b8bd1c8330c918b7a5e9bf374e53435ef8b7ec186abf62e1b1f65aeaaeb365677ac1d1172a1f5b44b4e6d022c252c58486c0a759fbdc7de15a756acc4d343064035667a594b4c2a6f0b0b421975977f297dba63ee2f63ffe47bb6").unwrap();
            let expected = true;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_valid_case_6eeb7c52dfd9baf0.json
        {
            let pubkey = hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap();
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("9674e2228034527f4c083206032b020310face156d4a4685e2fcaec2f6f3665aa635d90347b6ce124eb879266b1e801d185de36a0a289b85e9039662634f2eea1e02e670bc7ab849d006a70b2f93b84597558a05b879c8d445f387a5d5b653df").unwrap();
            let expected = true;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_valid_case_8761a0b7e920c323.json
        {
            let pubkey = hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap();
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("91347bccf740d859038fcdcaf233eeceb2a436bcaaee9b2aa3bfb70efe29dfb2677562ccbea1c8e061fb9971b0753c240622fab78489ce96768259fc01360346da5b9f579e5da0d941e4c6ba18a0e64906082375394f337fa1af2b7127b0d121").unwrap();
            let expected = true;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_valid_case_d34885d766d5f705.json
        {
            let pubkey = hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap();
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let signature = hex::decode("948a7cb99f76d616c2c564ce9bf4a519f1bea6b0a624a02276443c245854219fabb8d4ce061d255af5330b078d5380681751aa7053da2c98bae898edc218c75f07e24d8802a17cd1f6833b71e58f5eb5b94208b4d0bb3848cecb075ea21be115").unwrap();
            let expected = true;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_valid_case_e8a50c445c855360.json
        {
            let pubkey = hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap();
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let signature = hex::decode("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380b55285a55").unwrap();
            let expected = true;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verifycase_one_privkey_47117849458281be.json
        {
            let pubkey = hex::decode("97f1d3a73197d7942695638c4fa9ac0fc3688c4f9774b905a14e3a3f171bac586c55e83ff97a1aeffb3af00adb22c6bb").unwrap();
            let message =
                hex::decode("1212121212121212121212121212121212121212121212121212121212121212")
                    .unwrap();
            let signature = hex::decode("a42ae16f1c2a5fa69c04cb5998d2add790764ce8dd45bf25b29b4700829232052b52352dcff1cf255b3a7810ad7269601810f03b2bc8b68cf289cf295b206770605a190b6842583e47c3d1c0f73c54907bfb2a602157d46a4353a20283018763").unwrap();
            let expected = true;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_wrong_pubkey_case_195246ee3bd3b6ec.json
        {
            let pubkey = hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap();
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("9674e2228034527f4c083206032b020310face156d4a4685e2fcaec2f6f3665aa635d90347b6ce124eb879266b1e801d185de36a0a289b85e9039662634f2eea1e02e670bc7ab849d006a70b2f93b84597558a05b879c8d445f387a5d5b653df").unwrap();
            let expected = false;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_wrong_pubkey_case_2ea479adf8c40300.json
        {
            let pubkey = hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap();
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let signature = hex::decode("a4efa926610b8bd1c8330c918b7a5e9bf374e53435ef8b7ec186abf62e1b1f65aeaaeb365677ac1d1172a1f5b44b4e6d022c252c58486c0a759fbdc7de15a756acc4d343064035667a594b4c2a6f0b0b421975977f297dba63ee2f63ffe47bb6").unwrap();
            let expected = false;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_wrong_pubkey_case_2f09d443ab8a3ac2.json
        {
            let pubkey = hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap();
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let signature = hex::decode("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380b55285a55").unwrap();
            let expected = false;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_wrong_pubkey_case_3208262581c8fc09.json
        {
            let pubkey = hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap();
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let signature = hex::decode("882730e5d03f6b42c3abc26d3372625034e1d871b65a8a6b900a56dae22da98abbe1b68f85e49fe7652a55ec3d0591c20767677e33e5cbb1207315c41a9ac03be39c2e7668edc043d6cb1d9fd93033caa8a1c5b0e84bedaeb6c64972503a43eb").unwrap();
            let expected = false;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_wrong_pubkey_case_6b3b17f6962a490c.json
        {
            let pubkey = hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap();
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let signature = hex::decode("af1390c3c47acdb37131a51216da683c509fce0e954328a59f93aebda7e4ff974ba208d9a4a2a2389f892a9d418d618418dd7f7a6bc7aa0da999a9d3a5b815bc085e14fd001f6a1948768a3f4afefc8b8240dda329f984cb345c6363272ba4fe").unwrap();
            let expected = false;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_wrong_pubkey_case_6eeb7c52dfd9baf0.json
        {
            let pubkey = hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap();
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("91347bccf740d859038fcdcaf233eeceb2a436bcaaee9b2aa3bfb70efe29dfb2677562ccbea1c8e061fb9971b0753c240622fab78489ce96768259fc01360346da5b9f579e5da0d941e4c6ba18a0e64906082375394f337fa1af2b7127b0d121").unwrap();
            let expected = false;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_wrong_pubkey_case_8761a0b7e920c323.json
        {
            let pubkey = hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap();
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("ae82747ddeefe4fd64cf9cedb9b04ae3e8a43420cd255e3c7cd06a8d88b7c7f8638543719981c5d16fa3527c468c25f0026704a6951bde891360c7e8d12ddee0559004ccdbe6046b55bae1b257ee97f7cdb955773d7cf29adf3ccbb9975e4eb9").unwrap();
            let expected = false;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_wrong_pubkey_case_d34885d766d5f705.json
        {
            let pubkey = hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap();
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let signature = hex::decode("b23c46be3a001c63ca711f87a005c200cc550b9429d5f4eb38d74322144f1b63926da3388979e5321012fb1a0526bcd100b5ef5fe72628ce4cd5e904aeaa3279527843fae5ca9ca675f4f51ed8f83bbf7155da9ecc9663100a885d5dc6df96d9").unwrap();
            let expected = false;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_wrong_pubkey_case_e8a50c445c855360.json
        {
            let pubkey = hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap();
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let signature = hex::decode("948a7cb99f76d616c2c564ce9bf4a519f1bea6b0a624a02276443c245854219fabb8d4ce061d255af5330b078d5380681751aa7053da2c98bae898edc218c75f07e24d8802a17cd1f6833b71e58f5eb5b94208b4d0bb3848cecb075ea21be115").unwrap();
            let expected = false;

            let pk = BlsPubKey::uncompress(&pubkey).unwrap();
            let sig = BlsSignature::uncompress(&signature).unwrap();
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_tampered_signature_case_195246ee3bd3b6ec.json
        {
            let pubkey = hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap();
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("ae82747ddeefe4fd64cf9cedb9b04ae3e8a43420cd255e3c7cd06a8d88b7c7f8638543719981c5d16fa3527c468c25f0026704a6951bde891360c7e8d12ddee0559004ccdbe6046b55bae1b257ee97f7cdb955773d7cf29adf3ccbb9ffffffff").unwrap();
            let expected = false;

            let pk = match BlsPubKey::uncompress(&pubkey) {
                Ok(pk) => pk,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let sig = match BlsSignature::uncompress(&signature) {
                Ok(sig) => sig,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_tampered_signature_case_2ea479adf8c40300.json
        {
            let pubkey = hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap();
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let signature = hex::decode("882730e5d03f6b42c3abc26d3372625034e1d871b65a8a6b900a56dae22da98abbe1b68f85e49fe7652a55ec3d0591c20767677e33e5cbb1207315c41a9ac03be39c2e7668edc043d6cb1d9fd93033caa8a1c5b0e84bedaeb6c64972ffffffff").unwrap();
            let expected = false;

            let pk = match BlsPubKey::uncompress(&pubkey) {
                Ok(pk) => pk,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let sig = match BlsSignature::uncompress(&signature) {
                Ok(sig) => sig,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_tampered_signature_case_2f09d443ab8a3ac2.json
        {
            let pubkey = hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap();
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let signature = hex::decode("b23c46be3a001c63ca711f87a005c200cc550b9429d5f4eb38d74322144f1b63926da3388979e5321012fb1a0526bcd100b5ef5fe72628ce4cd5e904aeaa3279527843fae5ca9ca675f4f51ed8f83bbf7155da9ecc9663100a885d5dffffffff").unwrap();
            let expected = false;

            let pk = match BlsPubKey::uncompress(&pubkey) {
                Ok(pk) => pk,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let sig = match BlsSignature::uncompress(&signature) {
                Ok(sig) => sig,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_tampered_signature_case_3208262581c8fc09.json
        {
            let pubkey = hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap();
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let signature = hex::decode("af1390c3c47acdb37131a51216da683c509fce0e954328a59f93aebda7e4ff974ba208d9a4a2a2389f892a9d418d618418dd7f7a6bc7aa0da999a9d3a5b815bc085e14fd001f6a1948768a3f4afefc8b8240dda329f984cb345c6363ffffffff").unwrap();
            let expected = false;

            let pk = match BlsPubKey::uncompress(&pubkey) {
                Ok(pk) => pk,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let sig = match BlsSignature::uncompress(&signature) {
                Ok(sig) => sig,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_tampered_signature_case_6b3b17f6962a490c.json
        {
            let pubkey = hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap();
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let signature = hex::decode("a4efa926610b8bd1c8330c918b7a5e9bf374e53435ef8b7ec186abf62e1b1f65aeaaeb365677ac1d1172a1f5b44b4e6d022c252c58486c0a759fbdc7de15a756acc4d343064035667a594b4c2a6f0b0b421975977f297dba63ee2f63ffffffff").unwrap();
            let expected = false;

            let pk = match BlsPubKey::uncompress(&pubkey) {
                Ok(pk) => pk,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let sig = match BlsSignature::uncompress(&signature) {
                Ok(sig) => sig,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_tampered_signature_case_6eeb7c52dfd9baf0.json
        {
            let pubkey = hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap();
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("9674e2228034527f4c083206032b020310face156d4a4685e2fcaec2f6f3665aa635d90347b6ce124eb879266b1e801d185de36a0a289b85e9039662634f2eea1e02e670bc7ab849d006a70b2f93b84597558a05b879c8d445f387a5ffffffff").unwrap();
            let expected = false;

            let pk = match BlsPubKey::uncompress(&pubkey) {
                Ok(pk) => pk,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let sig = match BlsSignature::uncompress(&signature) {
                Ok(sig) => sig,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_tampered_signature_case_8761a0b7e920c323.json
        {
            let pubkey = hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap();
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("91347bccf740d859038fcdcaf233eeceb2a436bcaaee9b2aa3bfb70efe29dfb2677562ccbea1c8e061fb9971b0753c240622fab78489ce96768259fc01360346da5b9f579e5da0d941e4c6ba18a0e64906082375394f337fa1af2b71ffffffff").unwrap();
            let expected = false;

            let pk = match BlsPubKey::uncompress(&pubkey) {
                Ok(pk) => pk,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let sig = match BlsSignature::uncompress(&signature) {
                Ok(sig) => sig,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_tampered_signature_case_d34885d766d5f705.json
        {
            let pubkey = hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap();
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let signature = hex::decode("948a7cb99f76d616c2c564ce9bf4a519f1bea6b0a624a02276443c245854219fabb8d4ce061d255af5330b078d5380681751aa7053da2c98bae898edc218c75f07e24d8802a17cd1f6833b71e58f5eb5b94208b4d0bb3848cecb075effffffff").unwrap();
            let expected = false;

            let pk = match BlsPubKey::uncompress(&pubkey) {
                Ok(pk) => pk,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let sig = match BlsSignature::uncompress(&signature) {
                Ok(sig) => sig,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_tampered_signature_case_e8a50c445c855360.json
        {
            let pubkey = hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap();
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let signature = hex::decode("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380bffffffff").unwrap();
            let expected = false;

            let pk = match BlsPubKey::uncompress(&pubkey) {
                Ok(pk) => pk,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let sig = match BlsSignature::uncompress(&signature) {
                Ok(sig) => sig,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: verify_infinity_pubkey_and_infinity_signature.json
        {
            let pubkey = hex::decode("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let message =
                hex::decode("1212121212121212121212121212121212121212121212121212121212121212")
                    .unwrap();
            let signature = hex::decode("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let pk = match BlsPubKey::uncompress(&pubkey) {
                Ok(pk) => pk,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let sig = match BlsSignature::uncompress(&signature) {
                Ok(sig) => sig,
                Err(_) => {
                    assert!(!expected);
                    return;
                }
            };
            let ok = sig.verify::<NoPrefix>(&message, &pk).is_ok();
            assert_eq!(ok, expected);
        }
    }

    fn run_ethereum_bls12_381_sign() {
        struct NoPrefix;
        impl SigningDomain for NoPrefix {
            const PREFIX: &'static [u8] = b"";
        }

        // Test: sign_case_11b8c7cad5238946.json
        {
            let privkey =
                hex::decode("47b8192d77bf871b62e87859d653922725724a5c031afeabc60bcef5ff665138")
                    .unwrap();
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let expected = hex::decode("b23c46be3a001c63ca711f87a005c200cc550b9429d5f4eb38d74322144f1b63926da3388979e5321012fb1a0526bcd100b5ef5fe72628ce4cd5e904aeaa3279527843fae5ca9ca675f4f51ed8f83bbf7155da9ecc9663100a885d5dc6df96d9").unwrap();

            let keypair = BlsKeyPair::from_secret_key_bytes(privkey).unwrap();
            let sig = BlsSignature::sign::<NoPrefix>(&message, &keypair).compress();
            assert_eq!(sig, expected);
        }

        // Test: sign_case_142f678a8d05fcd1.json
        {
            let privkey =
                hex::decode("47b8192d77bf871b62e87859d653922725724a5c031afeabc60bcef5ff665138")
                    .unwrap();
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let expected = hex::decode("af1390c3c47acdb37131a51216da683c509fce0e954328a59f93aebda7e4ff974ba208d9a4a2a2389f892a9d418d618418dd7f7a6bc7aa0da999a9d3a5b815bc085e14fd001f6a1948768a3f4afefc8b8240dda329f984cb345c6363272ba4fe").unwrap();

            let keypair = BlsKeyPair::from_secret_key_bytes(privkey).unwrap();
            let sig = BlsSignature::sign::<NoPrefix>(&message, &keypair).compress();
            assert_eq!(sig, expected);
        }

        // Test: sign_case_37286e1a6d1f6eb3.json
        {
            let privkey =
                hex::decode("47b8192d77bf871b62e87859d653922725724a5c031afeabc60bcef5ff665138")
                    .unwrap();
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let expected = hex::decode("9674e2228034527f4c083206032b020310face156d4a4685e2fcaec2f6f3665aa635d90347b6ce124eb879266b1e801d185de36a0a289b85e9039662634f2eea1e02e670bc7ab849d006a70b2f93b84597558a05b879c8d445f387a5d5b653df").unwrap();

            let keypair = BlsKeyPair::from_secret_key_bytes(privkey).unwrap();
            let sig = BlsSignature::sign::<NoPrefix>(&message, &keypair).compress();
            assert_eq!(sig, expected);
        }

        // Test: sign_case_7055381f640f2c1d.json
        {
            let privkey =
                hex::decode("328388aff0d4a5b7dc9205abd374e7e98f3cd9f3418edb4eafda5fb16473d216")
                    .unwrap();
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let expected = hex::decode("948a7cb99f76d616c2c564ce9bf4a519f1bea6b0a624a02276443c245854219fabb8d4ce061d255af5330b078d5380681751aa7053da2c98bae898edc218c75f07e24d8802a17cd1f6833b71e58f5eb5b94208b4d0bb3848cecb075ea21be115").unwrap();

            let keypair = BlsKeyPair::from_secret_key_bytes(privkey).unwrap();
            let sig = BlsSignature::sign::<NoPrefix>(&message, &keypair).compress();
            assert_eq!(sig, expected);
        }

        // Test: sign_case_84d45c9c7cca6b92.json
        {
            let privkey =
                hex::decode("328388aff0d4a5b7dc9205abd374e7e98f3cd9f3418edb4eafda5fb16473d216")
                    .unwrap();
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let expected = hex::decode("ae82747ddeefe4fd64cf9cedb9b04ae3e8a43420cd255e3c7cd06a8d88b7c7f8638543719981c5d16fa3527c468c25f0026704a6951bde891360c7e8d12ddee0559004ccdbe6046b55bae1b257ee97f7cdb955773d7cf29adf3ccbb9975e4eb9").unwrap();

            let keypair = BlsKeyPair::from_secret_key_bytes(privkey).unwrap();
            let sig = BlsSignature::sign::<NoPrefix>(&message, &keypair).compress();
            assert_eq!(sig, expected);
        }

        // Test: sign_case_8cd3d4d0d9a5b265.json
        {
            let privkey =
                hex::decode("328388aff0d4a5b7dc9205abd374e7e98f3cd9f3418edb4eafda5fb16473d216")
                    .unwrap();
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let expected = hex::decode("a4efa926610b8bd1c8330c918b7a5e9bf374e53435ef8b7ec186abf62e1b1f65aeaaeb365677ac1d1172a1f5b44b4e6d022c252c58486c0a759fbdc7de15a756acc4d343064035667a594b4c2a6f0b0b421975977f297dba63ee2f63ffe47bb6").unwrap();

            let keypair = BlsKeyPair::from_secret_key_bytes(privkey).unwrap();
            let sig = BlsSignature::sign::<NoPrefix>(&message, &keypair).compress();
            assert_eq!(sig, expected);
        }

        // Test: sign_case_c82df61aa3ee60fb.json
        {
            let privkey =
                hex::decode("263dbd792f5b1be47ed85f8938c0f29586af0d3ac7b977f21c278fe1462040e3")
                    .unwrap();
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let expected = hex::decode("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380b55285a55").unwrap();

            let keypair = BlsKeyPair::from_secret_key_bytes(privkey).unwrap();
            let sig = BlsSignature::sign::<NoPrefix>(&message, &keypair).compress();
            assert_eq!(sig, expected);
        }

        // Test: sign_case_d0e28d7e76eb6e9c.json
        {
            let privkey =
                hex::decode("263dbd792f5b1be47ed85f8938c0f29586af0d3ac7b977f21c278fe1462040e3")
                    .unwrap();
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let expected = hex::decode("882730e5d03f6b42c3abc26d3372625034e1d871b65a8a6b900a56dae22da98abbe1b68f85e49fe7652a55ec3d0591c20767677e33e5cbb1207315c41a9ac03be39c2e7668edc043d6cb1d9fd93033caa8a1c5b0e84bedaeb6c64972503a43eb").unwrap();

            let keypair = BlsKeyPair::from_secret_key_bytes(privkey).unwrap();
            let sig = BlsSignature::sign::<NoPrefix>(&message, &keypair).compress();
            assert_eq!(sig, expected);
        }

        // Test: sign_case_f2ae1097e7d0e18b.json
        {
            let privkey =
                hex::decode("263dbd792f5b1be47ed85f8938c0f29586af0d3ac7b977f21c278fe1462040e3")
                    .unwrap();
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let expected = hex::decode("91347bccf740d859038fcdcaf233eeceb2a436bcaaee9b2aa3bfb70efe29dfb2677562ccbea1c8e061fb9971b0753c240622fab78489ce96768259fc01360346da5b9f579e5da0d941e4c6ba18a0e64906082375394f337fa1af2b7127b0d121").unwrap();

            let keypair = BlsKeyPair::from_secret_key_bytes(privkey).unwrap();
            let sig = BlsSignature::sign::<NoPrefix>(&message, &keypair).compress();
            assert_eq!(sig, expected);
        }

        // Test: sign_case_zero_privkey.json
        {
            let privkey =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let _message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let expected = false;

            // Expected output is null - this should fail
            assert_eq!(expected, BlsKeyPair::from_secret_key_bytes(privkey).is_ok());
        }
    }

    fn run_ethereum_bls12_381_aggregate_verify() {
        struct NoPrefix;
        impl SigningDomain for NoPrefix {
            const PREFIX: &'static [u8] = b"";
        }

        // Test: aggregate_verify_valid.json
        {
            let pubkeys = [
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
                hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap(),
                hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap(),
            ];
            let messages = [
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap(),
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap(),
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap(),
            ];
            let signature = hex::decode("9104e74b9dfd3ad502f25d6a5ef57db0ed7d9a0e00f3500586d8ce44231212542fcfaf87840539b398bf07626705cf1105d246ca1062c6c2e1a53029a0f790ed5e3cb1f52f8234dc5144c45fc847c0cd37a92d68e7c5ba7c648a8a339f171244").unwrap();
            let expected = true;

            let mut pks: Vec<BlsAggregatePubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks.push(BlsAggregatePubKey::from_pubkey(&pk));
            }

            let aggsig = BlsAggregateSignature::deserialize(&signature).unwrap();
            let msgs_ref: Vec<&[u8]> = messages.iter().map(|m| m.as_slice()).collect();
            let pks_ref: Vec<&BlsAggregatePubKey> = pks.iter().collect();

            let ok = aggsig.verify::<NoPrefix>(&msgs_ref, &pks_ref).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: aggregate_verify_infinity_pubkey.json
        {
            let pubkeys = [
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
                hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap(),
                hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap(),
                hex::decode("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap(), // infinity pubkey
            ];
            let messages = [
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap(),
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap(),
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap(),
                hex::decode("1212121212121212121212121212121212121212121212121212121212121212")
                    .unwrap(),
            ];
            let signature = hex::decode("9104e74b9dfd3ad502f25d6a5ef57db0ed7d9a0e00f3500586d8ce44231212542fcfaf87840539b398bf07626705cf1105d246ca1062c6c2e1a53029a0f790ed5e3cb1f52f8234dc5144c45fc847c0cd37a92d68e7c5ba7c648a8a339f171244").unwrap();
            let expected = false;

            let mut pks: Vec<BlsAggregatePubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                match BlsPubKey::uncompress(pk_bytes) {
                    Ok(pk) => pks.push(BlsAggregatePubKey::from_pubkey(&pk)),
                    Err(_) => {
                        // Infinity pubkey should fail to uncompress
                        assert!(!expected);
                        continue;
                    }
                }
            }

            let aggsig = BlsAggregateSignature::deserialize(&signature).unwrap();
            let msgs_ref: Vec<&[u8]> = messages.iter().map(|m| m.as_slice()).collect();
            let pks_ref: Vec<&BlsAggregatePubKey> = pks.iter().collect();
            let ok = aggsig.verify::<NoPrefix>(&msgs_ref, &pks_ref).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: aggregate_verify_na_pubkeys_and_infinity_signature.json
        {
            let pubkeys: Vec<Vec<u8>> = vec![];
            let messages: Vec<Vec<u8>> = vec![];
            let signature = hex::decode("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let mut pks: Vec<BlsAggregatePubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks.push(BlsAggregatePubKey::from_pubkey(&pk));
            }
            let aggsig = BlsAggregateSignature::deserialize(&signature).unwrap();
            let msgs_ref: Vec<&[u8]> = messages.iter().map(|m| m.as_slice()).collect();
            let pks_ref: Vec<&BlsAggregatePubKey> = pks.iter().collect();

            let ok = aggsig.verify::<NoPrefix>(&msgs_ref, &pks_ref).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: aggregate_verify_na_pubkeys_and_na_signature.json
        {
            let pubkeys: Vec<Vec<u8>> = vec![];
            let messages: Vec<Vec<u8>> = vec![];
            let signature = hex::decode("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let mut pks: Vec<BlsAggregatePubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks.push(BlsAggregatePubKey::from_pubkey(&pk));
            }
            let msgs_ref: Vec<&[u8]> = messages.iter().map(|m| m.as_slice()).collect();
            let pks_ref: Vec<&BlsAggregatePubKey> = pks.iter().collect();

            // Zero signature might fail to deserialize
            match BlsAggregateSignature::deserialize(&signature) {
                Ok(aggsig) => {
                    let ok = aggsig.verify::<NoPrefix>(&msgs_ref, &pks_ref).is_ok();
                    assert_eq!(ok, expected);
                }
                Err(_) => {
                    assert!(!expected);
                }
            }
        }

        // Test: aggregate_verify_tampered_signature.json
        {
            let pubkeys = [
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
                hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap(),
                hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap(),
            ];
            let messages = [
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap(),
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap(),
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap(),
            ];
            let signature = hex::decode("9104e74bffffffff").unwrap(); // Tampered/truncated
            let expected = false;

            let mut pks: Vec<BlsAggregatePubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks.push(BlsAggregatePubKey::from_pubkey(&pk));
            }
            let pks_ref: Vec<&BlsAggregatePubKey> = pks.iter().collect();
            let msgs_ref: Vec<&[u8]> = messages.iter().map(|m| m.as_slice()).collect();

            // This should fail to deserialize due to truncated signature
            match BlsAggregateSignature::deserialize(&signature) {
                Ok(aggsig) => {
                    let ok = aggsig.verify::<NoPrefix>(&msgs_ref, &pks_ref).is_ok();
                    assert_eq!(ok, expected);
                }
                Err(_) => {
                    assert!(!expected);
                }
            }
        }
    }

    fn run_ethereum_bls12_381_batch_verify() {
        struct NoPrefix;
        impl SigningDomain for NoPrefix {
            const PREFIX: &'static [u8] = b"";
        }

        // Test: batc_verify_valid_signature_set.json
        {
            let pubkeys = [
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
                hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap(),
                hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap(),
            ];
            let messages = [
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap(),
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap(),
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap(),
            ];
            let signatures = [
                hex::decode("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380b55285a55").unwrap(),
                hex::decode("af1390c3c47acdb37131a51216da683c509fce0e954328a59f93aebda7e4ff974ba208d9a4a2a2389f892a9d418d618418dd7f7a6bc7aa0da999a9d3a5b815bc085e14fd001f6a1948768a3f4afefc8b8240dda329f984cb345c6363272ba4fe").unwrap(),
                hex::decode("ae82747ddeefe4fd64cf9cedb9b04ae3e8a43420cd255e3c7cd06a8d88b7c7f8638543719981c5d16fa3527c468c25f0026704a6951bde891360c7e8d12ddee0559004ccdbe6046b55bae1b257ee97f7cdb955773d7cf29adf3ccbb9975e4eb9").unwrap(),
            ];
            let expected = true;

            // Batch verify semantics: true iff all individual verifications succeed
            let mut ok = true;
            for i in 0..pubkeys.len() {
                let pk = match BlsPubKey::uncompress(&pubkeys[i]) {
                    Ok(pk) => pk,
                    Err(_) => {
                        ok = false;
                        break;
                    }
                };
                let sig = match BlsSignature::uncompress(&signatures[i]) {
                    Ok(sig) => sig,
                    Err(_) => {
                        ok = false;
                        break;
                    }
                };
                if sig.verify::<NoPrefix>(&messages[i], &pk).is_err() {
                    ok = false;
                    break;
                }
            }
            assert_eq!(ok, expected);
        }
    }

    fn run_ethereum_bls12_381_fast_aggregate_verify() {
        use blst::BLST_ERROR::BLST_PK_IS_INFINITY;
        struct NoPrefix;
        impl SigningDomain for NoPrefix {
            const PREFIX: &'static [u8] = b"";
        }

        // Test: fast_aggregate_verify_valid_3d7576f3c0e3570a.json
        {
            let pubkeys = vec![
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
                hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap(),
                hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap(),
            ];
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("9712c3edd73a209c742b8250759db12549b3eaf43b5ca61376d9f30e2747dbcf842d8b2ac0901d2a093713e20284a7670fcf6954e9ab93de991bb9b313e664785a075fc285806fa5224c82bde146561b446ccfc706a64b8579513cfc4ff1d930").unwrap();
            let expected = true;

            let mut pks_plain: Vec<BlsPubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks_plain.push(pk);
            }
            let pk_refs: Vec<&BlsPubKey> = pks_plain.iter().collect();
            let agg_pk = BlsAggregatePubKey::aggregate(&pk_refs).unwrap();
            let aggsig = BlsAggregateSignature::deserialize(&signature).unwrap();
            let ok = aggsig.fast_verify::<NoPrefix>(&message, &agg_pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: fast_aggregate_verify_valid_5e745ad0c6199a6c.json
        {
            let pubkeys = vec![
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
            ];
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let signature = hex::decode("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380b55285a55").unwrap();
            let expected = true;

            let mut pks_plain: Vec<BlsPubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks_plain.push(pk);
            }
            let pk_refs: Vec<&BlsPubKey> = pks_plain.iter().collect();
            let agg_pk = BlsAggregatePubKey::aggregate(&pk_refs).unwrap();
            let aggsig = BlsAggregateSignature::deserialize(&signature).unwrap();
            let ok = aggsig.fast_verify::<NoPrefix>(&message, &agg_pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: fast_aggregate_verify_valid_652ce62f09290811.json
        {
            let pubkeys = vec![
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
                hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap(),
            ];
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let signature = hex::decode("912c3615f69575407db9392eb21fee18fff797eeb2fbe1816366ca2a08ae574d8824dbfafb4c9eaa1cf61b63c6f9b69911f269b664c42947dd1b53ef1081926c1e82bb2a465f927124b08391a5249036146d6f3f1e17ff5f162f779746d830d1").unwrap();
            let expected = true;

            let mut pks_plain: Vec<BlsPubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks_plain.push(pk);
            }
            let pk_refs: Vec<&BlsPubKey> = pks_plain.iter().collect();
            let agg_pk = BlsAggregatePubKey::aggregate(&pk_refs).unwrap();
            let aggsig = BlsAggregateSignature::deserialize(&signature).unwrap();
            let ok = aggsig.fast_verify::<NoPrefix>(&message, &agg_pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: fast_aggregate_verify_extra_pubkey_4f079f946446fabf.json
        {
            let pubkeys = vec![
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
                hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap(),
                hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap(),
            ];
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let signature = hex::decode("912c3615f69575407db9392eb21fee18fff797eeb2fbe1816366ca2a08ae574d8824dbfafb4c9eaa1cf61b63c6f9b69911f269b664c42947dd1b53ef1081926c1e82bb2a465f927124b08391a5249036146d6f3f1e17ff5f162f779746d830d1").unwrap();
            let expected = false;

            let mut pks_plain: Vec<BlsPubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks_plain.push(pk);
            }
            let pk_refs: Vec<&BlsPubKey> = pks_plain.iter().collect();
            let agg_pk = BlsAggregatePubKey::aggregate(&pk_refs).unwrap();
            let aggsig = BlsAggregateSignature::deserialize(&signature).unwrap();
            let ok = aggsig.fast_verify::<NoPrefix>(&message, &agg_pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: fast_aggregate_verify_extra_pubkey_5a38e6b4017fe4dd.json
        {
            let pubkeys = vec![
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
                hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap(),
                hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap(),
                hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap(),
            ];
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("9712c3edd73a209c742b8250759db12549b3eaf43b5ca61376d9f30e2747dbcf842d8b2ac0901d2a093713e20284a7670fcf6954e9ab93de991bb9b313e664785a075fc285806fa5224c82bde146561b446ccfc706a64b8579513cfc4ff1d930").unwrap();
            let expected = false;

            let mut pks_plain: Vec<BlsPubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks_plain.push(pk);
            }
            let pk_refs: Vec<&BlsPubKey> = pks_plain.iter().collect();
            let agg_pk = BlsAggregatePubKey::aggregate(&pk_refs).unwrap();
            let aggsig = BlsAggregateSignature::deserialize(&signature).unwrap();
            let ok = aggsig.fast_verify::<NoPrefix>(&message, &agg_pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: fast_aggregate_verify_extra_pubkey_a698ea45b109f303.json
        {
            let pubkeys = vec![
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
                hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap(),
            ];
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let signature = hex::decode("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380b55285a55").unwrap();
            let expected = false;

            let mut pks_plain: Vec<BlsPubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks_plain.push(pk);
            }
            let pk_refs: Vec<&BlsPubKey> = pks_plain.iter().collect();
            let agg_pk = BlsAggregatePubKey::aggregate(&pk_refs).unwrap();
            let aggsig = BlsAggregateSignature::deserialize(&signature).unwrap();
            let ok = aggsig.fast_verify::<NoPrefix>(&message, &agg_pk).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: fast_aggregate_verify_infinity_pubkey.json
        {
            let pubkeys = vec![
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
                hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap(),
                hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap(),
                hex::decode("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap(),
            ];
            let message =
                hex::decode("1212121212121212121212121212121212121212121212121212121212121212")
                    .unwrap();
            let signature = hex::decode("afcb4d980f079265caa61aee3e26bf48bebc5dc3e7f2d7346834d76cbc812f636c937b6b44a9323d8bc4b1cdf71d6811035ddc2634017faab2845308f568f2b9a0356140727356eae9eded8b87fd8cb8024b440c57aee06076128bb32921f584").unwrap();
            let expected = false;

            let mut pks_plain: Vec<BlsPubKey> = Vec::new();
            let mut has_infinity = false;
            for pk_bytes in &pubkeys {
                match BlsPubKey::uncompress(pk_bytes) {
                    Ok(pk) => pks_plain.push(pk),
                    Err(err) => {
                        if err == BlsError(BLST_PK_IS_INFINITY) {
                            has_infinity = true;
                            break;
                        }
                    }
                }
            }

            if has_infinity {
                assert!(!expected);
            } else {
                let pk_refs: Vec<&BlsPubKey> = pks_plain.iter().collect();
                let agg_pk = BlsAggregatePubKey::aggregate(&pk_refs).unwrap();
                let aggsig = BlsAggregateSignature::deserialize(&signature).unwrap();
                let ok = aggsig.fast_verify::<NoPrefix>(&message, &agg_pk).is_ok();
                assert_eq!(ok, expected);
            }
        }

        // Test: fast_aggregate_verify_na_pubkeys_and_infinity_signature.json
        {
            let pubkeys: Vec<Vec<u8>> = vec![];
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let mut pks_plain: Vec<BlsPubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks_plain.push(pk);
            }
            let pk_refs: Vec<&BlsPubKey> = pks_plain.iter().collect();
            if let Ok(agg_pk) = BlsAggregatePubKey::aggregate(&pk_refs) {
                if let Ok(aggsig) = BlsAggregateSignature::deserialize(&signature) {
                    let ok = aggsig.fast_verify::<NoPrefix>(&message, &agg_pk).is_ok();
                    assert_eq!(ok, expected);
                }
            }
        }

        // Test: fast_aggregate_verify_na_pubkeys_and_na_signature.json
        {
            let pubkeys: Vec<Vec<u8>> = vec![];
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let mut pks_plain: Vec<BlsPubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks_plain.push(pk);
            }
            let pk_refs: Vec<&BlsPubKey> = pks_plain.iter().collect();
            if let Ok(agg_pk) = BlsAggregatePubKey::aggregate(&pk_refs) {
                if let Ok(aggsig) = BlsAggregateSignature::deserialize(&signature) {
                    let ok = aggsig.fast_verify::<NoPrefix>(&message, &agg_pk).is_ok();
                    assert_eq!(ok, expected);
                }
            }
        }

        // Test: fast_aggregate_verify_tampered_signature_3d7576f3c0e3570a.json
        {
            let pubkeys = vec![
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
                hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap(),
                hex::decode("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").unwrap(),
            ];
            let message =
                hex::decode("abababababababababababababababababababababababababababababababab")
                    .unwrap();
            let signature = hex::decode("9712c3edd73a209c742b8250759db12549b3eaf43b5ca61376d9f30e2747dbcf842d8b2ac0901d2a093713e20284a7670fcf6954e9ab93de991bb9b313e664785a075fc285806fa5224c82bde146561b446ccfc706a64b8579513cfcffffffff").unwrap();
            let expected = false;

            let mut pks_plain: Vec<BlsPubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks_plain.push(pk);
            }
            let pk_refs: Vec<&BlsPubKey> = pks_plain.iter().collect();
            let agg_pk = BlsAggregatePubKey::aggregate(&pk_refs).unwrap();
            match BlsAggregateSignature::deserialize(&signature) {
                Ok(aggsig) => {
                    let ok = aggsig.fast_verify::<NoPrefix>(&message, &agg_pk).is_ok();
                    assert_eq!(ok, expected);
                }
                Err(_) => {
                    assert!(!expected);
                }
            }
        }

        // Test: fast_aggregate_verify_tampered_signature_5e745ad0c6199a6c.json
        {
            let pubkeys = vec![
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
            ];
            let message =
                hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap();
            let signature = hex::decode("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380bffffffff").unwrap();
            let expected = false;

            let mut pks_plain: Vec<BlsPubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks_plain.push(pk);
            }
            let pk_refs: Vec<&BlsPubKey> = pks_plain.iter().collect();
            let agg_pk = BlsAggregatePubKey::aggregate(&pk_refs).unwrap();
            match BlsAggregateSignature::deserialize(&signature) {
                Ok(aggsig) => {
                    let ok = aggsig.fast_verify::<NoPrefix>(&message, &agg_pk).is_ok();
                    assert_eq!(ok, expected);
                }
                Err(_) => {
                    assert!(!expected);
                }
            }
        }

        // Test: fast_aggregate_verify_tampered_signature_652ce62f09290811.json
        {
            let pubkeys = vec![
                hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap(),
                hex::decode("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").unwrap(),
            ];
            let message =
                hex::decode("5656565656565656565656565656565656565656565656565656565656565656")
                    .unwrap();
            let signature = hex::decode("912c3615f69575407db9392eb21fee18fff797eeb2fbe1816366ca2a08ae574d8824dbfafb4c9eaa1cf61b63c6f9b69911f269b664c42947dd1b53ef1081926c1e82bb2a465f927124b08391a5249036146d6f3f1e17ff5f162f7797ffffffff").unwrap();
            let expected = false;

            let mut pks_plain: Vec<BlsPubKey> = Vec::new();
            for pk_bytes in &pubkeys {
                let pk = BlsPubKey::uncompress(pk_bytes).unwrap();
                pks_plain.push(pk);
            }
            let pk_refs: Vec<&BlsPubKey> = pks_plain.iter().collect();
            let agg_pk = BlsAggregatePubKey::aggregate(&pk_refs).unwrap();
            match BlsAggregateSignature::deserialize(&signature) {
                Ok(aggsig) => {
                    let ok = aggsig.fast_verify::<NoPrefix>(&message, &agg_pk).is_ok();
                    assert_eq!(ok, expected);
                }
                Err(_) => {
                    assert!(!expected);
                }
            }
        }
    }

    fn run_ethereum_bls12_381_deserialization_g1() {
        // Test: deserialization_succeeds_correct_point.json
        {
            let pubkey_bytes = hex::decode("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").unwrap();
            let expected = true;

            let ok = BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_succeeds_infinity_with_true_b_flag.json
        {
            let pubkey_bytes = hex::decode("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = true;

            // This is the valid infinity point representation
            let ok = pubkey_bytes.as_slice() == INFINITY_PUBKEY.as_slice()
                || BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_infinity_with_false_b_flag.json
        {
            let pubkey_bytes = hex::decode("800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let ok = BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_infinity_with_true_b_flag.json
        {
            let pubkey_bytes = hex::decode("c01000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let ok = BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_not_in_curve.json
        {
            let pubkey_bytes = hex::decode("8123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde0").unwrap();
            let expected = false;

            let ok = BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_not_in_G1.json
        {
            let pubkey_bytes = hex::decode("8123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap();
            let expected = false;

            let ok = BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_too_few_bytes.json
        {
            let pubkey_bytes = hex::decode("9a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaa").unwrap();
            let expected = false;

            let ok = BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_too_many_bytes.json
        {
            let pubkey_bytes = hex::decode("9a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaa900").unwrap();
            let expected = false;

            let ok = BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_with_b_flag_and_a_flag_true.json
        {
            let pubkey_bytes = hex::decode("e00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let ok = BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_with_b_flag_and_x_nonzero.json
        {
            let pubkey_bytes = hex::decode("c123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap();
            let expected = false;

            let ok = BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_with_wrong_c_flag.json
        {
            let pubkey_bytes = hex::decode("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap();
            let expected = false;

            let ok = BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_x_equal_to_modulus.json
        {
            let pubkey_bytes = hex::decode("9a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab").unwrap();
            let expected = false;

            let ok = BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_x_greater_than_modulus.json
        {
            let pubkey_bytes = hex::decode("9a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaac").unwrap();
            let expected = false;

            let ok = BlsPubKey::uncompress(&pubkey_bytes).is_ok();
            assert_eq!(ok, expected);
        }
    }

    fn run_ethereum_bls12_381_deserialization_g2() {
        // Test: deserialization_succeeds_correct_point.json
        {
            let signature_bytes = hex::decode("b2cc74bc9f089ed9764bbceac5edba416bef5e73701288977b9cac1ccb6964269d4ebf78b4e8aa7792ba09d3e49c8e6a1351bdf582971f796bbaf6320e81251c9d28f674d720cca07ed14596b96697cf18238e0e03ebd7fc1353d885a39407e0").unwrap();
            let expected = true;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_succeeds_infinity_with_true_b_flag.json
        {
            let signature_bytes = hex::decode("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = true;

            // This is the valid infinity point representation
            let ok = signature_bytes.as_slice() == INFINITY_SIGNATURE.as_slice()
                || BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_infinity_with_false_b_flag.json
        {
            let signature_bytes = hex::decode("800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_infinity_with_true_b_flag.json
        {
            let signature_bytes = hex::decode("c01000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_not_in_curve.json
        {
            let signature_bytes = hex::decode("8123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde0").unwrap();
            let expected = false;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_not_in_G2.json
        {
            let signature_bytes = hex::decode("8123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap();
            let expected = false;

            let sig = BlsSignature::uncompress(&signature_bytes).unwrap();
            assert_eq!(expected, sig.validate(true).is_ok());
        }

        // Test: deserialization_fails_too_few_bytes.json
        {
            let signature_bytes = hex::decode("8123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd").unwrap();
            let expected = false;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_too_many_bytes.json
        {
            let signature_bytes = hex::decode("8123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdefff").unwrap();
            let expected = false;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_with_b_flag_and_a_flag_true.json
        {
            let signature_bytes = hex::decode("e00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_with_b_flag_and_x_nonzero.json
        {
            let signature_bytes = hex::decode("c123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap();
            let expected = false;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_with_wrong_c_flag.json
        {
            let signature_bytes = hex::decode("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap();
            let expected = false;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_xim_equal_to_modulus.json
        {
            let signature_bytes = hex::decode("9a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_xim_greater_than_modulus.json
        {
            let signature_bytes = hex::decode("9a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaac000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap();
            let expected = false;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_xre_equal_to_modulus.json
        {
            let signature_bytes = hex::decode("8000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab").unwrap();
            let expected = false;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }

        // Test: deserialization_fails_xre_greater_than_modulus.json
        {
            let signature_bytes = hex::decode("8000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaac").unwrap();
            let expected = false;

            let ok = BlsSignature::uncompress(&signature_bytes).is_ok();
            assert_eq!(ok, expected);
        }
    }
}
