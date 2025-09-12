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
    pub fn add_assign(&mut self, other: &BlsPubKey) -> Result<(), BlsError> {
        self.0.add_public_key(&other.0, false).map_err(BlsError)
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

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use monad_crypto::signing_domain;

    use super::{
        blst_core, BlsAggregatePubKey, BlsAggregateSignature, BlsError, BlsKeyPair, BlsPubKey,
        BlsSecretKey, BlsSignature, BLST_BAD_ENCODING, INFINITY_PUBKEY,
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
            aggpk.add_assign(pk0).unwrap();
            if let Some(pk1) = iter.next() {
                aggpk.add_assign(pk1).unwrap();
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
            agg1.add_assign(pk).unwrap();
        }

        let mut agg2 = BlsAggregatePubKey::infinity();
        for pk in pks.iter().rev() {
            agg2.add_assign(pk).unwrap();
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
            pk_add_assign.add_assign(pk).unwrap();
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
}
