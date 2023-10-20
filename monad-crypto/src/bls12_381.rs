use blst::min_pk as blst_core;
use zeroize::Zeroize;

use crate::hasher::{Hashable, Hasher};

// if the curve is switched to min_sig
// the DST needs to be BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_
// POP uses a separate pubkey validation step, enables fast verification for
// signatures over the same message
// https://datatracker.ietf.org/doc/html/draft-irtf-cfrg-bls-signature-05#name-proof-of-possession
const DST: &[u8] = MIN_PK_DST;
#[allow(dead_code)]
const MIN_PK_DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";
#[allow(dead_code)]
const MIN_SIG_DST: &[u8] = b"BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_";

// TODO: simplify group swap with macro?
const SIGNATURE_BYTE_LEN: usize = G2_BYTE_LEN;
const SIGNATURE_COMPRESSED_LEN: usize = G2_COMPRESSED_LEN;
const INFINITY_SIGNATURE: [u8; SIGNATURE_COMPRESSED_LEN] = G2_INFINITY;

const PUBKEY_BYTE_LEN: usize = G1_BYTE_LEN;
const PUBKEY_COMPRESSED_LEN: usize = G1_COMPRESSED_LEN;
const INFINITY_PUBKEY: [u8; PUBKEY_COMPRESSED_LEN] = G1_INFINITY;

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

#[derive(Debug, PartialEq, Eq)]
pub struct BlsError(blst::BLST_ERROR);

impl std::fmt::Display for BlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for BlsError {}

fn map_err_to_result(bls_error: blst::BLST_ERROR) -> Result<(), BlsError> {
    match bls_error {
        blst::BLST_ERROR::BLST_SUCCESS => Ok(()),
        err => Err(BlsError(err)),
    }
}

// PubKey and AggregatePubKey
#[derive(Debug, Clone, Copy)]
pub struct BlsAggregatePubKey(blst_core::AggregatePublicKey);

#[derive(Debug, Clone, Copy)]
pub struct BlsPubKey(blst_core::PublicKey);

impl From<blst_core::PublicKey> for BlsPubKey {
    fn from(value: blst_core::PublicKey) -> Self {
        Self(value)
    }
}

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

impl BlsPubKey {
    pub fn validate(&self) -> Result<(), BlsError> {
        self.0.validate().map_err(BlsError)
    }

    pub fn serialize(&self) -> Vec<u8> {
        self.0.serialize().to_vec()
    }

    pub fn deserialize(message: &[u8]) -> Result<Self, BlsError> {
        blst_core::PublicKey::deserialize(message)
            .map(Self)
            .map_err(BlsError)
    }

    pub fn compress(&self) -> Vec<u8> {
        self.0.compress().to_vec()
    }

    pub fn uncompress(msg: &[u8]) -> Result<Self, BlsError> {
        blst_core::PublicKey::uncompress(msg)
            .map(Self)
            .map_err(BlsError)
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
    pub fn infinity() -> Self {
        Self::deserialize(&INFINITY_PUBKEY).expect("Infinity BLS pubkey")
    }

    fn as_pubkey(&self) -> BlsPubKey {
        BlsPubKey(self.0.to_public_key())
    }

    fn from_pubkey(pubkey: &BlsPubKey) -> Self {
        Self(blst_core::AggregatePublicKey::from_public_key(&pubkey.0))
    }

    pub fn validate(&self) -> Result<(), BlsError> {
        self.as_pubkey().validate()
    }

    pub fn aggregate(pks: &[&BlsPubKey]) -> Result<Self, BlsError> {
        let pks = pks.iter().map(|p| &p.0).collect::<Vec<_>>();
        blst_core::AggregatePublicKey::aggregate(pks.as_ref(), false)
            .map(Self)
            .map_err(BlsError)
    }

    pub fn add_assign(&mut self, other: &BlsPubKey) -> Result<(), BlsError> {
        self.0.add_public_key(&other.0, false).map_err(BlsError)
    }

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

    pub fn compress(&self) -> Vec<u8> {
        self.as_pubkey().compress()
    }

    pub fn uncompress(msg: &[u8]) -> Result<Self, BlsError> {
        let pk = BlsPubKey::uncompress(msg)?;
        Ok(Self::from_pubkey(&pk))
    }
}

struct BlsSecretKey(blst_core::SecretKey);

pub struct BlsKeyPair {
    pubkey: BlsPubKey,
    secretkey: BlsSecretKey,
}

impl BlsSecretKey {
    fn key_gen(ikm: &[u8], key_info: &[u8]) -> Result<Self, BlsError> {
        blst_core::SecretKey::key_gen(ikm, key_info)
            .map(Self)
            .map_err(BlsError)
    }

    fn sk_to_pk(&self) -> BlsPubKey {
        self.0.sk_to_pk().into()
    }
}

impl BlsKeyPair {
    pub fn from_bytes(mut secret: impl AsMut<[u8]>) -> Result<Self, BlsError> {
        let secret = secret.as_mut();
        let sk = BlsSecretKey::key_gen(secret, &[])?;
        secret.zeroize();
        let keypair = Self {
            pubkey: sk.sk_to_pk(),
            secretkey: sk,
        };
        Ok(keypair)
    }

    pub fn sign(&self, msg: &[u8]) -> BlsSignature {
        self.secretkey.0.sign(msg, DST, &[]).into()
    }

    pub fn pubkey(&self) -> BlsPubKey {
        self.pubkey
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BlsAggregateSignature(blst_core::AggregateSignature);

#[derive(Debug, Clone, Copy)]
pub struct BlsSignature(blst_core::Signature);

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

impl Hashable for BlsSignature {
    fn hash(&self, state: &mut impl Hasher) {
        let slice =
            unsafe { std::mem::transmute::<Self, [u8; std::mem::size_of::<Self>()]>(*self) };
        state.update(slice);
    }
}

impl PartialEq for BlsSignature {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for BlsSignature {}

impl BlsSignature {
    pub fn serialize(&self) -> Vec<u8> {
        self.0.serialize().to_vec()
    }

    pub fn deserialize(message: &[u8]) -> Result<Self, BlsError> {
        blst_core::Signature::deserialize(message)
            .map(Self)
            .map_err(BlsError)
    }

    pub fn compress(&self) -> Vec<u8> {
        self.0.compress().to_vec()
    }

    pub fn uncompress(message: &[u8]) -> Result<Self, BlsError> {
        blst_core::Signature::uncompress(message)
            .map(Self)
            .map_err(BlsError)
    }

    pub fn sign(msg: &[u8], keypair: &BlsKeyPair) -> Self {
        keypair.sign(msg)
    }

    pub fn verify(&self, msg: &[u8], pubkey: &BlsPubKey) -> Result<(), BlsError> {
        let err = self.0.verify(true, msg, DST, &[], &pubkey.0, false);
        map_err_to_result(err)
    }

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
}

impl From<blst_core::AggregateSignature> for BlsAggregateSignature {
    fn from(value: blst_core::AggregateSignature) -> Self {
        Self(value)
    }
}

impl BlsAggregateSignature {
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

    pub fn infinity() -> Self {
        Self::deserialize(&INFINITY_SIGNATURE).expect("Infinity BLS signature")
    }

    pub fn validate(&self) -> Result<(), BlsError> {
        self.as_signature().validate(true)
    }

    pub fn add_assign(&mut self, other: &BlsSignature) -> Result<(), BlsError> {
        self.0.add_signature(&other.0, false).map_err(BlsError)
    }

    pub fn add_assign_aggregate(&mut self, other: &Self) {
        self.0.add_aggregate(&other.0)
    }

    pub fn fast_verify(&self, msg: &[u8], pubkey: &BlsAggregatePubKey) -> Result<(), BlsError> {
        let err = self
            .as_signature()
            .fast_aggregate_verify_pre_aggregated(false, msg, DST, pubkey);
        map_err_to_result(err)
    }

    pub fn verify(&self, msgs: &[&[u8]], pubkeys: &[&BlsAggregatePubKey]) -> Result<(), BlsError> {
        let pks = pubkeys.iter().map(|pk| pk.as_pubkey()).collect::<Vec<_>>();
        let pks: Vec<&BlsPubKey> = pks.iter().collect();

        let err = self
            .as_signature()
            .aggregate_verify(false, msgs, DST, pks.as_ref(), false);
        map_err_to_result(err)
    }

    pub fn as_signature(&self) -> BlsSignature {
        self.0.to_signature().into()
    }

    fn from_signature(sig: &BlsSignature) -> Self {
        blst_core::AggregateSignature::from_signature(&sig.0).into()
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

impl Hashable for BlsAggregateSignature {
    fn hash(&self, state: &mut impl Hasher) {
        Hashable::hash(&self.as_signature(), state);
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::{
        BlsAggregatePubKey, BlsAggregateSignature, BlsError, BlsKeyPair, BlsPubKey, BlsSignature,
    };

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
    fn test_signature_roundtrip() {
        let keypair = keygen(7);
        let msg = keypair.pubkey().serialize();

        let sig = BlsSignature::sign(msg.as_ref(), &keypair);

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

        let sig = BlsSignature::sign(msg.as_ref(), &keypair);

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
            aggsig.add_assign(&kp.sign(msg)).unwrap();
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
    fn test_aggregate_signature_roundtrip_compressed() {
        let keypairs = gen_keypairs(2);
        let msg = b"hello world";
        let mut aggsig = BlsAggregateSignature::infinity();
        for kp in keypairs.iter() {
            aggsig.add_assign(&kp.sign(msg)).unwrap();
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
            .map(|kp| kp.sign(&kp.pubkey().serialize()))
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

        let sig = keypair.sign(msg);
        assert!(sig.verify(msg, &pubkey).is_ok());
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
            sig.add_assign(&kp.sign(msg)).unwrap();
        }

        assert!(sig.fast_verify(msg, &agg_pk).is_ok())
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
            sig.add_assign(&kp.sign(msg)).unwrap();
        }

        let msg2 = b"bye world";
        sig.add_assign(&keypairs[2].sign(msg2)).unwrap();

        assert_eq!(
            sig.fast_verify(msg, &agg_pk),
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
            let sig = kp.sign(&msg);
            aggsig.add_assign(&sig).unwrap();
        }

        assert!(aggsig.verify(&msgs_ref, &pks_ref).is_ok());
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
            let sig = kp.sign(&msg);
            aggsig.add_assign(&sig).unwrap();
        }

        assert_eq!(
            aggsig.verify(&msgs_ref, &pks_ref),
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
            sig1.add_assign(&kp.sign(msg)).unwrap();
        }

        let mut sig2 = BlsAggregateSignature::infinity();
        for kp in keypairs.iter().rev() {
            sig2.add_assign(&kp.sign(msg)).unwrap();
        }

        assert!(sig1.fast_verify(msg, &agg_pk).is_ok());
        assert!(sig2.fast_verify(msg, &agg_pk).is_ok());
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
            sigs.push(kp.sign(msg));
        }

        let aggsigv1 = aggregate_signature_by_2(sigs.iter());
        for aggsig in aggsigv1.iter() {
            sig1.add_assign_aggregate(aggsig);
        }

        let aggsigv2 = aggregate_signature_by_2(sigs.iter().rev());
        for aggsig in aggsigv2.iter() {
            sig2.add_assign_aggregate(aggsig);
        }

        assert!(sig1.fast_verify(msg, &agg_pk).is_ok());
        assert!(sig2.fast_verify(msg, &agg_pk).is_ok());
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
            sigs.push(kp.sign(msg));
        }

        for sig in sigs.iter() {
            sig1.add_assign(sig).unwrap();
        }

        let aggsigv = aggregate_signature_by_2(sigs.iter());
        for aggsig in aggsigv.iter() {
            sig2.add_assign_aggregate(aggsig);
        }

        assert!(sig1.fast_verify(msg, &agg_pk).is_ok());
        assert!(sig2.fast_verify(msg, &agg_pk).is_ok());
        assert_eq!(sig1, sig2);
    }
}
