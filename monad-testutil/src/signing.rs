use std::marker::PhantomData;

use monad_consensus::validation::signing::Unverified;
use monad_consensus_types::{
    block::{Block, TransactionList},
    ledger::LedgerCommitInfo,
    quorum_certificate::{genesis_vote_info, QuorumCertificate},
    signature::{SignatureBuilder, SignatureCollection},
    validation::Hasher,
};
use monad_crypto::{
    bls12_381::{BlsKeyPair, BlsPubKey},
    secp256k1::{Error as SecpError, KeyPair, PubKey, SecpSignature},
    Signature,
};
use monad_types::{Hash, NodeId, Round};
use monad_validator::validator_property::{ValidatorSetProperty, ValidatorSetPropertyType};
use sha2::{Digest, Sha256};

#[derive(Clone, Default, Debug)]
pub struct MockSignatures {
    pubkey: Vec<PubKey>,
}

#[derive(Debug)]
struct MockSignatureError;

impl std::fmt::Display for MockSignatureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for MockSignatureError {}

impl MockSignatures {
    pub fn with_pubkeys(pubkeys: &[PubKey]) -> Self {
        Self {
            pubkey: pubkeys.to_vec(),
        }
    }
}

impl SignatureCollection for MockSignatures {
    type SignatureError = SecpError;
    type SignatureType = SecpSignature;

    fn new(
        _sigs: SignatureBuilder<Self>,
        _validator_list: &[(NodeId, BlsPubKey)],
        _msg: &[u8],
    ) -> Result<Self, Self::SignatureError> {
        Ok(Self { pubkey: Vec::new() })
    }

    fn get_hash(&self) -> Hash {
        Default::default()
    }

    fn verify(
        &self,
        validator_list: &[(NodeId, BlsPubKey)],
        _msg: &[u8],
    ) -> Result<Vec<NodeId>, Self::SignatureError> {
        Ok(validator_list.iter().map(|(nodeid, _)| *nodeid).collect())
    }

    fn num_signatures(&self) -> usize {
        0
    }
}

pub fn hash<T: SignatureCollection>(b: &Block<T>) -> Hash {
    let mut hasher = sha2::Sha256::new();
    hasher.update(b.author.0.bytes());
    hasher.update(b.round);
    hasher.update(&b.payload.0);
    hasher.update(b.qc.info.vote.id.0);
    hasher.update(b.qc.signatures.get_hash());

    Hash(hasher.finalize().into())
}

pub fn node_id() -> NodeId {
    let mut privkey: [u8; 32] = [127; 32];
    let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
    NodeId(keypair.pubkey())
}

pub fn create_keys(num_keys: u32) -> Vec<KeyPair> {
    let mut res = Vec::new();
    for i in 0..num_keys {
        let keypair = get_key(i.into());
        res.push(keypair);
    }

    res
}

pub fn create_keys_bls(num_keys: u32) -> Vec<BlsKeyPair> {
    let mut res = Vec::new();
    for i in 0..num_keys {
        let keypair = get_key_bls(i.into());
        res.push(keypair);
    }

    res
}

pub fn get_genesis_config<'k, H: Hasher, T: SignatureCollection>(
    keys: impl Iterator<Item = &'k KeyPair>,
    validators_property: &ValidatorSetProperty,
) -> (Block<T>, T) {
    let genesis_txn = TransactionList::default();
    let genesis_prime_qc = QuorumCertificate::<T>::genesis_prime_qc::<H>();
    let genesis_block = Block::<T>::new::<H>(
        // FIXME init from genesis config, don't use random key
        NodeId(KeyPair::from_bytes(&mut [0xBE_u8; 32]).unwrap().pubkey()),
        Round(0),
        &genesis_txn,
        &genesis_prime_qc,
    );

    let genesis_lci = LedgerCommitInfo::new::<H>(None, &genesis_vote_info(genesis_block.get_id()));
    let msg = H::hash_object(&genesis_lci);

    let mut builder = SignatureBuilder::new();
    for k in keys {
        let idx = validators_property.get_index(&NodeId(k.pubkey())).unwrap();
        let sig = T::SignatureType::sign(msg.as_ref(), k);
        builder.add_signature(idx, sig);
    }

    let sigs = T::new(builder, validators_property.get_voting_list(), msg.as_ref()).unwrap();
    (genesis_block, sigs)
}

pub struct TestSigner<S> {
    _p: PhantomData<S>,
}

impl TestSigner<SecpSignature> {
    pub fn sign_object<T>(o: T, msg: &[u8], key: &KeyPair) -> Unverified<SecpSignature, T> {
        let sig = key.sign(msg);

        Unverified::new(o, sig)
    }
}

pub fn get_key(seed: u64) -> KeyPair {
    let mut hasher = Sha256::new();
    hasher.update(seed.to_le_bytes());
    let mut hash = hasher.finalize();
    KeyPair::from_bytes(&mut hash).unwrap()
}

pub fn get_key_bls(seed: u64) -> BlsKeyPair {
    let mut hasher = Sha256::new();
    hasher.update(seed.to_le_bytes());
    let mut hash = hasher.finalize();
    BlsKeyPair::from_bytes(&mut hash).unwrap()
}
