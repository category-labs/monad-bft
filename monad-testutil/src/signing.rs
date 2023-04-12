use std::marker::PhantomData;

use monad_consensus::types::block::Block;
use monad_consensus::types::signature::SignatureCollection;
use monad_consensus::validation::signing::Unverified;
use monad_crypto::secp256k1::{Error, KeyPair, PubKey, SecpSignature};
use monad_types::{Hash, NodeId};

#[derive(Clone, Default, Debug)]
pub struct MockSignatures;
impl SignatureCollection for MockSignatures {
    type SignatureType = SecpSignature;

    fn new() -> Self {
        MockSignatures {}
    }

    fn get_hash(&self) -> Hash {
        Default::default()
    }

    fn add_signature(&mut self, _s: SecpSignature) {}

    fn verify_signatures(&self, _msg: &[u8]) -> Result<(), Error> {
        Ok(())
    }

    fn get_pubkeys(&self, _msg: &[u8]) -> Result<Vec<PubKey>, Error> {
        Ok(Vec::new())
    }

    fn num_signatures(&self) -> usize {
        0
    }
}

use sha2::Digest;

pub fn hash<T: SignatureCollection>(b: &Block<T>) -> Hash {
    let mut hasher = sha2::Sha256::new();
    hasher.update(b.author.0.into_bytes());
    hasher.update(b.round);
    hasher.update(&b.payload.0);
    hasher.update(b.qc.info.vote.id.0);
    hasher.update(b.qc.signatures.get_hash());

    hasher.finalize().into()
}

pub fn node_id() -> NodeId {
    let privkey =
        hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530").unwrap();
    let keypair = KeyPair::from_slice(&privkey).unwrap();
    NodeId(keypair.pubkey())
}

pub fn create_keys(num_keys: u32) -> Vec<KeyPair> {
    assert!(num_keys < 255);
    let mut res = Vec::new();
    for i in 0..num_keys {
        let k: [u8; 32] = [(i + 1) as u8; 32];
        let keypair = KeyPair::from_slice(&k).unwrap();

        res.push(keypair);
    }

    res
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

pub fn get_key(seed: &str) -> KeyPair {
    let privkey = hex::decode(seed.repeat(64)).unwrap();
    KeyPair::from_slice(&privkey).unwrap()
}
