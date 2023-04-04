use monad_consensus::types::block::Block;
use monad_consensus::types::signature::ConsensusSignature;
use monad_consensus::types::voting::VotingQuorum;
use monad_consensus::validation::hashing::Hashable;
use monad_consensus::validation::signing::Signable;
use monad_consensus::{Hash, NodeId};
use monad_crypto::secp256k1::KeyPair;

#[derive(Clone, Default, Debug)]
pub struct MockSignatures;
impl VotingQuorum for MockSignatures {
    fn verify_quorum(&self) -> bool {
        true
    }

    fn current_voting_power(&self) -> i64 {
        0
    }

    fn get_hash(&self) -> Hash {
        Default::default()
    }

    fn add_signature(&mut self, _s: ConsensusSignature, _vote_power: i64) {}

    fn get_signatures(&self) -> Vec<&ConsensusSignature> {
        Default::default()
    }
}

use sha2::Digest;

pub fn hash<T: VotingQuorum>(b: &Block<T>) -> Hash {
    let mut hasher = sha2::Sha256::new();
    hasher.update(b.author);
    hasher.update(b.round);
    hasher.update(&b.payload.0);
    hasher.update(b.qc.info.vote.id.0);
    hasher.update(b.qc.signatures.get_hash());

    hasher.finalize().into()
}

pub struct Signer;
impl Signer {
    pub fn sign_object<T: Signable>(o: T, msg: &[u8], key: KeyPair) -> <T as Signable>::Output {
        let sig = key.sign(msg);

        let id = NodeId(0);
        o.signed_object(id, ConsensusSignature(sig))
    }
}

pub struct Hasher;
impl Hasher {
    pub fn hash_object<'a, T: Hashable<'a>>(o: T) -> [u8; 32] {
        let mut hasher = sha2::Sha256::new();

        for f in o.msg_parts() {
            hasher.update(f);
        }
        hasher.finalize().into()
    }
}

pub fn get_key(seed: &str) -> KeyPair {
    let privkey = hex::decode(seed.repeat(64)).unwrap();
    KeyPair::from_slice(&privkey).unwrap()
}
