use monad_consensus::types::block::Block;
use monad_consensus::types::signature::ConsensusSignature;
use monad_consensus::types::voting::VotingQuorum;
use monad_consensus::Hash;

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
