use crate::types::signature::ConsensusSignature;
use crate::types::voting::VotingQuorum;
use crate::Hash;

#[derive(Clone, Default, Debug)]
pub struct MockSignatures();
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

    fn add_signature(&mut self, _s: ConsensusSignature) {}
}
