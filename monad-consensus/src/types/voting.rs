use crate::types::signature::ConsensusSignature;
use crate::*;

pub trait VotingQuorum: Default + Clone {
    // is voting power >= 2f+1
    fn verify_quorum(&self) -> bool;

    // TODO get the return type from monad-validators later
    fn current_voting_power(&self) -> i64;

    fn get_hash(&self) -> Hash;

    // this function is used by the leader to add signed VoteMsgs from replicas
    // to the QC it is creating.
    // Pass in the whole VoteMsg and let the implementation deal with how the signature
    // is extracted from that or what portion is used for the adding the signature
    fn add_signature(&mut self, s: ConsensusSignature);
}

use sha2::Digest;

#[derive(Copy, Clone, Debug, Default)]
pub struct VoteInfo {
    pub id: BlockId,
    pub round: Round,
    pub parent_id: BlockId,
    pub parent_round: Round,
}

impl VoteInfo {
    // TODO will use something from monad-crypto later...
    pub fn get_hash(&self) -> Hash {
        let mut hasher = sha2::Sha256::new();
        hasher.update(self.id.0);
        hasher.update(self.round);
        hasher.update(self.parent_id.0);
        hasher.update(self.parent_round);

        hasher.finalize().into()
    }
}
