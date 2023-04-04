use crate::types::signature::ConsensusSignature;
use crate::validation::hashing::Hashable;
use crate::*;

pub trait VotingQuorum: Default + Clone {
    // is voting power >= 2f+1
    fn verify_quorum(&self) -> bool;

    // TODO get the return type from monad-validators later
    fn current_voting_power(&self) -> i64;

    // hash of all the signatures
    fn get_hash(&self) -> Hash;

    // add the signature from a signed vote message and its voting power to the
    // aggregate
    fn add_signature(&mut self, s: ConsensusSignature, vote_power: i64);

    fn get_signatures(&self) -> Vec<&ConsensusSignature>;
}

use sha2::Digest;

#[derive(Copy, Clone, Debug, Default)]
pub struct VoteInfo {
    pub id: BlockId,
    pub round: Round,
    pub parent_id: BlockId,
    pub parent_round: Round,
}

pub struct VoteInfoIter<'a> {
    pub v: &'a VoteInfo,
    pub index: usize,
}

impl<'a> Iterator for VoteInfoIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.index {
            0 => Some(self.v.id.0.as_bytes()),
            1 => Some(self.v.round.as_bytes()),
            2 => Some(self.v.parent_id.0.as_bytes()),
            3 => Some(self.v.parent_round.as_bytes()),
            _ => None,
        };

        self.index += 1;
        result
    }
}

impl<'a> Hashable<'a> for &'a VoteInfo {
    type DataIter = VoteInfoIter<'a>;

    fn msg_parts(&self) -> Self::DataIter {
        VoteInfoIter { v: self, index: 0 }
    }
}

impl VoteInfo {
    // TODO make the hasher a parameter
    pub fn get_hash(&self) -> Hash {
        let mut hasher = sha2::Sha256::new();
        for m in (&self).msg_parts() {
            hasher.update(m);
        }

        hasher.finalize().into()
    }
}

#[cfg(test)]
mod test {
    use crate::Hash;

    use super::VoteInfo;
    use sha2::Digest;

    pub fn hash_vote(v: &VoteInfo) -> Hash {
        let mut hasher = sha2::Sha256::new();
        hasher.update(v.id.0);
        hasher.update(v.round);
        hasher.update(v.parent_id.0);
        hasher.update(v.parent_round);

        hasher.finalize().into()
    }

    #[test]
    fn voteinfo_hash() {
        let vi = VoteInfo::default();

        let h1 = vi.get_hash();
        let h2 = hash_vote(&vi);

        assert_eq!(h1, h2);
    }
}
