use crate::validation::hashing::{Hashable, Hasher};
use crate::*;

#[derive(Copy, Clone, Debug, Default)]
pub struct VoteInfo {
    pub id: BlockId,
    pub round: Round,
    pub parent_id: BlockId,
    pub parent_round: Round,
}

impl Hashable for &VoteInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.id.0.as_bytes());
        state.update(self.round.as_bytes());
        state.update(self.parent_id.0.as_bytes());
        state.update(self.parent_round.as_bytes());
    }
}

#[cfg(test)]
mod test {
    use crate::{
        validation::hashing::{Hasher, Sha256Hash},
        Hash,
    };

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

        let h1 = Sha256Hash::hash_object(&vi);
        let h2 = hash_vote(&vi);

        assert_eq!(h1, h2);
    }
}
