use crate::types::signature::ConsensusSignature;
use crate::types::voting::VotingQuorum;
use sha2::Digest;

#[derive(Clone, Debug)]
pub struct AggregateSignatures {
    pub sigs: Vec<ConsensusSignature>,
    voting_power: i64,
    max_power: i64,
}

impl AggregateSignatures {
    pub fn new(max_power: i64) -> Self {
        AggregateSignatures {
            sigs: Vec::new(),
            voting_power: 0,
            max_power,
        }
    }
}

impl Default for AggregateSignatures {
    fn default() -> Self {
        Self {
            sigs: Vec::new(),
            voting_power: 0,
            max_power: 10000,
        }
    }
}

fn div_ceil(a: i64) -> i64 {
    // algorithm doesn't work for negative dividend
    if a < 0 {
        panic!("cannot have negative dividend")
    }
    if i64::MAX - a < 3 - 1 {
        panic!("dividend value results in integer overflow")
    }

    (a + (3 - 1)) / 3
}

impl VotingQuorum for AggregateSignatures {
    fn verify_quorum(&self) -> bool {
        let super_majority = div_ceil(2 * self.max_power);
        self.voting_power >= super_majority
    }

    fn current_voting_power(&self) -> i64 {
        self.voting_power
    }

    fn get_hash(&self) -> crate::Hash {
        let mut hasher = sha2::Sha256::new();

        for v in self.sigs.iter() {
            hasher.update(v.0.serialize());
        }

        hasher.finalize().into()
    }

    fn add_signature(&mut self, s: ConsensusSignature, vote_power: i64) {
        self.sigs.push(s);
        self.voting_power += vote_power;
    }
}

#[cfg(test)]
mod test {
    use crate::{signatures::aggregate_signature::div_ceil, types::voting::VotingQuorum};

    use super::AggregateSignatures;

    #[test]
    fn div_ceil_test() {
        assert_eq!(3, div_ceil(8));
        assert_eq!(1, div_ceil(1));
        assert_eq!(0, div_ceil(0));
    }

    #[test]
    fn super_maj_test() {
        let mut s = AggregateSignatures::new(4);

        s.voting_power = 2;
        assert!(!s.verify_quorum());

        s.voting_power = 3;
        assert!(s.verify_quorum());

        s.voting_power = 4;
        assert!(s.verify_quorum());

        s.voting_power = 5;
        assert!(s.verify_quorum());
    }

    #[test]
    #[should_panic]
    fn negative_dividend() {
        let s = AggregateSignatures::new(-4);
        s.verify_quorum();
    }
}
