use std::collections::{HashMap, HashSet};
use std::error;

use monad_types::{NodeId, Round};

use super::leader_election::LeaderElection;
use super::validator::Validator;
use std::fmt;

pub type Result<T> = std::result::Result<T, ValidatorSetError>;

#[derive(Debug)]
pub enum ValidatorSetError {
    DuplicateValidator(String),
}

impl fmt::Display for ValidatorSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateValidator(s) => write!(f, "Duplicate validator entry: {}", s),
        }
    }
}

impl error::Error for ValidatorSetError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

#[derive(Debug)]
pub struct ValidatorSet<T: LeaderElection> {
    validators: HashMap<NodeId, Validator>,
    leader_election: T,
    total_stake: i64,
    round: Round, // monotonically increasing: initial 1
}

impl<T: LeaderElection> ValidatorSet<T> {
    pub fn new(validators: Vec<Validator>) -> Result<Self> {
        let mut vmap = HashMap::new();
        for v in validators.into_iter() {
            let entry = vmap.entry(NodeId(v.pubkey)).or_insert(v);
            if entry.stake != v.stake {
                return Err(ValidatorSetError::DuplicateValidator(format!(
                    "{:?}",
                    v.pubkey
                )));
            }
        }

        let mut election = T::new();
        election.start_new_epoch(vmap.values().map(|v| (NodeId(v.pubkey), v.stake)).collect());
        let total_stake: i64 = vmap.values().map(|v| v.stake).sum();

        Ok(ValidatorSet {
            validators: vmap,
            leader_election: election,
            total_stake,
            round: Round(1),
        })
    }

    pub fn get_members(&self) -> &HashMap<NodeId, Validator> {
        &self.validators
    }

    pub fn is_member(&self, addr: &NodeId) -> bool {
        self.validators.contains_key(addr)
    }

    pub fn has_super_majority_votes<'a, I>(&self, addrs: I) -> bool
    where
        I: IntoIterator<Item = &'a NodeId>,
    {
        let mut duplicates = HashSet::new();

        let mut voter_stake: i64 = 0;
        for addr in addrs {
            if let Some(v) = self.validators.get(addr) {
                voter_stake += v.stake;
                assert!(duplicates.insert(addr));
            }
        }
        voter_stake >= self.total_stake * 2 / 3 + 1
    }

    pub fn has_honest_vote(&self, addrs: &Vec<NodeId>) -> bool {
        assert_eq!(addrs.iter().collect::<HashSet<_>>().len(), addrs.len());
        addrs
            .iter()
            .filter_map(|addr| self.validators.get(addr).map(|v| v.stake))
            .sum::<i64>()
            >= self.total_stake / 3 + 1
    }

    pub fn get_leader(&mut self, round: Round) -> &NodeId {
        // FIXME switch back to weighted round robin
        let mut validators = self.validators.iter().collect::<Vec<_>>();
        validators.sort_by_key(|(pubkey, _)| *pubkey);
        validators[round.0 as usize % self.validators.len()].0
        // if round < self.round {
        //     panic!("round reversed {:?}->{:?}", self.round, round);
        // }
        // if round > self.round {
        //     self.leader_election.increment_view(round - self.round);
        // }
        // self.leader_election.get_leader()
    }
    // fn udpate_stake(&mut self, validator: Validator) -> bool {}
    // fn udpate_validators(&mut self, validators: Vec<Validator>) {}
}

#[cfg(test)]
mod test {

    use crate::{validator::Validator, weighted_round_robin::WeightedRoundRobin};

    use super::ValidatorSet;
    use monad_crypto::secp256k1::KeyPair;
    use monad_types::{NodeId, Round};

    #[test]
    fn test_membership() {
        let mut privkey: [u8; 32] = [100; 32];
        let keypair1 = KeyPair::from_bytes(&mut privkey).unwrap();

        let v1 = Validator {
            pubkey: keypair1.pubkey().clone(),
            stake: 1,
        };

        let v1_ = Validator {
            pubkey: keypair1.pubkey().clone(),
            stake: 2,
        };

        privkey = [101; 32];
        let v2 = Validator {
            pubkey: KeyPair::from_bytes(&mut privkey).unwrap().pubkey(),
            stake: 2,
        };

        let validators_duplicate = vec![v1.clone(), v1_];
        let _vs_err = ValidatorSet::<WeightedRoundRobin>::new(validators_duplicate).unwrap_err();

        let validators = vec![v1, v2];
        let vs = ValidatorSet::<WeightedRoundRobin>::new(validators).unwrap();
        assert!(vs.is_member(&NodeId(keypair1.pubkey())));

        let mut pkey3: [u8; 32] = [102; 32];
        let pubkey3 = KeyPair::from_bytes(&mut pkey3).unwrap().pubkey();
        assert!(!vs.is_member(&NodeId(pubkey3)));
    }

    #[test]
    fn test_super_maj() {
        let mut pkey1: [u8; 32] = [100; 32];
        let mut pkey2: [u8; 32] = [101; 32];
        let v1 = Validator {
            pubkey: KeyPair::from_bytes(&mut pkey1).unwrap().pubkey(),
            stake: 1,
        };

        let v2 = Validator {
            pubkey: KeyPair::from_bytes(&mut pkey2).unwrap().pubkey(),
            stake: 3,
        };

        let mut pkey3: [u8; 32] = [102; 32];
        let pubkey3 = KeyPair::from_bytes(&mut pkey3).unwrap().pubkey();

        let validators = vec![v1.clone(), v2.clone()];
        let vs = ValidatorSet::<WeightedRoundRobin>::new(validators).unwrap();
        assert!(vs.has_super_majority_votes(&vec![NodeId(v2.pubkey)]));
        assert!(!vs.has_super_majority_votes(&vec![NodeId(v1.pubkey)]));
        assert!(vs.has_super_majority_votes(&vec![NodeId(v2.pubkey), NodeId(pubkey3)]));
        // Address(3) is a non-member
    }

    #[test]
    fn test_honest_vote() {
        let mut pkey1: [u8; 32] = [100; 32];
        let mut pkey2: [u8; 32] = [101; 32];
        let v1 = Validator {
            pubkey: KeyPair::from_bytes(&mut pkey1).unwrap().pubkey(),
            stake: 1,
        };

        let v2 = Validator {
            pubkey: KeyPair::from_bytes(&mut pkey2).unwrap().pubkey(),
            stake: 2,
        };

        let validators = vec![v1.clone(), v2.clone()];
        let vs = ValidatorSet::<WeightedRoundRobin>::new(validators).unwrap();
        assert!(!vs.has_honest_vote(&vec![NodeId(v1.pubkey)]));
        assert!(vs.has_honest_vote(&vec![NodeId(v2.pubkey)]));
    }

    #[test]
    fn test_get_leader() {
        let mut pkey1: [u8; 32] = [100; 32];
        let pubkey1 = KeyPair::from_bytes(&mut pkey1).unwrap().pubkey();
        let v1 = Validator {
            pubkey: pubkey1,
            stake: 1,
        };

        let mut pkey2: [u8; 32] = [101; 32];
        let pubkey2 = KeyPair::from_bytes(&mut pkey2).unwrap().pubkey();
        let v2 = Validator {
            pubkey: pubkey2,
            stake: 1,
        };

        let validators = vec![v1.clone(), v2.clone()];
        let mut vs = ValidatorSet::<WeightedRoundRobin>::new(validators).unwrap();
        assert_eq!(vs.get_leader(Round(1)), &NodeId(pubkey2));
        assert_eq!(vs.get_leader(Round(3)), &NodeId(pubkey2));
        assert_eq!(vs.get_leader(Round(4)), &NodeId(pubkey1));
    }
}
