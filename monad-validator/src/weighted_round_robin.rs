use super::{leader_election::LeaderElection, validator::Address};
use log::warn;
use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Eq, Clone, Copy, Debug)]
struct Voter {
    address: Address,
    voting_power: i64,
    priority: i64,
}

// ordering Voters first on priority, then on address
// higher priority -> smaller in Ord
impl Ord for Voter {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.priority, self.address)
            .cmp(&(other.priority, other.address))
            .reverse()
    }
}

impl PartialOrd for Voter {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Voter {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
    }
}

impl Voter {
    pub fn verified(&self) -> bool {
        self.voting_power > 0
    }
}

#[derive(Debug)]
pub struct WeightedRoundRobin {
    voters: Vec<Voter>,
    leader: usize, // voter idx
    total_voting_power: i64,
}

impl LeaderElection for WeightedRoundRobin {
    fn new() -> Self {
        Self {
            voters: Vec::new(),
            leader: 0,
            total_voting_power: 0,
        }
    }

    fn start_new_epoch(&mut self, voting_powers: Vec<(Address, i64)>) {
        self.voters.reserve(voting_powers.len());
        self.total_voting_power = 0;
        for (addr, vp) in voting_powers.into_iter() {
            let voter = Voter {
                address: addr,
                voting_power: vp,
                priority: vp,
            };
            if !voter.verified() {
                warn!("ignoring voter {:?} with zero voting power", voter.address);
                continue;
            }
            self.voters.push(voter);
            self.total_voting_power += vp;
        }

        if !self.voters.is_empty() {
            self.voters.sort();
            self.increment_one_view();
        }
    }

    fn increment_view(&mut self, view: i64) {
        self.panic_if_empty();
        for _ in 0..view {
            self.increment_one_view();
        }
    }

    fn get_leader(&self) -> &Address {
        self.panic_if_empty();
        &self.voters[self.leader].address
    }

    fn update_voting_power(&mut self, addr: &Address, new_voting_power: i64) -> bool {
        self.panic_if_empty();
        let v = match self.voters.iter_mut().filter(|v| addr == &v.address).next() {
            Some(v) => v,
            None => return false,
        };
        self.total_voting_power -= v.voting_power;
        self.total_voting_power += new_voting_power;
        v.voting_power = new_voting_power;
        v.priority = -self.total_voting_power;
        true
    }
}

impl WeightedRoundRobin {
    fn get_highest_priority_validator(&self) -> usize {
        self.panic_if_empty();
        self.voters
            .iter()
            .enumerate()
            .min_by(|(_, vx), (_, vy)| vx.cmp(vy))
            .map(|(idx, _)| idx)
            .unwrap()
    }

    fn increment_one_view(&mut self) {
        for v in self.voters.iter_mut() {
            v.priority += v.voting_power;
        }
        println!("voters: {:?}", self.voters);
        self.leader = self.get_highest_priority_validator();
        self.voters[self.leader].priority -= self.total_voting_power;
    }

    fn panic_if_empty(&self) {
        if self.voters.is_empty() {
            panic!("empty validators");
        }
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::secp256k1::KeyPair;

    use super::super::leader_election::LeaderElection;
    use super::super::validator::{Address, Validator};

    use super::WeightedRoundRobin;

    fn collect_voting_powers(validators: &Vec<Validator>) -> Vec<(Address, i64)> {
        validators.iter().map(|v| (v.address, v.stake)).collect()
    }

    fn get_key1() -> Vec<u8> {
        hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530").unwrap()
    }

    fn get_key2() -> Vec<u8> {
        hex::decode("6fe42879ece8a11c0df224953ded12cd3c19d0353aaf80057bddfd4d4fc90530").unwrap()
    }

    // expected schedule (basic round robin)
    #[test]
    fn test_basic_round_robin() {
        let v1 = Validator {
            address: Address(1),
            pubkey: KeyPair::from_slice(&get_key1()).unwrap().pubkey(),
            stake: 1,
        };
        let v2 = Validator {
            address: Address(2),
            pubkey: KeyPair::from_slice(&get_key2()).unwrap().pubkey(),
            stake: 1,
        };
        let validators = vec![v1, v2];
        let mut wrr: WeightedRoundRobin = LeaderElection::new();
        wrr.start_new_epoch(collect_voting_powers(&validators));

        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);
        assert!(wrr.get_leader() == &v1.address);
        wrr.increment_view(1);
        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);
        assert!(wrr.get_leader() == &v1.address);
    }

    // expected schedule (weighted round robin)
    #[test]
    fn test_weighted_round_robin() {
        let v1 = Validator {
            address: Address(1),
            pubkey: KeyPair::from_slice(&get_key1()).unwrap().pubkey(),
            stake: 1,
        };
        let v2 = Validator {
            address: Address(2),
            pubkey: KeyPair::from_slice(&get_key2()).unwrap().pubkey(),
            stake: 2,
        };
        let validators = vec![v1, v2];
        let mut wrr: WeightedRoundRobin = LeaderElection::new();
        wrr.start_new_epoch(collect_voting_powers(&validators));

        // expected schedule: (v2, v2, v1), (v2, v2, v1)...
        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);
        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);
        assert!(wrr.get_leader() == &v1.address);
        wrr.increment_view(1);

        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);
        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);
        assert!(wrr.get_leader() == &v1.address);
    }

    // two instances agree on the same schedule
    #[test]
    fn test_agreement() {
        let v1 = Validator {
            address: Address(1),
            pubkey: KeyPair::from_slice(&get_key1()).unwrap().pubkey(),
            stake: 1,
        };
        let v2 = Validator {
            address: Address(2),
            pubkey: KeyPair::from_slice(&get_key2()).unwrap().pubkey(),
            stake: 3,
        };
        let validators = vec![v1, v2];
        let mut wrr1: WeightedRoundRobin = LeaderElection::new();
        let mut wrr2: WeightedRoundRobin = LeaderElection::new();
        wrr1.start_new_epoch(collect_voting_powers(&validators));
        wrr2.start_new_epoch(collect_voting_powers(&validators));

        for _ in 0..20 {
            assert!(wrr1.get_leader() == wrr2.get_leader());
            wrr1.increment_view(1);
            wrr2.increment_view(1);
        }
    }

    // advancing n views equivalent to incrementing 1 view n times
    #[test]
    fn test_increment_views_equivalent() {
        let v1 = Validator {
            address: Address(1),
            pubkey: KeyPair::from_slice(&get_key1()).unwrap().pubkey(),
            stake: 1,
        };
        let v2 = Validator {
            address: Address(2),
            pubkey: KeyPair::from_slice(&get_key2()).unwrap().pubkey(),
            stake: 3,
        };
        let validators = vec![v1, v2];
        let mut wrr1: WeightedRoundRobin = LeaderElection::new();
        let mut wrr2: WeightedRoundRobin = LeaderElection::new();
        wrr1.start_new_epoch(collect_voting_powers(&validators));
        wrr2.start_new_epoch(collect_voting_powers(&validators));

        for _ in 0..20 {
            assert!(wrr1.get_leader() == wrr2.get_leader());
            wrr1.increment_view(1);
            wrr1.increment_view(1);
            wrr2.increment_view(2);
        }
    }

    // update stake
    #[test]
    fn test_update_stake() {
        let mut v1 = Validator {
            address: Address(1),
            pubkey: KeyPair::from_slice(&get_key1()).unwrap().pubkey(),
            stake: 10,
        };
        let v2 = Validator {
            address: Address(2),
            pubkey: KeyPair::from_slice(&get_key2()).unwrap().pubkey(),
            stake: 10,
        };

        let validators = vec![v1, v2];
        let mut wrr: WeightedRoundRobin = LeaderElection::new();
        wrr.start_new_epoch(collect_voting_powers(&validators));

        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);
        assert!(wrr.get_leader() == &v1.address);
        wrr.increment_view(1);

        // now v1 gets slashed to 5
        v1.stake = 5;
        assert!(wrr.update_voting_power(&v1.address, v1.stake));
        assert!(wrr.total_voting_power == 15);

        // we do not change the proposer intra-view; v2 is still the proposer
        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);
        // "compensating" v2/"slashing" v1 by giving v2 one more round
        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);

        // schedule after (v2, v2, v1), (v2, v2, v1)...
        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);
        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);
        assert!(wrr.get_leader() == &v1.address);
        wrr.increment_view(1);

        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);
        assert!(wrr.get_leader() == &v2.address);
        wrr.increment_view(1);
        assert!(wrr.get_leader() == &v1.address);
    }
}
