use std::{cmp::Ordering, fmt::Debug};

use monad_consensus_types::signature_collection::SignatureCollection;
use monad_types::{NodeId, Round, Stake};
use tracing::warn;

use super::leader_election::LeaderElection;
use crate::validator_set::{ValidatorSetType, ValidatorsEpochMapping};

#[derive(Eq, Clone, Copy, Debug)]
struct Voter {
    address: NodeId,
    voting_power: Stake,
    priority: Stake,
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
        self.voting_power > Stake(0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WeightedRoundRobin {
    voters: Vec<Voter>,
    leader: usize, // voter idx
    total_voting_power: Stake,
}

impl LeaderElection for WeightedRoundRobin {
    fn new() -> Self {
        Self {
            voters: Vec::new(),
            leader: 0,
            total_voting_power: Stake(0),
        }
    }

    fn get_leader<VT, SCT>(
        &self,
        round: Round,
        epoch_length: Round,
        validators_epoch_mapping: &ValidatorsEpochMapping<VT, SCT>,
    ) -> NodeId
    where
        VT: ValidatorSetType,
        SCT: SignatureCollection,
    {
        let round_epoch = round.get_epoch_num(epoch_length);

        if let Some(validator_set) = validators_epoch_mapping.get_val_set(&round_epoch) {
            let validator_list = validator_set.get_list();
            validator_list[round.0 as usize % validator_list.len()]
        } else {
            // TODO: fix panic
            panic!(
                "validator set for epoch #{} not in validator set mapping",
                round_epoch.0
            )
        }
    }
}

impl WeightedRoundRobin {
    pub fn get_highest_priority_validator(&self) -> usize {
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
        self.leader = self.get_highest_priority_validator();
        self.voters[self.leader].priority -= self.total_voting_power;
    }

    fn panic_if_empty(&self) {
        if self.voters.is_empty() {
            panic!("empty validators");
        }
    }

    pub fn start_new_epoch(&mut self, voting_powers: Vec<(NodeId, Stake)>) {
        self.voters.reserve(voting_powers.len());
        self.total_voting_power = Stake(0);
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

    pub fn increment_view(&mut self, view: Round) {
        self.panic_if_empty();
        for _ in 0..view.0 {
            self.increment_one_view();
        }
    }

    pub fn update_voting_power(&mut self, addr: &NodeId, new_voting_power: Stake) -> bool {
        self.panic_if_empty();
        let v = match self.voters.iter_mut().find(|v| addr == &v.address) {
            Some(v) => v,
            None => return false,
        };
        self.total_voting_power -= v.voting_power;
        self.total_voting_power += new_voting_power;
        v.voting_power = new_voting_power;
        v.priority = Stake(-self.total_voting_power.0);
        true
    }
}

// #[cfg(test)]
// mod tests {
//     use monad_crypto::secp256k1::KeyPair;
//     use monad_types::{NodeId, Round, Stake};

//     use super::super::leader_election::LeaderElection;

//     use super::WeightedRoundRobin;

//     fn get_key1() -> [u8; 32] {
//         [126; 32]
//     }

//     fn get_key2() -> [u8; 32] {
//         [127; 32]
//     }

//     // expected schedule (basic round robin)
//     #[test]
//     fn test_basic_round_robin() {
//         let v1 = (
//             NodeId(KeyPair::from_bytes(&mut get_key1()).unwrap().pubkey()),
//             Stake(1),
//         );
//         let v2 = (
//             NodeId(KeyPair::from_bytes(&mut get_key2()).unwrap().pubkey()),
//             Stake(1),
//         );
//         let validators = vec![v1, v2];
//         let mut wrr: WeightedRoundRobin = LeaderElection::new();
//         wrr.start_new_epoch(validators);

//         assert!(wrr.get_leader() == v2.0);
//         wrr.increment_view(Round(1));
//         assert!(wrr.get_leader() == v1.0);
//         wrr.increment_view(Round(1));
//         assert!(wrr.get_leader() == v2.0);
//         wrr.increment_view(Round(1));
//         assert!(wrr.get_leader() == v1.0);
//     }

//     // expected schedule (weighted round robin)
//     #[test]
//     fn test_weighted_round_robin() {
//         let v1 = (
//             NodeId(KeyPair::from_bytes(&mut get_key1()).unwrap().pubkey()),
//             Stake(1),
//         );
//         let v2 = (
//             NodeId(KeyPair::from_bytes(&mut get_key2()).unwrap().pubkey()),
//             Stake(2),
//         );
//         let validators = vec![v1, v2];
//         let mut wrr: WeightedRoundRobin = LeaderElection::new();
//         wrr.start_new_epoch(validators);

//         // expected schedule: (v2, v2, v1), (v2, v2, v1)...
//         assert!(wrr.get_leader() == &v2.0);
//         wrr.increment_view(Round(1));
//         assert!(wrr.get_leader() == &v2.0);
//         wrr.increment_view(Round(1));
//         assert!(wrr.get_leader() == &v1.0);
//         wrr.increment_view(Round(1));

//         assert!(wrr.get_leader() == &v2.0);
//         wrr.increment_view(Round(1));
//         assert!(wrr.get_leader() == &v2.0);
//         wrr.increment_view(Round(1));
//         assert!(wrr.get_leader() == &v1.0);
//     }

//     // two instances agree on the same schedule
//     #[test]
//     fn test_agreement() {
//         let v1 = (
//             NodeId(KeyPair::from_bytes(&mut get_key1()).unwrap().pubkey()),
//             Stake(1),
//         );
//         let v2 = (
//             NodeId(KeyPair::from_bytes(&mut get_key2()).unwrap().pubkey()),
//             Stake(3),
//         );
//         let validators = vec![v1, v2];
//         let mut wrr1: WeightedRoundRobin = LeaderElection::new();
//         let mut wrr2: WeightedRoundRobin = LeaderElection::new();
//         wrr1.start_new_epoch(validators.clone());
//         wrr2.start_new_epoch(validators);

//         for _ in 0..20 {
//             assert!(wrr1.get_leader() == wrr2.get_leader());
//             wrr1.increment_view(Round(1));
//             wrr2.increment_view(Round(1));
//         }
//     }

//     // advancing n views equivalent to incrementing 1 view n times
//     #[test]
//     fn test_increment_views_equivalent() {
//         let v1 = (
//             NodeId(KeyPair::from_bytes(&mut get_key1()).unwrap().pubkey()),
//             Stake(1),
//         );
//         let v2 = (
//             NodeId(KeyPair::from_bytes(&mut get_key2()).unwrap().pubkey()),
//             Stake(3),
//         );
//         let validators = vec![v1, v2];
//         let mut wrr1: WeightedRoundRobin = LeaderElection::new();
//         let mut wrr2: WeightedRoundRobin = LeaderElection::new();
//         wrr1.start_new_epoch(validators.clone());
//         wrr2.start_new_epoch(validators);

//         for _ in 0..20 {
//             assert!(wrr1.get_leader() == wrr2.get_leader());
//             wrr1.increment_view(Round(1));
//             wrr1.increment_view(Round(1));
//             wrr2.increment_view(Round(2));
//         }
//     }

//     // update stake
//     #[test]
//     fn test_update_stake() {
//         let mut v1 = (
//             NodeId(KeyPair::from_bytes(&mut get_key1()).unwrap().pubkey()),
//             Stake(10),
//         );
//         let v2 = (
//             NodeId(KeyPair::from_bytes(&mut get_key2()).unwrap().pubkey()),
//             Stake(10),
//         );

//         let validators = vec![v1, v2];
//         let mut wrr: WeightedRoundRobin = LeaderElection::new();
//         wrr.start_new_epoch(validators);

//         assert!(wrr.get_leader() == &v2.0);
//         wrr.increment_view(Round(1));
//         assert!(wrr.get_leader() == &v1.0);
//         wrr.increment_view(Round(1));

//         // now v1 gets slashed to 5
//         v1.1 = Stake(5);
//         assert!(wrr.update_voting_power(&v1.0, v1.1));
//         assert!(wrr.total_voting_power == Stake(15));

//         // we do not change the proposer intra-view; v2 is still the proposer
//         assert!(wrr.get_leader() == &v2.0);
//         wrr.increment_view(Round(1));
//         // "compensating" v2/"slashing" v1 by giving v2 one more round
//         assert!(wrr.get_leader() == &v2.0);
//         wrr.increment_view(Round(1));

//         // schedule after (v2, v2, v1), (v2, v2, v1)...
//         assert!(wrr.get_leader() == &v2.0);
//         wrr.increment_view(Round(1));
//         assert!(wrr.get_leader() == &v2.0);
//         wrr.increment_view(Round(1));
//         assert!(wrr.get_leader() == &v1.0);
//         wrr.increment_view(Round(1));

//         assert!(wrr.get_leader() == &v2.0);
//         wrr.increment_view(Round(1));
//         assert!(wrr.get_leader() == &v2.0);
//         wrr.increment_view(Round(1));
//         assert!(wrr.get_leader() == &v1.0);
//     }
// }
