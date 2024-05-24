use std::{collections::BTreeMap, marker::PhantomData};

use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Round, Stake};

use crate::leader_election::LeaderElection;

#[derive(Clone)]
pub struct WeightedRoundRobin<PT: PubKey> {
    _phantom: PhantomData<PT>,
}

impl<PT: PubKey> Default for WeightedRoundRobin<PT> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<PT: PubKey> LeaderElection for WeightedRoundRobin<PT> {
    type NodeIdPubKey = PT;

    /// Computes the leader using interleaved weighted round-robin (IWRR) scheduling
    /// # Panics
    /// Panics if `validators.is_empty()` or if `validators` does not contain an element whose stake is > 0, because
    /// there is no sensible choice for leader in either of those cases.
    fn get_leader(&self, round: Round, validators: &BTreeMap<NodeId<PT>, Stake>) -> NodeId<PT> {
        let max_stake = validators
            .iter()
            .map(|(_, stake)| stake.0)
            .max()
            .expect("get_leader called with empty validator set");

        let total_valid_stake: i64 = validators
            .iter()
            .filter_map(|(_, stake)| {
                // we do not include validators with stake <= 0 in the sum because they should not get a chance to vote
                if stake.0 > 0 {
                    Some(stake.0)
                } else {
                    None
                }
            })
            .sum();

        // the size of the complete IWRR schedule is `total_valid_stake`,
        // so we can think of leader election as round-robin into the computed schedule
        let schedule_index: usize = (round.0 % total_valid_stake as u64) as usize;

        (1i64..=max_stake)
            .flat_map(|cycle| {
                validators.iter().filter_map(move |(validator, stake)| {
                    if cycle <= stake.0 {
                        Some(*validator)
                    } else {
                        None
                    }
                })
            })
            .nth(schedule_index)
            .expect("no validator had positive stake")
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::NopPubKey;
    use test_case::test_case;

    use super::*;

    #[test_case(vec![('A', 1), ('B', 1), ('C', 1)],    vec!['A', 'B', 'C']; "equal stakes")]
    #[test_case(vec![('A', 1), ('B', 1), ('C', 1)],    vec!['A', 'B', 'C']; "test equal stakes")]
    #[test_case(vec![('A', 1), ('B', 0), ('C', 1)],    vec!['A', 'C'];      "test unstaked schedule")]
    #[test_case(vec![('A', 1), ('B', 2), ('C', 1)],    vec!['A', 'B', 'C', 'B']; "test validator with more stake")]
    #[test_case(vec![],                                vec![]; "test empty schedule")]
    #[test_case(vec![('A', 2), ('B', 2), ('C', 2)],    vec!['A', 'B', 'C']; "test equal schedule with more stake")]
    #[test_case(vec![('A', 1), ('B', 2), ('C', 3)],    vec!['A', 'B', 'C', 'B', 'C', 'C']; "test unequal schedule")]
    #[test_case(vec![('A', 10), ('B', 2), ('C', 3)],   vec![ 'A', 'B', 'C', 'A', 'B', 'C', 'A', 'C', 'A', 'A', 'A', 'A', 'A', 'A', 'A']; "test big stake")]
    #[test_case(vec![('A', -10), ('B', 2), ('C', 3)],  vec!['B', 'C', 'B', 'C', 'C']; "test negative stake")]
    fn test_weighted_round_robin(validator_set: Vec<(char, i64)>, expected_schedule: Vec<char>) {
        let l = WeightedRoundRobin::default();
        let validator_set = validator_set
            .into_iter()
            .map(|(validator, stake)| {
                (
                    NodeId::new(NopPubKey::from_bytes(&[validator as u8; 32]).unwrap()),
                    Stake(stake),
                )
            })
            .collect();
        let expected_schedule = expected_schedule
            .into_iter()
            .map(|validator| NodeId::new(NopPubKey::from_bytes(&[validator as u8; 32]).unwrap()))
            .collect::<Vec<_>>();
        let schedule_size = expected_schedule.len();

        for round in 0..(2 * schedule_size) {
            assert_eq!(
                l.get_leader(Round(round as u64), &validator_set),
                expected_schedule[round % schedule_size]
            );
        }
    }
}
