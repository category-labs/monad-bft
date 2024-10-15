use std::{collections::BTreeMap, marker::PhantomData};

use monad_crypto::{
    certificate_signature::PubKey,
    hasher::{Hasher, HasherType},
};
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

fn randomize(x: u64, m: u64) -> u64 {
    let mut hasher = HasherType::new();
    hasher.update(x.to_le_bytes());
    let hash = hasher.hash().0;
    (u64::from_le_bytes(hash[0..8].try_into().unwrap())
        ^ u64::from_le_bytes(hash[8..16].try_into().unwrap())
        ^ u64::from_le_bytes(hash[16..24].try_into().unwrap())
        ^ u64::from_le_bytes(hash[24..32].try_into().unwrap()))
        % m
}

impl<PT: PubKey> LeaderElection for WeightedRoundRobin<PT> {
    type NodeIdPubKey = PT;

    /// Computes the leader using randomized interleaved weighted round-robin scheduling
    /// # Panics
    /// Panics if `validators.is_empty()` or if `validators` does not contain an element whose stake is > 0, because
    /// there is no sensible choice for leader in either of those cases.
    fn get_leader(&self, round: Round, validators: &BTreeMap<NodeId<PT>, Stake>) -> NodeId<PT> {
        let mut total_stakes = 0_u64;
        let stake_bounds: Vec<_> = validators
            .iter()
            .filter_map(|(node_id, stake)| {
                if stake.0 > 0 {
                    total_stakes += stake.0 as u64;
                    Some((node_id, total_stakes))
                } else {
                    None
                }
            })
            .collect();
        if stake_bounds.is_empty() {
            panic!("no validator has positive stake");
        }

        let stake_index = randomize(round.0, total_stakes);
        let upper_bound = stake_bounds
            .binary_search_by(|&(_, stake_bound)| {
                if stake_bound > stake_index {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .unwrap_err();
        *stake_bounds[upper_bound].0
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::NopPubKey;
    use test_case::test_case;

    use super::*;

    #[test_case(vec![('A', 1), ('B', 1), ('C', 1)]; "equal stakes")]
    #[test_case(vec![('A', 1), ('B', 1), ('C', 1)]; "test equal stakes")]
    #[test_case(vec![('A', 1), ('B', 0), ('C', 1)];      "test unstaked schedule")]
    #[test_case(vec![('A', 1), ('B', 2), ('C', 1)]; "test validator with more stake")]
    #[test_case(vec![('A', 2), ('B', 2), ('C', 2)]; "test equal schedule with more stake")]
    #[test_case(vec![('A', 1), ('B', 2), ('C', 3)]; "test unequal schedule")]
    #[test_case(vec![('A', 10), ('B', 2), ('C', 3)]; "test big stake")]
    #[test_case(vec![('A', -10), ('B', 2), ('C', 3)]; "test negative stake")]
    fn test_weighted_round_robin(validator_set: Vec<(char, i64)>) {
        let num_iterations = 10000_u64;
        let l = WeightedRoundRobin::default();
        let total_stakes = validator_set
            .iter()
            .filter_map(|(_, stake)| if *stake > 0 { Some(*stake) } else { None })
            .sum::<i64>() as u64;
        let expected_num_picked = validator_set
            .iter()
            .map(|(validator, stake)| {
                (
                    NodeId::new(NopPubKey::from_bytes(&[*validator as u8; 32]).unwrap()),
                    if *stake > 0 {
                        num_iterations * *stake as u64 / total_stakes
                    } else {
                        0
                    },
                )
            })
            .collect::<Vec<_>>();

        let mut num_picked = vec![0; validator_set.len()];

        let validator_set = validator_set
            .into_iter()
            .map(|(validator, stake)| {
                (
                    NodeId::new(NopPubKey::from_bytes(&[validator as u8; 32]).unwrap()),
                    Stake(stake),
                )
            })
            .collect();

        for i in 0..num_iterations {
            let leader = l.get_leader(Round(i), &validator_set);
            let index = validator_set.keys().position(|k| k == &leader).unwrap();
            num_picked[index] += 1;
        }

        for (node, expected) in expected_num_picked.iter() {
            let index = validator_set.keys().position(|k| k == node).unwrap();
            if *expected == 0 {
                assert_eq!(num_picked[index], 0);
            } else {
                println!(
                    "expected: {} num_picked[{}]: {}",
                    *expected, index, num_picked[index]
                );
                // Expect number of picks to be within small delta of expected perfect number of picks
                assert!(num_picked[index] > *expected - 150);
                assert!(num_picked[index] < *expected + 150);
            }
        }
    }
}
