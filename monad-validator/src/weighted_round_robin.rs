// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{collections::BTreeMap, marker::PhantomData};

use alloy_primitives::U256;
use itertools::Itertools;
use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Round, Stake};
use rand::Rng;
use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng};

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
    let mut gen = ChaCha20Rng::seed_from_u64(x);
    gen.gen_range(0..m)
}

fn generate_random_validator_u64<PT: PubKey>(
    round: Round,
    validators: Vec<(&NodeId<PT>, &Stake)>,
) -> NodeId<PT> {
    let mut total_stakes = 0_u64;
    let stake_bounds = validators
        .iter()
        .filter_map(|&(node_id, stake)| {
            // Panics if stake is too big
            let stake_u64 = stake.0.to::<u64>();
            if stake_u64 > 0 {
                total_stakes = total_stakes
                    .checked_add(stake_u64)
                    .expect("total stake <= u64::MAX");
                Some((node_id, total_stakes))
            } else {
                None
            }
        })
        .collect_vec();
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

pub fn randomize_256_with_rng(gen: &mut impl Rng, m: U256) -> U256 {
    let max = U256::MAX - (U256::MAX - m + U256::from(1)) % m;
    loop {
        let r: U256 = gen.gen();
        if r <= max {
            return r % m;
        }
    }
}

/// # Panics
/// Panics if `validators.is_empty()` or if `validators` does not contain an element whose stake is > 0
pub fn generate_random_validator_with_randomizer<PT: PubKey>(
    validators: Vec<(&NodeId<PT>, &Stake)>,
    mut randomize_from_total_stake: impl FnMut(U256) -> U256,
) -> NodeId<PT> {
    let mut total_stake = U256::ZERO;
    let stake_bounds = validators
        .iter()
        .filter_map(|&(node_id, stake)| {
            if stake.0 > U256::ZERO {
                total_stake += stake.0;
                Some((node_id, total_stake))
            } else {
                None
            }
        })
        .collect_vec();
    if stake_bounds.is_empty() {
        panic!("no validator has positive stake");
    }

    let stake_index = randomize_from_total_stake(total_stake);
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

impl<PT: PubKey> LeaderElection for WeightedRoundRobin<PT> {
    type NodeIdPubKey = PT;

    /// Computes the leader using randomized interleaved weighted round-robin scheduling
    /// # Panics
    /// Panics if `validators.is_empty()` or if `validators` does not contain an element whose stake is > 0, because
    /// there is no sensible choice for leader in either of those cases.
    fn get_leader(&self, round: Round, validators: &BTreeMap<NodeId<PT>, Stake>) -> NodeId<PT> {
        generate_random_validator_u64(round, validators.iter().collect_vec())

        // TODO switch to U256 gen on fork
        // let mut gen = ChaCha20Rng::seed_from_u64(round.0);
        // let randomizer = |total_stake| randomize_256_with_rng(&mut gen, total_stake);
        // generate_random_validator_with_randomizer(validators.iter().collect_vec(), randomizer)
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::NopPubKey;
    use test_case::test_case;

    use super::*;

    #[test_case(vec![('A', U256::ONE), ('B', U256::ONE), ('C', U256::ONE)]; "equal stakes")]
    #[test_case(vec![('A', U256::ONE), ('B', U256::ONE), ('C', U256::ONE)]; "test equal stakes")]
    #[test_case(vec![('A', U256::ONE), ('B', U256::ZERO), ('C', U256::ONE)];      "test unstaked schedule")]
    #[test_case(vec![('A', U256::ONE), ('B', U256::from(2)), ('C', U256::ONE)]; "test validator with more stake")]
    #[test_case(vec![('A', U256::from(2)), ('B', U256::from(2)), ('C', U256::from(2))]; "test equal schedule with more stake")]
    #[test_case(vec![('A', U256::ONE), ('B', U256::from(2)), ('C', U256::from(3))]; "test unequal schedule")]
    #[test_case(vec![('A', U256::from(10)), ('B', U256::from(2)), ('C', U256::from(3))]; "test big stake")]
    fn test_weighted_round_robin(validator_set: Vec<(char, U256)>) {
        let num_iterations = 10000_u64;
        let l = WeightedRoundRobin::default();
        let total_stakes = validator_set
            .iter()
            .filter_map(|(_, stake)| {
                if *stake > U256::ZERO {
                    Some(*stake)
                } else {
                    None
                }
            })
            .sum::<U256>();
        let expected_num_picked = validator_set
            .iter()
            .map(|(validator, stake)| {
                (
                    NodeId::new(NopPubKey::from_bytes(&[*validator as u8; 32]).unwrap()),
                    if *stake > U256::ZERO {
                        (U256::from(num_iterations) * *stake / total_stakes).to::<u64>()
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

    #[test]
    fn test_equivalent_leader_schedule() {
        let stakes = vec![('A', 1), ('B', 2), ('C', 3), ('D', 4)];
        let expected_schedule = vec![
            'A', 'D', 'C', 'D', 'C', 'D', 'D', 'A', 'D', 'A', 'D', 'C', 'C', 'B', 'D', 'D', 'C',
            'B', 'B', 'D', 'D', 'A', 'C', 'B', 'C', 'A', 'B', 'D', 'C', 'D', 'D', 'B', 'D', 'D',
            'D', 'A', 'D', 'A', 'D', 'D', 'D', 'B', 'C', 'D', 'C', 'A', 'A', 'C', 'B', 'B', 'D',
            'C', 'C', 'B', 'C', 'B', 'D', 'B', 'B', 'B', 'D', 'A', 'C', 'C', 'B', 'C', 'C', 'C',
            'A', 'D', 'D', 'D', 'A', 'C', 'C', 'C', 'C', 'D', 'B', 'A', 'D', 'D', 'D', 'D', 'C',
            'A', 'D', 'D', 'A', 'C', 'D', 'B', 'B', 'D', 'A', 'C', 'C', 'C', 'C', 'C', 'B', 'B',
            'D', 'C', 'C', 'C', 'C', 'D', 'A', 'C', 'C', 'B', 'B', 'D', 'B', 'D', 'D', 'C', 'D',
            'C', 'B', 'C', 'C', 'A', 'D', 'B', 'D', 'B', 'C', 'C', 'D', 'C', 'D', 'C', 'D', 'D',
            'C', 'D', 'D', 'B', 'C', 'D', 'C', 'A', 'D', 'D', 'D', 'B', 'A', 'C', 'D', 'D', 'D',
            'D', 'B', 'D', 'C', 'D', 'B', 'D', 'B', 'D', 'D', 'C', 'B', 'C', 'D', 'B', 'D', 'C',
            'D', 'C', 'C', 'C', 'C', 'A', 'C', 'D', 'D', 'B', 'C', 'C', 'B', 'C', 'B', 'B', 'A',
            'C', 'B', 'D', 'C', 'C', 'C', 'C', 'C', 'C', 'A', 'C', 'A', 'D', 'D', 'D', 'D', 'D',
            'B', 'D', 'D', 'C', 'C', 'C', 'D', 'D', 'C', 'C', 'C', 'D', 'C', 'A', 'B', 'D', 'C',
            'D', 'B', 'D', 'B', 'D', 'B', 'A', 'C', 'C', 'D', 'D', 'C', 'B', 'B', 'D', 'D', 'C',
            'D', 'C', 'A', 'D', 'B', 'D', 'A', 'C', 'D', 'B', 'D', 'A',
        ];

        let validator_set = stakes
            .into_iter()
            .map(|(validator, stake)| {
                (
                    NodeId::new(NopPubKey::from_bytes(&[validator as u8; 32]).unwrap()),
                    Stake(U256::from(stake)),
                )
            })
            .collect();

        let leader_election = WeightedRoundRobin::default();
        for (round, expected_leader) in expected_schedule.into_iter().enumerate() {
            let leader = leader_election.get_leader(Round(round as u64), &validator_set);
            let leader_char = leader.pubkey().bytes()[0] as char;
            assert_eq!(expected_leader, leader_char);
        }
    }
}
