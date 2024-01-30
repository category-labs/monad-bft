use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Round, Stake};

use crate::leader_election::LeaderElection;

pub type Schedule<PT> = Vec<NodeId<PT>>;
#[derive(Clone)]
pub struct WeightedRoundRobin<PT: PubKey> {
    schedule: Schedule<PT>,
}

impl<PT: PubKey> Default for WeightedRoundRobin<PT> {
    fn default() -> Self {
        Self { schedule: vec![] }
    }
}

impl<PT: PubKey> WeightedRoundRobin<PT> {
    pub fn compute_schedule(stakes: Vec<(NodeId<PT>, Stake)>) -> Schedule<PT> {
        if let Some(max_stake) = stakes.iter().map(|(_, stake)| stake.0).max() {
            (1i64..=max_stake)
                .flat_map(|c| {
                    stakes.iter().filter_map(
                        move |(validator, stake)| {
                            if c <= stake.0 {
                                Some(*validator)
                            } else {
                                None
                            }
                        },
                    )
                })
                .collect::<_>()
        } else {
            vec![]
        }
    }
}

impl<PT: PubKey> LeaderElection for WeightedRoundRobin<PT> {
    type NodeIdPubKey = PT;

    fn update(&mut self, stakes: Vec<(NodeId<Self::NodeIdPubKey>, Stake)>) {
        self.schedule = WeightedRoundRobin::<PT>::compute_schedule(stakes);
    }

    fn get_schedule(&self) -> Vec<NodeId<Self::NodeIdPubKey>> {
        self.schedule.clone()
    }

    fn get_leader(&self, round: Round) -> NodeId<PT> {
        self.schedule[round.0 as usize % self.schedule.len()]
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::NopPubKey;

    use super::*;

    type ValidatorSet = Vec<(NodeId<NopPubKey>, Stake)>;

    fn make_schedule(schedule: Vec<char>) -> Vec<NodeId<NopPubKey>> {
        schedule
            .into_iter()
            .map(|validator| NodeId::new(NopPubKey::from_bytes(&[validator as u8; 32]).unwrap()))
            .collect()
    }
    fn make_validator(validator: char) -> NodeId<NopPubKey> {
        NodeId::new(NopPubKey::from_bytes(&[validator as u8; 32]).unwrap())
    }

    fn make_validator_set(validator_set: Vec<(char, i64)>) -> ValidatorSet {
        validator_set
            .into_iter()
            .map(|(validator, stake)| (make_validator(validator), Stake(stake)))
            .collect()
    }

    fn test_schedule(validator_set: ValidatorSet, schedule: Schedule<NopPubKey>) {
        let mut l = WeightedRoundRobin::default();
        l.update(validator_set);

        for round in 0..(2 * schedule.len()) {
            assert_eq!(
                l.get_leader(Round(round as u64)),
                schedule[round % schedule.len()]
            );
        }
    }

    #[test]
    fn test_equal_stakes() {
        let validator_set = make_validator_set(vec![('A', 1), ('B', 1), ('C', 1)]);
        let expected_schedule = make_schedule(vec!['A', 'B', 'C']);
        test_schedule(validator_set, expected_schedule);
    }

    #[test]
    fn test_unstaked_schedule() {
        let validator_set = make_validator_set(vec![('A', 1), ('B', 0), ('C', 1)]);
        let expected_schedule = make_schedule(vec!['A', 'C']);
        test_schedule(validator_set, expected_schedule);
    }

    #[test]
    fn test_validator_with_more_stake() {
        let validator_set = make_validator_set(vec![('A', 1), ('B', 2), ('C', 1)]);
        let expected_schedule = make_schedule(vec!['A', 'B', 'C', 'B']);
        test_schedule(validator_set, expected_schedule);
    }

    #[test]
    fn test_empty_schedule() {
        let validator_set = make_validator_set(vec![]);
        let expected_schedule = make_schedule(vec![]);
        test_schedule(validator_set, expected_schedule);
    }

    #[test]
    fn test_equal_schedule_with_more_stake() {
        let validator_set = make_validator_set(vec![('A', 2), ('B', 2), ('C', 2)]);
        let expected_schedule = make_schedule(vec!['A', 'B', 'C']);
        test_schedule(validator_set, expected_schedule);
    }

    #[test]
    fn test_unequal_schedule() {
        let validator_set = make_validator_set(vec![('A', 1), ('B', 2), ('C', 3)]);
        let expected_schedule = make_schedule(vec!['A', 'B', 'C', 'B', 'C', 'C']);
        test_schedule(validator_set, expected_schedule);
    }

    #[test]
    fn test_big_stake() {
        let validator_set = make_validator_set(vec![('A', 10), ('B', 2), ('C', 3)]);
        let expected_schedule = make_schedule(vec![
            'A', 'B', 'C', 'A', 'B', 'C', 'A', 'C', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
        ]);
        test_schedule(validator_set, expected_schedule);
    }

    #[test]
    fn test_negative_stake() {
        let validator_set = make_validator_set(vec![('A', -10), ('B', 2), ('C', 3)]);
        let expected_schedule = make_schedule(vec!['B', 'C', 'B', 'C', 'C']);
        test_schedule(validator_set, expected_schedule);
    }
}
