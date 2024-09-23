use std::{collections::BTreeMap, marker::PhantomData};

use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Round, Stake};

use crate::leader_election::LeaderElection;

#[derive(Clone)]
pub struct SimpleRoundRobin<PT>(PhantomData<PT>);
impl<PT> Default for SimpleRoundRobin<PT> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<PT: PubKey> LeaderElection for SimpleRoundRobin<PT> {
    type NodeIdPubKey = PT;
    fn get_leader(
        &self,
        round: Round,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> NodeId<PT> {
        let validators: Vec<_> = validators
            .iter()
            .filter_map(|(node_id, stake)| (*stake != Stake(0)).then_some(node_id))
            .collect();
        *validators[round.0 as usize % validators.len()]
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::NopPubKey;
    use test_case::test_case;

    use super::*;

    #[test_case(vec![('A', 1), ('B', 1), ('C', 1)],    vec!['A', 'B', 'C', 'A', 'B', 'C']; "equal stakes")]
    #[test_case(vec![('A', 1), ('B', 1), ('C', 1)],    vec!['A', 'B', 'C', 'A', 'B', 'C']; "test equal stakes")]
    #[test_case(vec![('A', 1), ('B', 0), ('C', 1)],    vec!['A', 'C', 'A', 'C']; "test unstaked schedule")]
    #[test_case(vec![('A', 1), ('B', 2), ('C', 1)],    vec!['A', 'B', 'C', 'A', 'B', 'C']; "test validator with more stake")]
    #[test_case(vec![],                                vec![]; "test empty schedule")]
    #[test_case(vec![('A', 2), ('B', 2), ('C', 2)],    vec!['A', 'B', 'C', 'A', 'B', 'C']; "test equal schedule with more stake")]
    #[test_case(vec![('A', 1), ('B', 2), ('C', 3)],    vec!['A', 'B', 'C', 'A', 'B', 'C']; "test unequal schedule")]
    #[test_case(vec![('A', 10), ('B', 2), ('C', 3)],   vec![ 'A', 'B', 'C', 'A', 'B', 'C', 'A', 'B', 'C', 'A', 'B', 'C', 'A', 'B', 'C']; "test big stake")]
    #[test_case(vec![('A', -10), ('B', 2), ('C', 3)],  vec!['A', 'B', 'C', 'A', 'B', 'C']; "test negative stake")]
    fn test_boxed_round_robin(validator_set: Vec<(char, i64)>, expected_schedule: Vec<char>) {
        let l: Box<dyn LeaderElection<NodeIdPubKey = NopPubKey>> =
            Box::new(SimpleRoundRobin::default());
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
