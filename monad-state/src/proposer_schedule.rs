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

use std::collections::BTreeMap;

use monad_crypto::certificate_signature::PubKey;
use monad_types::{Epoch, NodeId, Round};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    signature_collection::SignatureCollection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

/// Resolves proposer-schedule requests.
///
/// Each request specifies a span of rounds, and the service returns
/// the proposer for each round in the span. The service avoids
/// re-resolving proposers for rounds that have already been resolved
/// in previous requests.
#[derive(Debug, Eq, PartialEq)]
pub struct ProposerScheduleService {
    /// Highest round whose proposer has already been resolved, or
    /// `None` if no round has been resolved yet. Requests at or below
    /// this round are skipped.
    resolved_round: Option<Round>,
}

impl ProposerScheduleService {
    pub fn new() -> Self {
        Self {
            resolved_round: None,
        }
    }

    pub fn handle_request<PT, SCT, VTF, LT>(
        &mut self,
        mut span: monad_types::RoundSpan,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
    ) -> Option<BTreeMap<Round, NodeId<PT>>>
    where
        PT: PubKey,
        SCT: SignatureCollection<NodeIdPubKey = PT>,
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = PT>,
        LT: LeaderElection<NodeIdPubKey = PT>,
    {
        // Clamp rounds below the already resolved round.
        if let Some(resolved_round) = self.resolved_round {
            span = span.clamp_below(resolved_round.saturating_add(Round(1)))?;
        }

        // Clamp rounds from the past where the epoch data is pruned.
        let first_available_round = span
            .iter()
            .find(|round| get_epoch(*round, epoch_manager, val_epoch_map).is_some())?;
        let span = span.clamp_below(first_available_round)?;

        // Aggregate proposers for the remaining rounds.
        let mut proposers = BTreeMap::new();
        for round in span.iter() {
            let Some(epoch) = get_epoch(round, epoch_manager, val_epoch_map) else {
                break;
            };
            let proposer = get_proposer(epoch, round, val_epoch_map, election);
            self.resolved_round = Some(round);
            proposers.insert(round, proposer);
        }

        (!proposers.is_empty()).then_some(proposers)
    }
}

// Return the epoch corresponding to the given round, or None if it
// cannot be determined.
fn get_epoch<PT, SCT, VTF>(
    round: Round,
    epoch_manager: &EpochManager,
    val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
) -> Option<Epoch>
where
    PT: PubKey,
    SCT: SignatureCollection<NodeIdPubKey = PT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = PT>,
{
    let epoch = epoch_manager.get_stable_epoch(round)?;
    val_epoch_map.contains_epoch(&epoch).then_some(epoch)
}

// Return the proposer for the given round.
//
// The caller must have already verified that the epoch for the round
// is stable and available.
fn get_proposer<PT, SCT, VTF, LT>(
    epoch: Epoch,
    round: Round,
    val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
    election: &LT,
) -> NodeId<PT>
where
    PT: PubKey,
    SCT: SignatureCollection<NodeIdPubKey = PT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = PT>,
    LT: LeaderElection<NodeIdPubKey = PT>,
{
    debug_assert!(val_epoch_map.contains_epoch(&epoch));
    let validator_set = val_epoch_map
        .get_val_set(&epoch)
        .expect("caller should have checked epoch exists");
    let members = validator_set.get_members();
    election.get_leader(round, members)
}

#[cfg(test)]
mod tests {
    use monad_crypto::NopPubKey;
    use monad_types::{Epoch, RoundSpan, SeqNum, Stake};
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin, validator_mapping::ValidatorMapping,
        validator_set::ValidatorSetFactory,
    };

    use super::*;

    type PT = NopPubKey;
    #[expect(clippy::upper_case_acronyms)]
    type VEM = ValidatorsEpochMapping<
        ValidatorSetFactory<PT>,
        monad_testutil::signing::MockSignatures<monad_crypto::NopSignature>,
    >;

    fn span(start: u64, end: u64) -> RoundSpan {
        RoundSpan::new(Round(start), Round(end)).unwrap()
    }

    fn nid(seed: u8) -> NodeId<PT> {
        NodeId::new(NopPubKey::from_bytes(&[seed; 32]).unwrap())
    }

    // Fixed parameters for testing
    const EPOCH_LEN: SeqNum = SeqNum(100);
    const EPOCH_START_DELAY: Round = Round(10);

    struct State {
        epoch_manager: EpochManager,
        val_epoch_map: VEM,
        election: SimpleRoundRobin<PT>,
        pending_valset: Option<(Epoch, Vec<(NodeId<PT>, Stake)>)>,
    }

    impl State {
        fn new() -> Self {
            Self {
                epoch_manager: EpochManager::new(EPOCH_LEN, EPOCH_START_DELAY, &[]),
                val_epoch_map: VEM::new(Default::default()),
                election: SimpleRoundRobin::default(),
                pending_valset: None,
            }
        }

        // finalize a new epoch at the boundary block of the current epoch.
        fn finalize(&mut self, epoch: Epoch, round: Round, valset: Vec<(NodeId<PT>, Stake)>) {
            // the next epoch_start must be outside the stable horizon
            // of the current epoch.
            let epoch_start = round + EPOCH_START_DELAY;
            assert!(self.epoch_manager.get_stable_epoch(epoch_start).is_none());

            // pick the boundary block number
            let curr_epoch = epoch - Epoch(1);
            let curr_block_num = SeqNum(curr_epoch.0 * EPOCH_LEN.0 - 1);
            assert!(curr_block_num.is_boundary_block(EPOCH_LEN));

            // schedule the new epoch
            self.epoch_manager
                .schedule_epoch_start(curr_block_num, round);
            assert_eq!(
                self.epoch_manager.epoch_starts.last_key_value(),
                Some((&epoch, &epoch_start))
            );

            // insert the pending valset which is made available after
            // EXEC_DELAY.
            assert!(self.pending_valset.is_none());
            let locked_epoch = curr_block_num.get_locked_epoch(EPOCH_LEN);
            self.pending_valset = Some((locked_epoch, valset));
        }

        // simulate delayed execution
        fn execute(&mut self) {
            let (epoch, vs) = self
                .pending_valset
                .take()
                .expect("must call finalize before execute");
            self.val_epoch_map
                .insert(epoch, vs, ValidatorMapping::new(std::iter::empty()));
        }

        // prune old epoch data
        fn prune_epochs(&mut self, epoch: Epoch) {
            self.epoch_manager.epoch_starts.retain(|&e, _| e > epoch);
        }

        fn handle_request(
            &self,
            service: &mut ProposerScheduleService,
            span: RoundSpan,
        ) -> Option<BTreeMap<Round, NodeId<PT>>> {
            service.handle_request::<PT, _, _, _>(
                span,
                &self.epoch_manager,
                &self.val_epoch_map,
                &self.election,
            )
        }
    }

    fn assert_resolution(
        proposers: &BTreeMap<Round, NodeId<PT>>,
        expected: &[(std::ops::Range<u64>, NodeId<PT>)],
    ) {
        let total_rounds = expected.iter().map(|(r, _)| r.end - r.start).sum::<u64>();
        assert_eq!(proposers.len(), total_rounds as usize);

        for (range, nid) in expected {
            for round in range.clone() {
                assert_eq!(proposers[&Round(round)], *nid);
            }
        }
    }

    #[test]
    fn resolves_across_epoch_lifecycle() {
        let e2v = nid(2);
        let e3v = nid(3);
        let mut state = State::new();
        state.finalize(Epoch(2), Round(90), vec![(e2v, Stake::ONE)]);
        state.execute();

        // Epoch 3 [r=210..] is not finalized.
        let mut service = ProposerScheduleService::new();
        let proposers = state.handle_request(&mut service, span(0, 350)).unwrap();
        assert_resolution(&proposers, &[(100..200, e2v)]);

        // Epoch 3 [r=210..310] is finalized but valset not available yet.
        state.finalize(Epoch(3), Round(200), vec![(e3v, Stake::ONE)]);
        let mut service = ProposerScheduleService::new();
        let proposers = state.handle_request(&mut service, span(0, 350)).unwrap();
        assert_resolution(&proposers, &[(100..210, e2v)]); // Note: the epoch ending round is stablized

        // Epoch 3 valset is available.
        state.execute();
        let mut service = ProposerScheduleService::new();
        let proposers = state.handle_request(&mut service, span(0, 350)).unwrap();
        assert_resolution(&proposers, &[(100..210, e2v), (210..310, e3v)]);

        // Epoch 2 pruned
        state.prune_epochs(Epoch(2));
        let mut service = ProposerScheduleService::new();
        let proposers = state.handle_request(&mut service, span(0, 350)).unwrap();
        assert_resolution(&proposers, &[(210..310, e3v)]);
    }

    #[test]
    fn each_round_resolved_at_most_once() {
        let e2v = nid(2);
        let mut service = ProposerScheduleService::new();
        let mut state = State::new();
        state.finalize(Epoch(2), Round(90), vec![(e2v, Stake::ONE)]);
        state.execute();

        let proposers = state.handle_request(&mut service, span(0, 150)).unwrap();
        assert_resolution(&proposers, &[(100..150, e2v)]);

        // Repeated request with same span should return None.
        let proposers = state.handle_request(&mut service, span(0, 150));
        assert!(proposers.is_none());

        // Request with extended span should return proposers only
        // for the unresolved rounds.
        let proposers = state.handle_request(&mut service, span(0, 180)).unwrap();
        assert_resolution(&proposers, &[(150..180, e2v)]);

        // Requesting the same span again after updated epoch should
        // return newly stabilized proposers.
        let e3v = nid(3);
        state.finalize(Epoch(3), Round(190), vec![(e3v, Stake::ONE)]);
        state.execute();

        let proposers = state.handle_request(&mut service, span(0, 300)).unwrap();
        assert_resolution(&proposers, &[(180..200, e2v), (200..300, e3v)]);
    }
}
