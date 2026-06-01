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

use std::{cell::RefCell, collections::BTreeMap};

use monad_crypto::certificate_signature::PubKey;
use monad_types::{Epoch, NodeId, Round};

use crate::{
    leader_election::LeaderElection,
    validator_set::{ValidatorSet, ValidatorSetType},
};

pub trait ProposerSchedule<PT: PubKey> {
    // Returns None when round falls outside the known epochs
    fn check_proposer(&self, node: &NodeId<PT>, round: Round) -> Option<bool>;
    fn check_epoch(&self, epoch: Epoch, round: Round) -> Option<bool>;

    // The inserted epoch is expected to be consecutive and
    // non-overlapping with the existing epochs.
    fn insert_epoch(&mut self, epoch: Epoch, epoch_start: Round, val_set: ValidatorSet<PT>);

    // The pruning is expected to be called on every new round. The
    // purpose of this method avoid leak only. Querying pruned rounds
    // does not guarantee None results
    fn prune_below(&mut self, cutoff: Round);
}

pub type BoxedProposerSchedule<PT> = Box<dyn ProposerSchedule<PT> + Send>;

pub struct ElectedProposerSchedule<PT, LT>
where
    PT: PubKey,
    LT: LeaderElection<NodeIdPubKey = PT>,
{
    epoch_starts: BTreeMap<Epoch, Round>,
    val_sets: BTreeMap<Epoch, ValidatorSet<PT>>,
    election: LT,

    /// Memoized round -> proposer mapping
    cache: RefCell<BTreeMap<Round, NodeId<PT>>>,
}

impl<PT, LT> ElectedProposerSchedule<PT, LT>
where
    PT: PubKey,
    LT: LeaderElection<NodeIdPubKey = PT>,
{
    pub fn new(election: LT) -> Self {
        Self {
            epoch_starts: BTreeMap::new(),
            val_sets: BTreeMap::new(),
            election,
            cache: RefCell::new(BTreeMap::new()),
        }
    }

    // Get the epoch for a given round. This intentionally matches
    // EpochManager::get_epoch: the latest known epoch remains active
    // until a later epoch start is inserted.
    fn epoch_of(&self, round: Round) -> Option<Epoch> {
        self.epoch_starts
            .iter()
            .rfind(|(_, &start)| start <= round)
            .map(|(&epoch, _)| epoch)
    }

    fn leader(&self, round: Round) -> Option<NodeId<PT>> {
        if let Some(leader) = self.cache.borrow().get(&round) {
            return Some(*leader);
        }
        let epoch = self.epoch_of(round)?;
        let val_set = self.val_sets.get(&epoch)?;
        let leader = self.election.get_leader(round, val_set.get_members());
        // only positive matches are cached.
        self.cache.borrow_mut().insert(round, leader);
        Some(leader)
    }
}

impl<PT, LT> ProposerSchedule<PT> for ElectedProposerSchedule<PT, LT>
where
    PT: PubKey,
    LT: LeaderElection<NodeIdPubKey = PT>,
{
    fn check_proposer(&self, node: &NodeId<PT>, round: Round) -> Option<bool> {
        Some(self.leader(round)? == *node)
    }

    fn check_epoch(&self, epoch: Epoch, round: Round) -> Option<bool> {
        Some(self.epoch_of(round)? == epoch)
    }

    fn insert_epoch(&mut self, epoch: Epoch, epoch_start: Round, val_set: ValidatorSet<PT>) {
        if let Some(&existing) = self.epoch_starts.get(&epoch) {
            assert_eq!(existing, epoch_start, "conflicting epoch start round");
            return;
        }

        if let Some((prev_epoch, &max_start)) = self.epoch_starts.last_key_value() {
            // Sanity-check: the new epoch must start after the previous one.
            if epoch_start <= max_start {
                tracing::error!(
                    "new epoch_start {epoch_start:?} does not advance previous start {max_start:?}"
                );
            }

            // Sanity-check: the new epoch must be consecutive to the previous one.
            if let Some(expected_epoch) = prev_epoch.0.checked_add(1) {
                if epoch.0 != expected_epoch {
                    tracing::error!(
                        "new epoch {epoch:?} is not consecutive to previous epoch {prev_epoch:?}"
                    );
                }
            }
        }

        self.cache
            .borrow_mut()
            .retain(|&round, _| round < epoch_start);
        self.epoch_starts.insert(epoch, epoch_start);
        self.val_sets.insert(epoch, val_set);
    }

    fn prune_below(&mut self, cutoff: Round) {
        if let Some(min_epoch) = self.epoch_of(cutoff) {
            self.cache.get_mut().retain(|&round, _| round >= cutoff);
            self.val_sets.retain(|&epoch, _| epoch >= min_epoch);
            self.epoch_starts.retain(|&epoch, _| epoch >= min_epoch);
        }
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::NopPubKey;
    use monad_types::Stake;

    use super::*;
    use crate::simple_round_robin::SimpleRoundRobin;

    type PT = NopPubKey;

    fn nid(seed: u8) -> NodeId<PT> {
        NodeId::new(NopPubKey::from_bytes(&[seed; 32]).unwrap())
    }

    fn val_set(members: &[NodeId<PT>]) -> ValidatorSet<PT> {
        ValidatorSet::new_unchecked(members.iter().map(|id| (*id, Stake::ONE)).collect())
    }

    fn schedule() -> ElectedProposerSchedule<PT, SimpleRoundRobin<PT>> {
        ElectedProposerSchedule::new(SimpleRoundRobin::default())
    }

    #[test]
    fn unknown_round_resolves_to_none() {
        let mut sched = schedule();
        // No epochs inserted yet.
        assert_eq!(sched.leader(Round(5)), None);

        sched.insert_epoch(Epoch(1), Round(0), val_set(&[nid(1), nid(2)]));
        assert!(sched.leader(Round(99)).is_some());
        assert!(sched.leader(Round(100)).is_some());
    }

    #[test]
    fn resolves_and_caches_leader() {
        let mut sched = schedule();
        let members = [nid(1), nid(2), nid(3)];
        sched.insert_epoch(Epoch(1), Round(0), val_set(&members));

        let election = SimpleRoundRobin::<PT>::default();
        let vs = val_set(&members);
        for r in 0..50u64 {
            let expected = election.get_leader(Round(r), vs.get_members());
            assert_eq!(sched.leader(Round(r)), Some(expected));
        }
    }

    #[test]
    fn prune_drops_old_rounds_and_epochs() {
        let mut sched = schedule();
        // Epoch 1 [0, ?), epoch 2 starts at round 110 (boundary at block
        // 99, round 100, + delay 10).
        sched.insert_epoch(Epoch(1), Round(0), val_set(&[nid(1)]));
        sched.insert_epoch(Epoch(2), Round(110), val_set(&[nid(2)]));

        // Populate the cache across both epochs.
        assert!(sched.leader(Round(50)).is_some());
        assert!(sched.leader(Round(150)).is_some());

        // Prune below a round inside epoch 2.
        sched.prune_below(Round(120));

        // Epoch 1 data is gone; epoch 2 still resolves.
        assert_eq!(sched.epoch_of(Round(50)), None);
        assert!(!sched.val_sets.contains_key(&Epoch(1)));
        assert!(sched.val_sets.contains_key(&Epoch(2)));
        assert!(sched.leader(Round(150)).is_some());
        // Cache entry below cutoff dropped.
        assert!(!sched.cache.borrow().contains_key(&Round(50)));
    }

    #[test]
    fn insert_epoch_invalidates_cached_future_leaders() {
        let mut sched = schedule();
        let epoch_1_members = [nid(1), nid(2), nid(3)];
        let epoch_2_members = [nid(4), nid(5), nid(6)];
        sched.insert_epoch(Epoch(1), Round(0), val_set(&epoch_1_members));

        let election = SimpleRoundRobin::<PT>::default();
        let epoch_1_vs = val_set(&epoch_1_members);
        let stale_leader = election.get_leader(Round(120), epoch_1_vs.get_members());
        assert_eq!(sched.leader(Round(120)), Some(stale_leader));
        assert!(sched.cache.borrow().contains_key(&Round(120)));

        let epoch_2_vs = val_set(&epoch_2_members);
        let expected_leader = election.get_leader(Round(120), epoch_2_vs.get_members());
        sched.insert_epoch(Epoch(2), Round(110), epoch_2_vs);

        assert_eq!(sched.epoch_of(Round(120)), Some(Epoch(2)));
        assert_eq!(sched.leader(Round(120)), Some(expected_leader));
    }
}
