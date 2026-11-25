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

use std::{ops::Range, sync::Arc};

use monad_mcp_common::Slot;

use super::{
    election::{Election, ProposerSet},
    env::ValidatorData,
};

// The caller asked about a slot outside the slots of the handle.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SlotOutOfBound;

// The validators and the proposers of each slot of one epoch.
#[derive(Clone)]
pub struct EpochHandle {
    slots: Range<Slot>,
    validators: Arc<ValidatorData>,
    election: Arc<dyn Election + Send + Sync>,
}

impl EpochHandle {
    pub fn new(validators: ValidatorData, election: impl Election + Send + Sync + 'static) -> Self {
        Self {
            slots: Slot::FIRST..Slot::MAX_CAP,
            validators: Arc::new(validators),
            election: Arc::new(election),
        }
    }

    pub fn slots(&self) -> Range<Slot> {
        self.slots.clone()
    }

    pub fn validators_at(&self, slot: Slot) -> Result<Arc<ValidatorData>, SlotOutOfBound> {
        if !self.slots.contains(&slot) {
            return Err(SlotOutOfBound);
        }
        Ok(self.validators.clone())
    }

    pub fn proposers_at(&self, slot: Slot) -> Result<ProposerSet, SlotOutOfBound> {
        if !self.slots.contains(&slot) {
            return Err(SlotOutOfBound);
        }
        // todo: make the election total on the slots of the handle
        self.election.proposers_at(slot).map_err(|_| SlotOutOfBound)
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_common::{Lane, spec::ValidatorData as _};

    use super::{
        super::{
            election::FixedProposerSchedule,
            env::{NodeId, ValidatorData},
        },
        *,
    };

    fn handle() -> EpochHandle {
        let election = FixedProposerSchedule::new(vec![NodeId::dummy(2), NodeId::dummy(0)]);
        EpochHandle::new(ValidatorData::dummy(4), election)
    }

    #[test]
    fn a_slot_of_the_epoch_has_validators_and_proposers() {
        let handle = handle();
        let slot = Slot(7);

        let validators = handle.validators_at(slot).expect("in bound");
        assert_eq!(validators.len(), 4);

        let proposers = handle.proposers_at(slot).expect("in bound");
        assert_eq!(proposers.slot(), slot);
        assert_eq!(
            proposers.proposer(Lane::new_unchecked(0)),
            Some(NodeId::dummy(2))
        );
        assert_eq!(
            proposers.lane_of(&NodeId::dummy(0)),
            Some(Lane::new_unchecked(1))
        );
    }

    #[test]
    fn a_slot_at_the_cap_is_out_of_bound() {
        let handle = handle();
        let cap = handle.slots().end;

        assert_eq!(handle.validators_at(cap).err(), Some(SlotOutOfBound));
        assert_eq!(handle.proposers_at(cap).err(), Some(SlotOutOfBound));
    }
}
