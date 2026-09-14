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

use std::collections::{BTreeMap, HashMap, VecDeque};

use bytes::Bytes;

use crate::chorus::{
    SlotLifecycle,
    slot::chorus::SlotFinalization,
    types::{MerkleRoot, ProposalIndex, ProposalMap, Slot, Timestamp},
};

// a finalized slot with every committed proposal decoded, for the
// ledger and execution. proposals[j] is the message under the root
// the finalization commits at j.
pub struct FinalizedSlot {
    pub slot: Slot,
    // when cadence finalized it
    pub at: Timestamp,
    pub finalization: SlotFinalization,
    pub proposals: ProposalMap<Option<Bytes>>,
}

#[derive(Default)]
struct SlotCollection {
    finalized: Option<(Timestamp, SlotFinalization)>,
    decoded: HashMap<(ProposalIndex, MerkleRoot), Bytes>,
}

impl SlotCollection {
    fn is_complete(&self) -> bool {
        let Some((_, finalization)) = &self.finalized else {
            return false;
        };
        for (j, root) in finalization.roots().into_indexed_iter() {
            let Some(root) = root else {
                continue;
            };
            if !self.decoded.contains_key(&(j, root)) {
                return false;
            }
        }
        true
    }

    // the caller checked is_complete
    fn into_finalized(self, slot: Slot) -> FinalizedSlot {
        let (at, finalization) = self.finalized.expect("complete");
        let mut decoded = self.decoded;
        let proposals = finalization.roots().map_indexed(|j, root| {
            let root = root?;
            Some(decoded.remove(&(j, root)).expect("complete"))
        });
        FinalizedSlot {
            slot,
            at,
            finalization,
            proposals,
        }
    }
}

// joins cadence's finalization of a slot with DA's decodes of its
// committed proposals, which arrive in either order
#[derive(Default)]
pub struct FinalizationCollector {
    slots: BTreeMap<Slot, SlotCollection>,
    ready: VecDeque<FinalizedSlot>,
}

impl FinalizationCollector {
    pub fn handle_finalization(
        &mut self,
        at: Timestamp,
        slot: Slot,
        finalization: SlotFinalization,
    ) {
        self.slots.entry(slot).or_default().finalized = Some((at, finalization));
        self.complete(slot);
    }

    pub fn handle_decoded(
        &mut self,
        slot: Slot,
        proposal_index: ProposalIndex,
        root: MerkleRoot,
        message: Bytes,
    ) {
        let collection = self.slots.entry(slot).or_default();
        collection.decoded.insert((proposal_index, root), message);
        self.complete(slot);
    }

    // a slot that ended without finalizing is dropped. todo: decodes
    // that arrive after that
    pub fn handle_lifecycle(&mut self, slot: Slot, event: SlotLifecycle) {
        let SlotLifecycle::Completed = event else {
            return;
        };
        let Some(collection) = self.slots.get(&slot) else {
            return;
        };
        if collection.finalized.is_none() {
            self.slots.remove(&slot);
        }
    }

    pub fn poll(&mut self) -> Option<FinalizedSlot> {
        self.ready.pop_front()
    }

    fn complete(&mut self, slot: Slot) {
        let Some(collection) = self.slots.get(&slot) else {
            return;
        };
        if !collection.is_complete() {
            return;
        }
        let collection = self.slots.remove(&slot).expect("present");
        self.ready.push_back(collection.into_finalized(slot));
    }
}
