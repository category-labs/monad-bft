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
    // the decided deadline, if the slot was opened here
    pub deadline: Option<Timestamp>,
    // when cadence formed the fast block, on either path. demo(tx-timeline)
    pub fast_block_at: Option<Timestamp>,
    pub finalization: SlotFinalization,
    pub proposals: ProposalMap<Option<Bytes>>,
    // demo(tx-timeline): when DA decoded each committed lane here; None for a negative lane.
    pub lane_decoded_at: Vec<Option<Timestamp>>,
}

#[derive(Default)]
struct SlotCollection {
    deadline: Option<Timestamp>,
    fast_block_at: Option<Timestamp>, // demo(tx-timeline)
    finalized: Option<(Timestamp, SlotFinalization)>,
    decoded: HashMap<(ProposalIndex, MerkleRoot), (Bytes, Timestamp)>, // demo(tx-timeline)
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
        let mut lane_decoded_at = Vec::new(); // demo(tx-timeline)
        let proposals = finalization.roots().map_indexed(|j, root| {
            let decoded = root.map(|root| decoded.remove(&(j, root)).expect("complete"));
            lane_decoded_at.push(decoded.as_ref().map(|&(_, at)| at)); // demo(tx-timeline)
            decoded.map(|(message, _)| message)
        });
        FinalizedSlot {
            slot,
            at,
            deadline: self.deadline,
            fast_block_at: self.fast_block_at, // demo(tx-timeline)
            finalization,
            proposals,
            lane_decoded_at, // demo(tx-timeline)
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

    // demo(tx-timeline): an optimistic commit comes before its slot's finalization.
    pub fn handle_fast_block(&mut self, at: Timestamp, slot: Slot) {
        self.slots.entry(slot).or_default().fast_block_at = Some(at);
    }

    pub fn handle_decoded(
        &mut self,
        at: Timestamp, // demo(tx-timeline)
        slot: Slot,
        proposal_index: ProposalIndex,
        root: MerkleRoot,
        message: Bytes,
    ) {
        let collection = self.slots.entry(slot).or_default();
        collection
            .decoded
            .insert((proposal_index, root), (message, at)); // demo(tx-timeline)
        self.complete(slot);
    }

    // cadence emits a slot's finalization before any cap past it, so an
    // unfinalized slot below the cap never finalizes; late decodes wait for the next cap
    pub fn handle_lifecycle(&mut self, event: SlotLifecycle) {
        match event {
            SlotLifecycle::Opened { slot, deadline } => {
                self.slots.entry(slot).or_default().deadline = Some(deadline);
            }
            SlotLifecycle::CapAdvance { cap } => {
                self.slots
                    .retain(|&slot, c| slot >= cap || c.finalized.is_some());
            }
            SlotLifecycle::Completed { .. } => {}
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chorus::env::MerkleHash;

    fn decode(collector: &mut FinalizationCollector, slot: u64) {
        let root = MerkleRoot(MerkleHash([slot as u8; 20]));
        let at = Timestamp::from_millis(slot); // demo(tx-timeline)
        collector.handle_decoded(at, Slot(slot), 0, root, Bytes::from_static(b"m"));
    }

    #[test]
    fn a_cap_advance_drops_unfinalized_slots_below_it() {
        let mut collector = FinalizationCollector::default();
        decode(&mut collector, 3);
        decode(&mut collector, 5);

        collector.handle_lifecycle(SlotLifecycle::CapAdvance { cap: Slot(4) });
        assert_eq!(
            collector.slots.keys().copied().collect::<Vec<_>>(),
            [Slot(5)]
        );

        // a decode for a slot below the cap waits for the next one
        decode(&mut collector, 3);
        collector.handle_lifecycle(SlotLifecycle::CapAdvance { cap: Slot(4) });
        assert_eq!(
            collector.slots.keys().copied().collect::<Vec<_>>(),
            [Slot(5)]
        );
    }

    #[test]
    fn an_opened_slot_keeps_its_deadline_until_the_cap_passes() {
        let mut collector = FinalizationCollector::default();
        let deadline = Timestamp::from_millis(500);
        for slot in [2, 6] {
            collector.handle_lifecycle(SlotLifecycle::Opened {
                slot: Slot(slot),
                deadline,
            });
        }
        assert_eq!(collector.slots[&Slot(2)].deadline, Some(deadline));
        collector.handle_lifecycle(SlotLifecycle::CapAdvance { cap: Slot(4) });
        assert_eq!(
            collector.slots.keys().copied().collect::<Vec<_>>(),
            [Slot(6)]
        );
    }
}
