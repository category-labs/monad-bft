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
use monad_pq_chorus::{PqPath, slot::evidence::SignedPathVotes, types::Metablock};

use crate::{
    chorus::{
        SlotLifecycle,
        types::{ProposalId, ProposalIndex, ProposalMap, Slot, Timestamp},
    },
    da::Resolved,
};

// a slot finalized here (`PqEvent::Final`) with every committed proposal
// resolved, for the ledger. proposals[j] is the message the metablock
// commits at j, None for ⊥.
#[derive(Clone, Debug)]
pub struct PqFinalizedSlot {
    pub slot: Slot,
    // when the slot finalized here
    pub at: Timestamp,
    // the decided deadline, if the slot was opened here
    pub deadline: Option<Timestamp>,
    // when the slot speculated (`ln:qv:speculate`), if it did
    pub speculated_at: Option<Timestamp>,
    pub metablock: Metablock,
    pub path: PqPath,
    // the Coda view of the decision, on the Coda path
    pub at_view: Option<u64>,
    pub path_votes: SignedPathVotes,
    pub proposals: ProposalMap<Option<Bytes>>,
    // when DA resolved each committed lane here; None for ⊥
    pub lane_decoded_at: Vec<Option<Timestamp>>,
    // committed lanes DA never resolved here before it forgot the slot
    pub unresolved: Vec<ProposalIndex>,
}

impl PqFinalizedSlot {
    pub fn pids(&self) -> ProposalMap<Option<ProposalId>> {
        self.metablock.as_ref().map(|entry| entry.root())
    }
}

// what `PqEvent::Final` carries
#[derive(Clone, Debug)]
pub struct PqFinal {
    pub metablock: Metablock,
    pub path: PqPath,
    pub at_view: Option<u64>,
    pub path_votes: SignedPathVotes,
}

#[derive(Default)]
struct SlotCollection {
    deadline: Option<Timestamp>,
    speculated_at: Option<Timestamp>,
    finalized: Option<(Timestamp, PqFinal)>,
    resolved: HashMap<(ProposalIndex, ProposalId), (Resolved, Timestamp)>,
}

impl SlotCollection {
    fn is_complete(&self) -> bool {
        let Some((_, finalized)) = &self.finalized else {
            return false;
        };
        let pids = finalized.metablock.as_ref().map(|entry| entry.root());
        pids.into_indexed_iter()
            .all(|(j, pid)| pid.is_none_or(|pid| self.resolved.contains_key(&(j, pid))))
    }

    // the caller checked the slot finalized; lanes not yet resolved are
    // listed as unresolved
    fn into_finalized(self, slot: Slot) -> PqFinalizedSlot {
        let (at, finalized) = self.finalized.expect("finalized");
        let mut resolved = self.resolved;
        let mut lane_decoded_at = Vec::new();
        let mut unresolved = Vec::new();
        let pids = finalized.metablock.as_ref().map(|entry| entry.root());
        let proposals = pids.map_indexed(|j, pid| {
            let resolved = pid.and_then(|pid| resolved.remove(&(j, pid)));
            if pid.is_some() && resolved.is_none() {
                unresolved.push(j);
            }
            lane_decoded_at.push(resolved.as_ref().map(|&(_, at)| at));
            resolved.and_then(|(message, _)| message.ok())
        });
        PqFinalizedSlot {
            slot,
            at,
            deadline: self.deadline,
            speculated_at: self.speculated_at,
            metablock: finalized.metablock,
            path: finalized.path,
            at_view: finalized.at_view,
            path_votes: finalized.path_votes,
            proposals,
            lane_decoded_at,
            unresolved,
        }
    }
}

// joins a slot's `PqEvent::Final` with DA's resolution of its committed
// proposals, which arrive in either order
pub struct PqFinalizationCollector {
    slots: BTreeMap<Slot, SlotCollection>,
    ready: VecDeque<PqFinalizedSlot>,
    // DA's completed_slot_retention: it forgets slots this far below the cap
    da_retention: u64,
}

impl PqFinalizationCollector {
    pub fn new(da_retention: u64) -> Self {
        Self {
            slots: BTreeMap::new(),
            ready: VecDeque::new(),
            da_retention,
        }
    }

    pub fn handle_final(&mut self, at: Timestamp, slot: Slot, finalized: PqFinal) {
        let collection = self.slots.entry(slot).or_default();
        if collection.finalized.is_some() {
            return;
        }
        collection.finalized = Some((at, finalized));
        self.complete(slot);
    }

    pub fn handle_speculative(&mut self, at: Timestamp, slot: Slot) {
        self.slots.entry(slot).or_default().speculated_at = Some(at);
    }

    pub fn handle_resolved(
        &mut self,
        at: Timestamp,
        slot: Slot,
        proposal_index: ProposalIndex,
        pid: ProposalId,
        message: Resolved,
    ) {
        let collection = self.slots.entry(slot).or_default();
        collection
            .resolved
            .entry((proposal_index, pid))
            .or_insert((message, at));
        self.complete(slot);
    }

    // a slot halts after it finalizes, and the cap passes it only after
    // it halts, so an unfinalized slot below the cap never finalizes here
    pub fn handle_lifecycle(&mut self, event: SlotLifecycle) {
        match event {
            SlotLifecycle::Opened { slot, deadline } => {
                self.slots.entry(slot).or_default().deadline = Some(deadline);
            }
            SlotLifecycle::CapAdvance { cap } => {
                self.slots
                    .retain(|&slot, c| slot >= cap || c.finalized.is_some());
                self.give_up_below(cap);
            }
            SlotLifecycle::Completed { .. } => {}
        }
    }

    // a finalized slot DA has forgotten never resolves: it goes out with
    // its missing lanes marked rather than waiting forever
    fn give_up_below(&mut self, cap: Slot) {
        let Some(resolvable) = cap.checked_sub(self.da_retention) else {
            return;
        };
        let kept = self.slots.split_off(&resolvable);
        for (slot, collection) in std::mem::replace(&mut self.slots, kept) {
            let finalized = collection.into_finalized(slot);
            tracing::warn!(
                slot = slot.0,
                unresolved = ?finalized.unresolved,
                "finalized slot never resolved here, stored with lanes missing"
            );
            self.ready.push_back(finalized);
        }
    }

    pub fn poll(&mut self) -> Option<PqFinalizedSlot> {
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
    use monad_mcp_chorus::spec::ProposalHeader as _;
    use monad_pq_chorus::types::Entry;

    use super::*;
    use crate::chorus::env::{D25, EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader};

    const RETENTION: u64 = 50;

    fn pid(slot: u64) -> ProposalId {
        ProposalHeader {
            root: MerkleRoot(MerkleHash([slot as u8; 20])),
            scheme: EncodingScheme::D25(D25 {
                slot: Slot(slot),
                msg_len: 1,
                unix_ts: 0,
                depth: 3,
            }),
        }
        .pid()
    }

    fn final_of(slot: u64) -> PqFinal {
        PqFinal {
            metablock: ProposalMap::new(2, |j| [Entry::Root(pid(slot)), Entry::Bot][j]),
            path: PqPath::Fast,
            at_view: None,
            path_votes: Vec::new(),
        }
    }

    fn resolve(collector: &mut PqFinalizationCollector, slot: u64) {
        let at = Timestamp::from_millis(slot);
        let message = Ok(Bytes::from_static(b"m"));
        collector.handle_resolved(at, Slot(slot), 0, pid(slot), message);
    }

    #[test]
    fn a_slot_completes_once_finalized_and_resolved_in_either_order() {
        let mut collector = PqFinalizationCollector::new(RETENTION);
        collector.handle_final(Timestamp::from_millis(10), Slot(3), final_of(3));
        assert!(collector.poll().is_none());
        resolve(&mut collector, 3);
        let done = collector.poll().unwrap();
        assert_eq!(done.slot, Slot(3));
        assert_eq!(done.proposals[0].as_deref(), Some(&b"m"[..]));
        assert_eq!(done.proposals[1], None);
        assert_eq!(
            done.lane_decoded_at,
            [Some(Timestamp::from_millis(3)), None]
        );

        resolve(&mut collector, 4);
        collector.handle_speculative(Timestamp::from_millis(5), Slot(4));
        collector.handle_final(Timestamp::from_millis(6), Slot(4), final_of(4));
        let done = collector.poll().unwrap();
        assert_eq!(done.speculated_at, Some(Timestamp::from_millis(5)));
        assert!(collector.poll().is_none());
    }

    #[test]
    fn an_all_bot_metablock_completes_at_once() {
        let mut collector = PqFinalizationCollector::new(RETENTION);
        let mut finalized = final_of(1);
        finalized.metablock = ProposalMap::new(2, |_| Entry::Bot);
        collector.handle_final(Timestamp::from_millis(1), Slot(1), finalized);
        assert_eq!(
            collector.poll().unwrap().pids(),
            ProposalMap::new(2, |_| None)
        );
    }

    #[test]
    fn a_cap_advance_drops_unfinalized_slots_below_it() {
        let mut collector = PqFinalizationCollector::new(RETENTION);
        resolve(&mut collector, 3);
        resolve(&mut collector, 5);
        collector.handle_final(Timestamp::from_millis(2), Slot(2), final_of(2));
        collector.handle_lifecycle(SlotLifecycle::CapAdvance { cap: Slot(4) });
        assert_eq!(
            collector.slots.keys().copied().collect::<Vec<_>>(),
            [Slot(2), Slot(5)]
        );
    }

    #[test]
    fn a_finalized_slot_da_forgot_goes_out_with_its_lanes_marked_missing() {
        let mut collector = PqFinalizationCollector::new(RETENTION);
        collector.handle_final(Timestamp::from_millis(2), Slot(2), final_of(2));
        collector.handle_lifecycle(SlotLifecycle::CapAdvance {
            cap: Slot(2 + RETENTION),
        });
        assert!(collector.poll().is_none());

        collector.handle_lifecycle(SlotLifecycle::CapAdvance {
            cap: Slot(3 + RETENTION),
        });
        let done = collector.poll().unwrap();
        assert_eq!(done.slot, Slot(2));
        assert_eq!(done.unresolved, [0]);
        assert_eq!(done.pids()[0], Some(pid(2)));
        assert_eq!(done.proposals[0], None);
        assert!(collector.slots.is_empty());
    }
}
