use std::collections::{BTreeSet, HashMap, HashSet};

use bitvec::{bitbox, boxed::BitBox};

use super::{
    assignment::{ChunkAssignment, ChunkId, Upstream},
    types::{NodeId, Slot},
};

pub(crate) struct DecodingTracker {
    chunks: BitBox,
    count: usize,
    threshold: usize,
}

impl DecodingTracker {
    pub(crate) fn new(num_chunks: usize, threshold: usize) -> Self {
        assert!(0 < threshold && threshold < num_chunks);
        Self {
            chunks: bitbox![0; num_chunks],
            count: 0,
            threshold,
        }
    }

    // the caller must ensure chunk_id is in range.
    pub(crate) fn already_received(&self, chunk_id: ChunkId) -> bool {
        let index = usize::from(chunk_id);
        assert!(index < self.chunks.len());
        self.chunks[index]
    }

    // the caller must ensure chunk_id is in range.
    pub(crate) fn mark_received(&mut self, chunk_id: ChunkId) {
        let index = usize::from(chunk_id);
        assert!(index < self.chunks.len());

        self.chunks.set(index, true);
        self.count += 1;
    }

    pub(crate) fn ready(&self) -> bool {
        self.count > self.threshold
    }
}

pub(crate) struct ObligationTracker {
    // the node's own obliged chunks
    obliged_chunks: HashSet<ChunkId>,
    // the number of chunks remaining to be received from rebroadcast owners
    remaining_owner_obligation: HashMap<NodeId, usize>,
    // the number of chunks remaining to be received from the author
    remaining_author_obligation: usize,
}

impl ObligationTracker {
    // todo: for nodes whose obligation is zero, mark as fulfilled
    // immediately.  zero obligation is currently impossible with
    // round up assignment, but possible with e.g. the swiper
    // approach.
    pub(crate) fn new(assignment: &ChunkAssignment, receiver: &NodeId) -> Self {
        let mut remaining_owner_obligation: HashMap<_, usize> = HashMap::new();
        let mut remaining_author_obligation = 0;
        let mut obliged_chunks = HashSet::new();

        for chunk_id in 0..(assignment.num_chunks() as u16) {
            let chunk_id = ChunkId::from(chunk_id);
            let routing = assignment
                .resolve_chunk_id(chunk_id)
                .expect("chunk_id in range");

            match routing.upstream(receiver) {
                None => {}
                Some(Upstream::Author) => {
                    obliged_chunks.insert(chunk_id);
                    remaining_author_obligation += 1;
                }
                Some(Upstream::Owner(owner)) if remaining_owner_obligation.contains_key(owner) => {
                    *remaining_owner_obligation.get_mut(owner).unwrap() += 1;
                }
                Some(Upstream::Owner(owner)) => {
                    remaining_owner_obligation.insert(*owner, 1);
                }
            }
        }

        // todo: reduce obligations by a small fraction to account for
        // network packet loss. q: should we reduce obligation from
        // author as well?

        Self {
            obliged_chunks,
            remaining_author_obligation,
            remaining_owner_obligation,
        }
    }

    pub fn obliged_chunks(&self) -> &HashSet<ChunkId> {
        &self.obliged_chunks
    }

    // the caller must ensure each chunk_id is (1) in range; (2)
    // marked at most once. The caller must also ensure the upstream
    // is consistent with the assignment used in creating this
    // ObligationTracker.
    //
    // return true if the upstream's obligation is just fulfilled by
    // this call.
    pub(crate) fn record_received_chunk(&mut self, upstream: Upstream<&NodeId>) -> bool {
        let counter: &mut usize = match upstream {
            Upstream::Author => &mut self.remaining_author_obligation,
            Upstream::Owner(owner) => self
                .remaining_owner_obligation
                .get_mut(owner)
                .expect("upstream is deterministic"),
        };

        if *counter > 0 {
            *counter -= 1;
            return *counter == 0;
        }

        false
    }
}

// node will always recover its obliged chunks whenever possible, but
// it will limit the number of chunks it serves that are not obliged
// to it.
const MAX_UNOBLIGED_CHUNK: usize = 100;

// node will always recover its obliged chunks to any peer (at most
// once), but for unobliged chunks, it will limit the number of chunks it
// serves from the peer.
const MAX_UNOBLIGED_REQUEST: usize = 20;

pub struct ChunkRecoveryTracker {
    num_chunks: usize,

    // the node's own obliged chunks
    obliged_chunks: HashSet<ChunkId>,

    // the chunks served to each requester
    served_chunks: HashMap<NodeId, (usize, BitBox)>,
}

impl ChunkRecoveryTracker {
    pub(crate) fn new(num_chunks: usize, obliged_chunks: &HashSet<ChunkId>) -> Self {
        Self {
            num_chunks,
            obliged_chunks: obliged_chunks.clone(),
            served_chunks: HashMap::new(),
        }
    }

    // the caller must ensure chunk_id is in range.
    pub(crate) fn should_serve(&self, from: &NodeId, chunk_id: ChunkId) -> bool {
        let peer_record = self.served_chunks.get(from);

        let already_served = peer_record.is_some_and(|(_, served)| served[usize::from(chunk_id)]);
        if already_served {
            return false;
        }

        if self.obliged_chunks.contains(&chunk_id) {
            return true;
        }

        let peer_unobliged = peer_record.map_or(0, |(count, _)| *count);
        if peer_unobliged >= MAX_UNOBLIGED_REQUEST {
            return false;
        }

        let total_unobliged: usize = self.served_chunks.values().map(|(count, _)| count).sum();
        total_unobliged < MAX_UNOBLIGED_CHUNK
    }

    // the caller must ensure chunk_id is in range, and should_serve
    // holds for (from, chunk_id).
    pub(crate) fn mark_served(&mut self, from: &NodeId, chunk_id: ChunkId) {
        let unobliged = !self.obliged_chunks.contains(&chunk_id);

        let num_chunks = self.num_chunks;
        let (unobliged_count, served) = self
            .served_chunks
            .entry(*from)
            .or_insert_with(|| (0, bitbox![0; num_chunks]));

        served.set(usize::from(chunk_id), true);
        if unobliged {
            *unobliged_count += 1;
        }
    }
}

// todo: share with conductor's CompletionTracker
pub struct SlotCompletion {
    cap: Slot,
    completed_slots: BTreeSet<Slot>,
}

impl SlotCompletion {
    pub fn new() -> Self {
        Self {
            cap: Slot::MIN,
            completed_slots: BTreeSet::new(),
        }
    }

    pub fn mark_completed(&mut self, slot: Slot) {
        if slot < self.cap {
            return;
        }

        self.completed_slots.insert(slot);

        while self.completed_slots.contains(&self.cap) {
            self.completed_slots.remove(&self.cap);
            self.cap = self.cap.checked_next().expect("slot cap overflow");
        }
    }

    pub fn cap(&self) -> Slot {
        self.cap
    }
}
