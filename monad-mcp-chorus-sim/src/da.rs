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

//! An in-memory mock of the data-availability layer.
//!
//! The mock reduces the DA layer to its consensus-visible contract: a
//! submitted proposal becomes locally decoded immediately and is announced
//! to the other nodes as one [`DaAnnouncement`] message over the simulated
//! network (so announcements see the same latency as consensus messages);
//! a received announcement makes the proposal decoded at the receiver.
//! Chunking, erasure coding, and per-chunk forwarding are below this
//! abstraction and deliberately absent. Equivocation (conflicting roots for
//! the same proposal slot) is out of scope: the first root wins and
//! conflicts are only logged.
//!
//! Proposer identity is not validated on receipt yet — announcements are
//! trusted like all sim messages. The validation seam (checking the chunk
//! header signature against the scheduled proposer) belongs to the real DA
//! layer.

use std::{collections::BTreeMap, sync::Mutex};

use bytes::Bytes;
use chorus::{
    da::{DataAvailability, FetchProposalError},
    types::{
        MerkleRoot, NodeId, OpaqueChunkHeader, ProposalIndex, ProposalMeta, ProposalSignature, Slot,
    },
};
use monad_mcp_chorus::stub as chorus;

/// The mock's dissemination unit: everything a receiver needs to treat the
/// proposal at `(slot, index)` as decoded.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DaAnnouncement {
    pub slot: Slot,
    pub index: ProposalIndex,
    pub meta: ProposalMeta,
}

#[derive(Default)]
struct MockDaState {
    /// Locally decoded proposals.
    decoded: BTreeMap<(Slot, ProposalIndex), ProposalMeta>,
    /// Announcements queued for broadcast by the node wiring.
    outbox: Vec<DaAnnouncement>,
}

/// One node's [`DataAvailability`] instance; see the module docs.
#[derive(Default)]
pub struct MockDa {
    state: Mutex<MockDaState>,
}

impl MockDa {
    pub fn new() -> Self {
        Self::default()
    }

    /// An announcement arrived over the simulated network: the proposal is
    /// now decoded here. First root wins; a conflicting root would be
    /// equivocation, which the mock only logs.
    pub fn receive_announcement(&self, announcement: DaAnnouncement) {
        let mut state = self.state.lock().expect("mock DA state poisoned");
        let key = (announcement.slot, announcement.index);
        match state.decoded.get(&key) {
            None => {
                state.decoded.insert(key, announcement.meta);
            }
            Some(existing) if existing.root != announcement.meta.root => {
                tracing::warn!(
                    slot = ?announcement.slot,
                    index = announcement.index,
                    "conflicting proposal roots; equivocation is out of scope for the mock"
                );
            }
            Some(_) => {} // duplicate delivery (e.g. loopback), idempotent
        }
    }

    /// The announcements queued by [`DataAvailability::submit_proposal`]
    /// since the last drain, for broadcast by the node wiring.
    pub fn drain_announcements(&self) -> Vec<DaAnnouncement> {
        let mut state = self.state.lock().expect("mock DA state poisoned");
        std::mem::take(&mut state.outbox)
    }
}

impl DataAvailability for MockDa {
    fn proposal_decoded(&self, slot: Slot, index: ProposalIndex, root: &MerkleRoot) -> bool {
        let state = self.state.lock().expect("mock DA state poisoned");
        state
            .decoded
            .get(&(slot, index))
            .is_some_and(|meta| meta.root == *root)
    }

    fn observe_proposal(&self, _slot: Slot, _index: ProposalIndex, _meta: ProposalMeta) {
        // Observed metadata does not make a payload decoded; the mock does
        // not track observation separately.
    }

    fn fetch_proposal(
        &self,
        slot: Slot,
        index: ProposalIndex,
    ) -> Result<ProposalMeta, FetchProposalError> {
        let state = self.state.lock().expect("mock DA state poisoned");
        state
            .decoded
            .get(&(slot, index))
            .cloned()
            .ok_or(FetchProposalError::Absent)
    }

    fn submit_proposal(&self, slot: Slot, index: ProposalIndex, payload: Bytes) {
        let meta = ProposalMeta {
            root: mock_root(&payload),
            sig: ProposalSignature,
            opaque_header: OpaqueChunkHeader,
        };
        let mut state = self.state.lock().expect("mock DA state poisoned");
        let previous = state.decoded.insert((slot, index), meta.clone());
        debug_assert!(previous.is_none(), "proposal submitted twice");
        state.outbox.push(DaAnnouncement { slot, index, meta });
    }
}

/// The canonical mock payload of `proposer` for `(slot, index)`. Tests
/// derive the expected merkle roots from the same convention.
pub fn mock_payload(proposer: NodeId, slot: Slot, index: ProposalIndex) -> Bytes {
    Bytes::from(format!("{}/{}/{}", u64::from(proposer), slot.get(), index))
}

/// A deterministic stand-in for the payload's merkle root (FNV-1a).
pub fn mock_root(payload: &Bytes) -> MerkleRoot {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in payload {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    MerkleRoot(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn announcement(slot: u64, index: ProposalIndex, payload: &Bytes) -> DaAnnouncement {
        DaAnnouncement {
            slot: Slot(slot),
            index,
            meta: ProposalMeta {
                root: mock_root(payload),
                sig: ProposalSignature,
                opaque_header: OpaqueChunkHeader,
            },
        }
    }

    #[test]
    fn submitted_proposal_is_decoded_and_announced() {
        let da = MockDa::new();
        let payload = mock_payload(NodeId::dummy(0), Slot(3), 1);

        da.submit_proposal(Slot(3), 1, payload.clone());

        let meta = da.fetch_proposal(Slot(3), 1).ok().unwrap();
        assert_eq!(meta.root, mock_root(&payload));
        assert!(da.proposal_decoded(Slot(3), 1, &meta.root));
        assert!(!da.proposal_decoded(Slot(3), 0, &meta.root));

        let announcements = da.drain_announcements();
        assert_eq!(announcements.len(), 1);
        assert_eq!(announcements[0].meta.root, meta.root);
        assert!(da.drain_announcements().is_empty());
    }

    #[test]
    fn received_announcement_becomes_decoded() {
        let da = MockDa::new();
        let payload = mock_payload(NodeId::dummy(1), Slot(0), 0);
        let announcement = announcement(0, 0, &payload);

        assert!(matches!(
            da.fetch_proposal(Slot(0), 0),
            Err(FetchProposalError::Absent)
        ));

        da.receive_announcement(announcement.clone());
        // duplicate delivery (broadcast loopback) is idempotent
        da.receive_announcement(announcement.clone());

        assert_eq!(
            da.fetch_proposal(Slot(0), 0).ok().unwrap().root,
            announcement.meta.root
        );
        // nothing to re-announce: only submissions queue announcements
        assert!(da.drain_announcements().is_empty());
    }

    #[test]
    fn conflicting_root_does_not_replace_the_first() {
        let da = MockDa::new();
        let first = mock_payload(NodeId::dummy(1), Slot(0), 0);
        let second = mock_payload(NodeId::dummy(2), Slot(0), 0);

        da.receive_announcement(announcement(0, 0, &first));
        da.receive_announcement(announcement(0, 0, &second));

        assert_eq!(
            da.fetch_proposal(Slot(0), 0).ok().unwrap().root,
            mock_root(&first)
        );
    }
}
