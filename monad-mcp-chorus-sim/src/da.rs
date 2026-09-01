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
//! Availability reaches consensus the way the real layer reports it: as
//! `ChorusDAEvent`s, which the node wiring (see [`crate::node`]) drains
//! from [`MockDa::drain_available`] and injects into the runtime. A newly
//! available proposal yields a `HeaderSeen` for its signed header followed
//! by a `Decoded` for its root — the mock treats availability as whole
//! rather than per chunk.
//!
//! The mock owns the header checks the real layer owns: an announcement is
//! accepted only if its signer holds the announced proposal index at that
//! slot according to the proposer schedule. That is the same schedule
//! consensus derives its own `HeaderAuth` from, so the two cannot disagree.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use chorus::{
    da::DataAvailability,
    env::{D25, EncodingScheme, MerkleHash, ProposalSignature},
    types::{MerkleRoot, NodeId, ProposalHeader, ProposalIndex, ProposerSchedule, Slot},
};
use monad_mcp_chorus::stub as chorus;

/// A shared proposer schedule, as both consensus and the mock DA layer read
/// it.
pub type Schedule = Arc<dyn ProposerSchedule + Send + Sync>;

/// The mock's dissemination unit: the signed header a receiver needs to
/// treat the proposal at `(header.slot, index)` as available.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DaAnnouncement {
    pub index: ProposalIndex,
    pub header: ProposalHeader,
}

impl DaAnnouncement {
    pub fn slot(&self) -> Slot {
        self.header.slot
    }
}

#[derive(Default)]
struct MockDaState {
    /// Locally available (decoded) proposals.
    available: BTreeMap<(Slot, ProposalIndex), ProposalHeader>,
    /// Announcements queued for broadcast by the node wiring.
    outbox: Vec<DaAnnouncement>,
    /// Proposals that became available since the last drain, to be reported
    /// to consensus as DA events.
    fresh: Vec<DaAnnouncement>,
}

/// One node's [`DataAvailability`] instance; see the module docs.
pub struct MockDa {
    me: NodeId,
    schedule: Schedule,
    state: Mutex<MockDaState>,
}

impl MockDa {
    pub fn new(me: NodeId, schedule: Schedule) -> Self {
        Self {
            me,
            schedule,
            state: Mutex::new(MockDaState::default()),
        }
    }

    /// Whether `header`'s signer holds `index` at the header's slot. The
    /// real layer performs this check on the chunk header signature; here
    /// the signer is carried in the clear.
    fn authentic(&self, index: ProposalIndex, header: &ProposalHeader) -> bool {
        self.schedule
            .proposer_index_at(header.slot, &header.sig.signer)
            .is_ok_and(|held| held == Some(index))
    }

    /// An announcement arrived over the simulated network: the proposal is
    /// now available here. First root wins; a conflicting root would be
    /// equivocation, which the mock only logs.
    pub fn receive_announcement(&self, announcement: DaAnnouncement) {
        if !self.authentic(announcement.index, &announcement.header) {
            tracing::warn!(
                slot = ?announcement.slot(),
                index = announcement.index,
                "announcement from a node that does not hold the index; dropped"
            );
            return;
        }
        self.record(announcement);
    }

    fn record(&self, announcement: DaAnnouncement) {
        let mut state = self.state.lock().expect("mock DA state poisoned");
        let key = (announcement.slot(), announcement.index);
        match state.available.get(&key) {
            None => {
                state.available.insert(key, announcement.header.clone());
                state.fresh.push(announcement);
            }
            Some(existing) if existing.root != announcement.header.root => {
                tracing::warn!(
                    slot = ?announcement.slot(),
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

    /// The proposals that became available since the last drain, for the
    /// node wiring to report to consensus as DA events.
    pub fn drain_available(&self) -> Vec<DaAnnouncement> {
        let mut state = self.state.lock().expect("mock DA state poisoned");
        std::mem::take(&mut state.fresh)
    }

    /// The locally available proposal at `(slot, index)`, if any.
    pub fn available(&self, slot: Slot, index: ProposalIndex) -> Option<ProposalHeader> {
        let state = self.state.lock().expect("mock DA state poisoned");
        state.available.get(&(slot, index)).cloned()
    }
}

impl DataAvailability for MockDa {
    fn submit_proposal(&self, slot: Slot, index: ProposalIndex, payload: Bytes) {
        let header = mock_header(self.me, slot, &payload);
        let announcement = DaAnnouncement { index, header };

        debug_assert!(
            self.authentic(index, &announcement.header),
            "submitting a proposal for an index this node does not hold"
        );
        debug_assert!(
            self.available(slot, index).is_none(),
            "proposal submitted twice"
        );

        self.record(announcement.clone());
        let mut state = self.state.lock().expect("mock DA state poisoned");
        state.outbox.push(announcement);
    }
}

/// The canonical mock payload of `proposer` for `(slot, index)`. Tests
/// derive the expected merkle roots from the same convention.
pub fn mock_payload(proposer: NodeId, slot: Slot, index: ProposalIndex) -> Bytes {
    Bytes::from(format!("{}/{}/{}", u64::from(proposer), slot.get(), index))
}

/// The header the mock layer signs for `payload`. The DA-owned fields are
/// opaque to consensus, so they carry only what the mock needs: the signer,
/// for the index check, and the payload length.
pub fn mock_header(proposer: NodeId, slot: Slot, payload: &Bytes) -> ProposalHeader {
    ProposalHeader {
        slot,
        root: mock_root(payload),
        sig: ProposalSignature {
            signer: proposer,
            checksum: 0,
        },
        scheme: EncodingScheme::D25(D25 {
            msg_len: u32::try_from(payload.len()).expect("mock payload fits in u32"),
            unix_ts: 0,
            depth: 0,
        }),
    }
}

/// A deterministic stand-in for the payload's merkle root (FNV-1a, spread
/// over the hash width).
pub fn mock_root(payload: &Bytes) -> MerkleRoot {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in payload {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let digest = hash.to_be_bytes();
    let mut bytes = [0u8; 20];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = digest[i % digest.len()];
    }
    MerkleRoot(MerkleHash(bytes))
}

#[cfg(test)]
mod tests {
    use chorus::types::FixedProposerSchedule;

    use super::*;

    fn schedule(proposers: Vec<NodeId>) -> Schedule {
        Arc::new(FixedProposerSchedule::new(proposers))
    }

    // index 0 -> validator 0, index 1 -> validator 1
    fn da(me: NodeId) -> MockDa {
        MockDa::new(me, schedule(vec![NodeId::dummy(0), NodeId::dummy(1)]))
    }

    fn announcement(proposer: NodeId, slot: u64, index: ProposalIndex) -> DaAnnouncement {
        let slot = Slot(slot);
        let payload = mock_payload(proposer, slot, index);
        DaAnnouncement {
            index,
            header: mock_header(proposer, slot, &payload),
        }
    }

    #[test]
    fn submitted_proposal_is_available_and_announced() {
        let da = da(NodeId::dummy(1));
        let payload = mock_payload(NodeId::dummy(1), Slot(3), 1);

        da.submit_proposal(Slot(3), 1, payload.clone());

        let header = da.available(Slot(3), 1).expect("submission is available");
        assert_eq!(header.root, mock_root(&payload));
        assert!(da.available(Slot(3), 0).is_none());

        // the submission is both announced and reported to consensus
        let announcements = da.drain_announcements();
        assert_eq!(announcements.len(), 1);
        assert_eq!(announcements[0].header.root, header.root);
        assert!(da.drain_announcements().is_empty());

        let fresh = da.drain_available();
        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[0].header.root, header.root);
        assert!(da.drain_available().is_empty());
    }

    #[test]
    fn received_announcement_becomes_available() {
        let da = da(NodeId::dummy(0));
        let announcement = announcement(NodeId::dummy(1), 0, 1);

        assert!(da.available(Slot(0), 1).is_none());

        da.receive_announcement(announcement.clone());
        // duplicate delivery (broadcast loopback) is idempotent
        da.receive_announcement(announcement.clone());

        assert_eq!(
            da.available(Slot(0), 1).expect("announced").root,
            announcement.header.root
        );
        // reported once, and nothing to re-announce: only submissions do
        assert_eq!(da.drain_available().len(), 1);
        assert!(da.drain_announcements().is_empty());
    }

    #[test]
    fn an_announcement_for_an_index_the_signer_does_not_hold_is_dropped() {
        let da = da(NodeId::dummy(0));
        // validator 1 holds index 1, not index 0
        let mut forged = announcement(NodeId::dummy(1), 0, 1);
        forged.index = 0;

        da.receive_announcement(forged);

        assert!(da.available(Slot(0), 0).is_none());
        assert!(da.drain_available().is_empty());
    }

    #[test]
    fn conflicting_root_does_not_replace_the_first() {
        let da = da(NodeId::dummy(0));
        let first = announcement(NodeId::dummy(1), 0, 1);

        // same proposer and index, a different payload behind the root
        let mut second = first.clone();
        second.header.root = mock_root(&Bytes::from_static(b"rival"));

        da.receive_announcement(first.clone());
        da.receive_announcement(second);

        assert_eq!(
            da.available(Slot(0), 1).expect("announced").root,
            first.header.root
        );
    }
}
