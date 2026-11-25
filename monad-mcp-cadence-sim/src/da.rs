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
//! `ChorusDAEvent`s, which the node wiring (see [`crate::node`]) takes
//! from [`MockDa::poll`] and injects into the runtime. A newly available
//! proposal yields a `HeaderSeen` for its signed header followed by a
//! `Decoded` for its root — the mock treats availability as whole rather
//! than per chunk.
//!
//! The mock owns the header check the real layer owns: an announcement is
//! accepted only if the header authenticates to the announced proposal
//! lane. It is handed the very `HeaderAuth` and epoch handle consensus
//! votes with, so the two cannot disagree about who proposes where.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use bytes::Bytes;
use cadence::{
    env::{D33, EncodingScheme, MerkleHash},
    slot::chorus::{ChorusDAEvent, ProposalDAEvent},
    types::{HeaderAuth, MerkleRoot, ProposalHeader, ProposalId, SignedProposalHeader},
};
use monad_mcp_cadence::{spec::proposal::ProposalHeader as _, stub as cadence};
use monad_mcp_common::{
    Lane, Proposal, Slot,
    spec::{ProposalKeyPair as _, ProposalSignature as _},
    stub::{NodeId, ProposalKeyPair},
};
use monad_mcp_epoch::stub::EpochHandle;

/// The mock's dissemination unit: the signed header a receiver needs to
/// treat the proposal at `(header.slot, lane)` as available.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DaAnnouncement {
    pub lane: Lane,
    pub header: SignedProposalHeader,
}

impl DaAnnouncement {
    pub fn slot(&self) -> Slot {
        self.header.header.scheme.slot()
    }

    pub fn root(&self) -> MerkleRoot {
        self.header.header.root
    }

    pub fn pid(&self) -> ProposalId {
        self.header.pid()
    }
}

pub enum MockDaOutput {
    // for broadcast to the other nodes
    Announce(DaAnnouncement),
    // for the slot consensus of this node
    Event(Slot, ChorusDAEvent),
}

/// One node's mock DA layer; see the module docs.
pub struct MockDa {
    me: NodeId,
    epoch_handle: EpochHandle,
    /// The header check consensus votes with; see the module docs.
    header_auth: Arc<HeaderAuth>,
    /// Locally available (decoded) proposals.
    available: BTreeMap<(Slot, Lane), SignedProposalHeader>,
    outputs: VecDeque<MockDaOutput>,
}

impl MockDa {
    pub fn new(me: NodeId, epoch_handle: EpochHandle, header_auth: Arc<HeaderAuth>) -> Self {
        Self {
            me,
            epoch_handle,
            header_auth,
            available: BTreeMap::new(),
            outputs: VecDeque::new(),
        }
    }

    /// Whether the header authenticates to `lane`. The real layer performs
    /// this check on the chunk header signature; here the signer is carried
    /// in the clear.
    fn authentic(&self, lane: Lane, header: &SignedProposalHeader) -> bool {
        if header.lane() != lane {
            return false;
        }
        let slot = header.slot();
        let Ok(validators) = self.epoch_handle.validators_at(slot) else {
            return false;
        };
        let Ok(proposer_set) = self.epoch_handle.proposers_at(slot) else {
            return false;
        };
        let Some(proposer) = proposer_set.proposer(lane) else {
            return false;
        };
        (self.header_auth)(header, &validators, &proposer)
    }

    pub fn handle_proposal(&mut self, proposal: Proposal<Bytes>) {
        let Proposal {
            slot,
            lane,
            payload,
        } = proposal;
        let header = mock_header(self.me, slot, lane, &payload);
        let announcement = DaAnnouncement { lane, header };

        debug_assert!(
            self.authentic(lane, &announcement.header),
            "submitting a proposal for an lane this node does not hold"
        );
        debug_assert!(
            self.available(slot, lane).is_none(),
            "proposal submitted twice"
        );

        self.outputs
            .push_back(MockDaOutput::Announce(announcement.clone()));
        self.record(announcement);
    }

    /// An announcement arrived over the simulated network: the proposal is
    /// now available here. First root wins; a conflicting root would be
    /// equivocation, which the mock only logs.
    pub fn handle_announcement(&mut self, announcement: DaAnnouncement) {
        if !self.authentic(announcement.lane, &announcement.header) {
            tracing::warn!(
                slot = ?announcement.slot(),
                lane = usize::from(announcement.lane),
                "announcement from a node that does not hold the lane; dropped"
            );
            return;
        }
        self.record(announcement);
    }

    pub fn poll(&mut self) -> Option<MockDaOutput> {
        self.outputs.pop_front()
    }

    fn record(&mut self, announcement: DaAnnouncement) {
        let slot = announcement.slot();
        let lane = announcement.lane;
        match self.available.get(&(slot, lane)) {
            None => {
                let pid = announcement.pid();
                let header = announcement.header;
                self.available.insert((slot, lane), header.clone());
                // The mock makes a proposal available as a whole: the signed
                // header and its decoding at once.
                for event in [
                    ProposalDAEvent::HeaderSeen(header),
                    ProposalDAEvent::Decoded(pid),
                ] {
                    let event = ChorusDAEvent { j: lane, event };
                    self.outputs.push_back(MockDaOutput::Event(slot, event));
                }
            }
            Some(existing) if existing.header.root != announcement.root() => {
                tracing::warn!(
                    ?slot,
                    lane = usize::from(lane),
                    "conflicting proposal roots; equivocation is out of scope for the mock"
                );
            }
            Some(_) => {} // duplicate delivery (e.g. loopback), idempotent
        }
    }

    /// The locally available proposal at `(slot, lane)`, if any.
    pub fn available(&self, slot: Slot, lane: Lane) -> Option<&SignedProposalHeader> {
        self.available.get(&(slot, lane))
    }
}

/// The [`HeaderAuth`] of the mock: a header authenticates when the proposer
/// of its lane made its mock signature.
///
/// The production data-availability layer owns two further checks the
/// header carries for it -- the encoding scheme is the canonical one for
/// the validator set, and the signature covers the header.
pub fn header_auth() -> Arc<HeaderAuth> {
    Arc::new(|header, _, proposer| header.sig.recover_author(&[]).as_ref() == Some(proposer))
}

/// The canonical mock payload of `proposer` for `(slot, lane)`. Tests
/// derive the expected merkle roots from the same convention.
pub fn mock_payload(proposer: NodeId, slot: Slot, lane: Lane) -> Bytes {
    Bytes::from(format!(
        "{}/{}/{}",
        u64::from(proposer),
        slot.get(),
        usize::from(lane)
    ))
}

/// The header the mock layer signs for `payload`. The DA-owned fields are
/// opaque to consensus, so they carry only what the mock needs: the signer
/// and the lane, for the lane check, and the payload length.
pub fn mock_header(
    proposer: NodeId,
    slot: Slot,
    lane: Lane,
    payload: &Bytes,
) -> SignedProposalHeader {
    SignedProposalHeader {
        header: ProposalHeader {
            root: mock_root(payload),
            scheme: EncodingScheme::D33(D33 {
                slot,
                lane,
                depth: 0,
                msg_len: u32::try_from(payload.len()).expect("mock payload fits in u32"),
                unix_ts: 0,
            }),
        },
        // the mock signs no data: only the signer is read
        sig: ProposalKeyPair::dummy(proposer).sign(&[]),
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
    use monad_mcp_common::stub::ValidatorData;
    use monad_mcp_epoch::stub::election::FixedProposerSchedule;

    use super::*;

    // index 0 -> validator 0, index 1 -> validator 1
    fn da(me: NodeId) -> MockDa {
        let election = FixedProposerSchedule::new(vec![NodeId::dummy(0), NodeId::dummy(1)]);
        let epoch_handle = EpochHandle::new(ValidatorData::dummy(4), election);
        MockDa::new(me, epoch_handle, header_auth())
    }

    fn announcement(proposer: NodeId, slot: u64, lane: Lane) -> DaAnnouncement {
        let slot = Slot(slot);
        let payload = mock_payload(proposer, slot, lane);
        DaAnnouncement {
            lane,
            header: mock_header(proposer, slot, lane, &payload),
        }
    }

    fn header_seen(output: Option<MockDaOutput>) -> Option<MerkleRoot> {
        let MockDaOutput::Event(_, event) = output? else {
            return None;
        };
        let ProposalDAEvent::HeaderSeen(header) = event.event else {
            return None;
        };
        Some(header.header.root)
    }

    fn decoded(output: Option<MockDaOutput>) -> bool {
        let Some(MockDaOutput::Event(_, event)) = output else {
            return false;
        };
        matches!(event.event, ProposalDAEvent::Decoded(_))
    }

    #[test]
    fn submitted_proposal_is_available_and_announced() {
        let mut da = da(NodeId::dummy(1));
        let slot = Slot(3);
        let lane = Lane::new_unchecked(1);
        let payload = mock_payload(NodeId::dummy(1), slot, lane);
        let root = mock_root(&payload);

        da.handle_proposal(Proposal {
            slot,
            lane,
            payload,
        });

        let header = da.available(slot, lane).expect("submission is available");
        assert_eq!(header.header.root, root);
        assert!(da.available(slot, Lane::new_unchecked(0)).is_none());

        // the submission is both announced and reported to consensus
        let Some(MockDaOutput::Announce(announcement)) = da.poll() else {
            panic!("an announcement comes first");
        };
        assert_eq!(announcement.root(), root);
        assert_eq!(header_seen(da.poll()), Some(root));
        assert!(decoded(da.poll()));
        assert!(da.poll().is_none());
    }

    #[test]
    fn received_announcement_becomes_available() {
        let mut da = da(NodeId::dummy(0));
        let announcement = announcement(NodeId::dummy(1), 0, Lane::new_unchecked(1));

        assert!(da.available(Slot(0), Lane::new_unchecked(1)).is_none());

        da.handle_announcement(announcement.clone());
        // duplicate delivery (broadcast loopback) is idempotent
        da.handle_announcement(announcement.clone());

        assert_eq!(
            da.available(Slot(0), Lane::new_unchecked(1))
                .expect("announced")
                .header
                .root,
            announcement.root()
        );
        // reported once, and nothing to re-announce: only submissions do
        assert_eq!(header_seen(da.poll()), Some(announcement.root()));
        assert!(decoded(da.poll()));
        assert!(da.poll().is_none());
    }

    #[test]
    fn an_announcement_for_an_index_the_signer_does_not_hold_is_dropped() {
        let mut da = da(NodeId::dummy(0));
        // validator 1 holds index 1, not index 0
        let mut forged = announcement(NodeId::dummy(1), 0, Lane::new_unchecked(1));
        forged.lane = Lane::new_unchecked(0);

        da.handle_announcement(forged);

        assert!(da.available(Slot(0), Lane::new_unchecked(0)).is_none());
        assert!(da.poll().is_none());
    }

    #[test]
    fn conflicting_root_does_not_replace_the_first() {
        let mut da = da(NodeId::dummy(0));
        let first = announcement(NodeId::dummy(1), 0, Lane::new_unchecked(1));

        // same proposer and lane, a different payload behind the root
        let mut second = first.clone();
        second.header.header.root = mock_root(&Bytes::from_static(b"rival"));

        da.handle_announcement(first.clone());
        da.handle_announcement(second);

        assert_eq!(
            da.available(Slot(0), Lane::new_unchecked(1))
                .expect("announced")
                .header
                .root,
            first.root()
        );
    }
}
