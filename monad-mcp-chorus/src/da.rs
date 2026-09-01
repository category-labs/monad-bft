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

//! The seam towards the data-availability layer.
//!
//! The DA layer is its own component with its own dissemination transport
//! (chunking, erasure coding, forwarding); consensus only observes its local
//! state through [`DataAvailability`]. The trait has two sides:
//!
//! * the *consensus side* ([`DataAvailability::fetch_proposal`],
//!   [`DataAvailability::proposal_decoded`],
//!   [`DataAvailability::observe_proposal`]) — what the slot machinery reads
//!   when voting on proposals;
//! * the *proposer side* ([`DataAvailability::submit_proposal`]) — how the
//!   proposal-creation component (see [`super::proposing`]) hands a sealed
//!   payload to the layer for dissemination. Kept on the same trait while
//!   both sides are stubs; it may split off with the real DA component.
//!
//! No real DA layer exists yet: [`NullDa`] never carries any proposal (all
//! slots finalize empty), and the sim crate provides an in-memory mock that
//! disseminates announcements over the simulated network.

use bytes::Bytes;

use super::types::{EquivCert, MerkleRoot, ProposalIndex, ProposalMeta, Slot};

/// A shared handle to the node's data-availability layer.
pub type DaHandle = std::sync::Arc<dyn DataAvailability + Send + Sync>;

pub enum FetchProposalError {
    Absent,
    Equivocation(EquivCert),
}

/// The node-local view of the data-availability layer.
pub trait DataAvailability {
    /// Whether the proposal with `root` at `(slot, index)` is fully decoded
    /// locally.
    fn proposal_decoded(&self, slot: Slot, index: ProposalIndex, root: &MerkleRoot) -> bool;

    /// Inform DA about proposals we received through consensus messages
    /// (e.g. FallbackSignedEntry).
    fn observe_proposal(&self, slot: Slot, index: ProposalIndex, meta: ProposalMeta);

    /// The locally decoded proposal at `(slot, index)`, if any.
    ///
    /// Please do note that there is an potential to have more than one
    /// proposal meta for the same root. This can occur if the proposer
    /// sign the same root with different chunk header fields (e.g. varying
    /// unix_ts_ms).
    ///
    /// Q: How should we deal with this situation? Should we count it as
    /// equivocation? Or should we simply ignore that? Our current
    /// implementation follows the paper which doesn't currently consider
    /// this case as equivocation.
    fn fetch_proposal(
        &self,
        slot: Slot,
        index: ProposalIndex,
    ) -> Result<ProposalMeta, FetchProposalError>;

    /// Seal `payload` as this node's proposal for `(slot, index)` and start
    /// disseminating it. The caller (the proposal-creation component) is
    /// responsible for holding the proposal slot's index and for the timing;
    /// the layer is responsible for chunking, signing the chunk headers, and
    /// dissemination.
    fn submit_proposal(&self, slot: Slot, index: ProposalIndex, payload: Bytes);
}

/// A stub [`DataAvailability`] that never carries any proposal; every slot
/// votes negative on every index. Used by tests that exercise the consensus
/// machinery without proposals.
pub struct NullDa;

impl DataAvailability for NullDa {
    fn proposal_decoded(&self, _slot: Slot, _index: ProposalIndex, _root: &MerkleRoot) -> bool {
        false
    }

    fn observe_proposal(&self, _slot: Slot, _index: ProposalIndex, _meta: ProposalMeta) {
        // do nothing
    }

    fn fetch_proposal(
        &self,
        _slot: Slot,
        _index: ProposalIndex,
    ) -> Result<ProposalMeta, FetchProposalError> {
        Err(FetchProposalError::Absent)
    }

    fn submit_proposal(&self, _slot: Slot, _index: ProposalIndex, _payload: Bytes) {
        // do nothing
    }
}
