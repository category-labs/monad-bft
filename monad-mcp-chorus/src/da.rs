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

//! The proposer-side seam towards the data-availability layer.
//!
//! The DA layer is its own component with its own dissemination transport
//! (chunking, erasure coding, forwarding). Consensus observes its local
//! state through the *consensus-side* seam, which is event-driven and lives
//! elsewhere: the layer reports `ChorusDAEvent`s (a signed header seen,
//! obligations fulfilled, a proposal decoded) that the slot machinery folds
//! into its per-index `ProposalAvailability`, and consensus directs the
//! layer back through `ChorusDACommand`.
//!
//! What remains here is the other direction, which that event seam does not
//! cover: how the proposal-creation component (see [`super::proposing`])
//! hands a sealed payload to the layer for dissemination.

use bytes::Bytes;

use super::types::{ProposalIndex, Slot};

/// A shared handle to the node's data-availability layer.
pub type DaHandle = std::sync::Arc<dyn DataAvailability + Send + Sync>;

/// The proposer's entry point into the data-availability layer.
pub trait DataAvailability {
    /// Seal `payload` as this node's proposal for `(slot, index)` and start
    /// disseminating it. The caller (the proposal-creation component) is
    /// responsible for holding the proposal slot's index and for the timing;
    /// the layer is responsible for chunking, signing the chunk headers, and
    /// dissemination.
    fn submit_proposal(&self, slot: Slot, index: ProposalIndex, payload: Bytes);
}

/// A stub [`DataAvailability`] that drops every submission, so no proposal
/// ever becomes available and every slot finalizes empty. Used by tests that
/// exercise the consensus machinery without proposals.
pub struct NullDa;

impl DataAvailability for NullDa {
    fn submit_proposal(&self, _slot: Slot, _index: ProposalIndex, _payload: Bytes) {
        // do nothing
    }
}
