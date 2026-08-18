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

//! Retrieving the block behind entries a certificate has already settled.
//!
//! Agreement runs over `entries(x)` alone, so a validator can hold a prepare
//! or commit certificate for a block it has never seen. Two things then need
//! the block itself and cannot be served by the certificate: a decision has to
//! hand the block on to whatever consumes the slot, and a leader bound by a
//! lock has to put the locked block back on the wire. This module fetches it.
//!
//! A request is broadcast and names the entries; whoever holds the matching
//! block unicasts it back. Requests are unsigned -- the sender is
//! network-authenticated like every other message, and a request grants
//! nothing, so there is nothing for a signature to bind. Responses are not
//! trusted either: one is accepted only if its entries match a request this
//! instance actually made and every certified entry in it verifies. The
//! entries are the block's identity, so a response that passes those checks is
//! the block that was asked for, whoever sent it.
//!
//! Like the vote collectors, this is a dumb store: it never looks at the
//! state machine's phase and never decides anything. The state machine asks it
//! what it holds, tells it what to want, and hands it every block it
//! legitimately comes by -- its own input, an accepted proposal, a retrieved
//! response.

use std::collections::{HashMap, HashSet};

use super::{
    super::{
        fast::Entry,
        types::{ProposalMap, Slot, ValidatorData},
    },
    PartialBlock,
    monad_mvba::metablock::{entries_of, partial_block_is_valid},
};

/// `⟨BlockRequest, slot, entries(x)⟩`: broadcast ask for the block behind
/// `entries`.
///
/// The entries identify the block because that is what every certificate over
/// it certifies: a requester that got them from a certificate is naming
/// something a supermajority has attested to, and a response can be checked
/// against them without trusting the responder.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct BlockRequestMsg {
    pub slot: Slot,
    pub entries: ProposalMap<Entry>,
}

/// `⟨BlockResponse, slot, x⟩`: unicast answer to a request, carrying the block.
///
/// Sent back to the requester alone: everyone else either holds the block
/// already or has broadcast a request of their own.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct BlockResponseMsg {
    pub slot: Slot,
    pub block: PartialBlock,
}

/// The blocks this instance holds, and the ones it is waiting for.
pub(crate) struct BlockSync {
    slot: Slot,
    /// Blocks held, keyed by the entries that identify them.
    known: HashMap<ProposalMap<Entry>, PartialBlock>,
    /// Entries a request has gone out for and no valid response has come back
    /// for yet.
    pending: HashSet<ProposalMap<Entry>>,
}

impl BlockSync {
    pub(crate) fn new(slot: Slot) -> Self {
        Self {
            slot,
            known: HashMap::new(),
            pending: HashSet::new(),
        }
    }

    /// Store a block this instance holds legitimately: its own input, a
    /// proposal it accepted, or a response it validated. Keyed by the entries,
    /// so storing the same block twice is a no-op and any request for it is
    /// satisfied.
    pub(crate) fn remember(&mut self, block: PartialBlock) {
        let entries = entries_of(&block);
        self.pending.remove(&entries);
        self.known.insert(entries, block);
    }

    pub(crate) fn get(&self, entries: &ProposalMap<Entry>) -> Option<&PartialBlock> {
        self.known.get(entries)
    }

    /// Ask for the block behind `entries`, returning the request to broadcast.
    ///
    /// `None` when there is nothing to send: the block is already held, or a
    /// request for it is already outstanding. That dedup is what lets the
    /// caller ask on every pass through the state machine without flooding the
    /// network -- the retry is a separate, timer-driven decision.
    pub(crate) fn want(&mut self, entries: &ProposalMap<Entry>) -> Option<BlockRequestMsg> {
        if self.known.contains_key(entries) || !self.pending.insert(entries.clone()) {
            return None;
        }

        Some(BlockRequestMsg {
            slot: self.slot,
            entries: entries.clone(),
        })
    }

    /// The requests still outstanding, for the caller to re-broadcast when a
    /// view times out: a request or a response lost in the network would
    /// otherwise stall a decision forever.
    pub(crate) fn pending_requests(&self) -> impl Iterator<Item = BlockRequestMsg> + '_ {
        self.pending.iter().map(|entries| BlockRequestMsg {
            slot: self.slot,
            entries: entries.clone(),
        })
    }

    /// Answer a request from what is held; the caller unicasts the response
    /// back to the sender. `None` when this instance cannot help.
    pub(crate) fn handle_request(&self, request: &BlockRequestMsg) -> Option<BlockResponseMsg> {
        if request.slot != self.slot {
            return None;
        }

        Some(BlockResponseMsg {
            slot: self.slot,
            block: self.get(&request.entries)?.clone(),
        })
    }

    /// Take in a response: accepted only if its entries are ones this instance
    /// asked for and the block is well-formed under `slot`. `true` when it was
    /// stored, which is the caller's cue to re-run the state machine.
    ///
    /// Matching against `pending` is what keeps this from being a way to fill
    /// the store: a block nobody asked for is dropped, however valid.
    pub(crate) fn handle_response(
        &mut self,
        response: BlockResponseMsg,
        num_proposals: usize,
        validator_data: &ValidatorData,
    ) -> bool {
        if response.slot != self.slot {
            return false;
        }

        let entries = entries_of(&response.block);
        if !self.pending.contains(&entries) {
            return false;
        }

        if !partial_block_is_valid(&response.block, self.slot, num_proposals, validator_data) {
            return false;
        }

        self.pending.remove(&entries);
        self.known.insert(entries, response.block);
        true
    }

    /// Drop everything not reachable from what the state machine still cares
    /// about: its own input, the current phase, its lock, and the decision.
    ///
    /// Requests are dropped along with blocks. A want that is still live is
    /// re-registered on the next pass through the state machine, so the only
    /// thing this loses is the flood a Byzantine peer could otherwise make
    /// this instance hold.
    pub(crate) fn gc(&mut self, keep: &HashSet<ProposalMap<Entry>>) {
        self.known.retain(|entries, _| keep.contains(entries));
        self.pending.retain(|entries| keep.contains(entries));
    }
}
