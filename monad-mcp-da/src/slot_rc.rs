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

use std::collections::HashMap;

use monad_mcp_chorus::spec::proposal::HeaderAuth as _;

use super::{
    chunk::ProposalEnvelope,
    header::InvalidProposalHeader,
    runtime::{DAOutput, EpochHandle},
    types::{ChorusDACommand, ChorusDAEvent, ProposalHeader, ProposalIndex, Slot},
};

// per-slot raptorcast tracking. mainly handles proposer-related
// validation.
pub struct SlotRaptorcast {
    epoch_handle: EpochHandle,
    slot: Slot,

    // memoize the proposal index of headers that authenticated. todo:
    // bound per proposer.
    authenticated_headers: HashMap<ProposalHeader, ProposalIndex>,

    // events pending delivery since the last drain
    out_events: Vec<ChorusDAEvent>,
}

impl SlotRaptorcast {
    pub fn new(epoch_handle: &EpochHandle, slot: Slot) -> Self {
        Self {
            epoch_handle: epoch_handle.clone(),
            slot,
            authenticated_headers: HashMap::new(),
            out_events: Vec::new(),
        }
    }

    pub fn drain_events(&mut self) -> Vec<ChorusDAEvent> {
        std::mem::take(&mut self.out_events)
    }

    pub fn ingest(&mut self, envelope: ProposalEnvelope) -> Result<(), InvalidProposalHeader> {
        debug_assert!(envelope.header().slot == self.slot);

        self.authenticate(envelope.header())
            .ok_or(InvalidProposalHeader::Unauthenticated)?;
        Ok(())
    }

    // the proposal index of a proposer-signed header for this slot
    fn authenticate(&mut self, header: &ProposalHeader) -> Option<ProposalIndex> {
        if let Some(j) = self.authenticated_headers.get(header) {
            return Some(*j);
        }

        let j = self
            .epoch_handle
            .header_auth
            .authenticate(header, self.slot.get())?;
        self.authenticated_headers.insert(header.clone(), j);
        Some(j)
    }

    // no command has an effect on an authenticated header alone
    pub(crate) fn handle_command(&mut self, _command: ChorusDACommand) -> Vec<DAOutput> {
        vec![]
    }
}
