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

use super::{chorus::env::EncodingScheme, types::ProposalHeader};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidProposalHeader {
    SlotOutOfRange,
    // not signed by a proposer of the slot
    Unauthenticated,
    // the scheme fits no packet layout
    NoPacketLayout,
}

pub trait DAProposalHeader: monad_mcp_chorus::spec::ProposalHeader {
    // the encoding scheme determines the packet layout, the chunk
    // assignment and the symbol code.
    fn encoding_scheme(&self) -> EncodingScheme;
}

impl DAProposalHeader for ProposalHeader {
    fn encoding_scheme(&self) -> EncodingScheme {
        self.scheme
    }
}
