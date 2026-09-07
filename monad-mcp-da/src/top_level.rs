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

use super::{env, env::chorus, wire};

mod assignment;
pub(crate) mod chunk;
mod chunk_tree;
mod egress;
mod election;
pub(crate) mod encoding_scheme;
mod header;
mod instance_rc;
mod proposer_rc;
mod runtime;
mod slot_rc;
pub(crate) mod types;
mod util;

#[cfg(test)]
pub(crate) mod test_util;

pub use chunk::{Chunk, ChunkRequest, ProposalEnvelope};
pub use egress::Dissemination;
pub use encoding_scheme::d25;
pub use header::{InvalidProposalHeader, header_auth};
pub use runtime::{ChunkRecoveryRequest, DAConfig, DAOutput, DARuntime, EpochHandle};
pub use wire::{MalformedPacket, SEGMENT_LEN, read_chunk, write_chunk};
