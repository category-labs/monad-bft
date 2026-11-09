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

mod reed_solomon;

use bytes::Bytes;

pub(crate) use self::reed_solomon::{RsSymbolDecoder, RsSymbolEncoder};
use super::super::assignment::ChunkId;

pub(crate) trait SymbolEncoder: Send {
    // one symbol per chunk, in chunk id order
    fn encode(&self, message: &[u8]) -> Vec<Bytes>;
}

// the decoding state of one proposal.
pub(crate) trait SymbolDecoder: Send {
    fn ingest(&mut self, chunk_id: ChunkId, symbol: &Bytes);

    // the message, once enough symbols arrived
    fn try_decode(&mut self) -> Option<Bytes>;
}
