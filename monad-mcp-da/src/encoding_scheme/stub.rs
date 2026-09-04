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

use bytes::Bytes;

use super::{
    super::{assignment::ChunkId, layout::PacketLayout},
    SymbolDecoder, SymbolEncoder,
};

// the stub symbol code: every symbol is the whole message
pub(crate) struct StubSymbolEncoder {
    num_chunks: usize,
}

impl StubSymbolEncoder {
    pub(crate) fn new(num_chunks: usize) -> Self {
        Self { num_chunks }
    }
}

impl SymbolEncoder for StubSymbolEncoder {
    fn encode(&self, message: &[u8]) -> Vec<Bytes> {
        vec![Bytes::copy_from_slice(message); self.num_chunks]
    }
}

// decodes once more than num_source_chunks symbols arrived
pub(crate) struct StubSymbolDecoder {
    threshold: usize,
    received: usize,
    message: Option<Bytes>,
}

impl StubSymbolDecoder {
    pub(crate) fn new(layout: PacketLayout) -> Self {
        Self {
            threshold: layout.num_source_chunks() + 1,
            received: 0,
            message: None,
        }
    }
}

impl SymbolDecoder for StubSymbolDecoder {
    fn ingest(&mut self, _chunk_id: ChunkId, symbol: &Bytes) {
        self.received += 1;
        self.message = Some(symbol.clone());
    }

    fn try_decode(&mut self) -> Option<Bytes> {
        if self.received < self.threshold {
            return None;
        }
        self.message.clone()
    }
}
