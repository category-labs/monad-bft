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

use bytes::{BufMut as _, Bytes, BytesMut};

use super::{super::assignment::ChunkId, SymbolDecoder, SymbolEncoder};

// the stub symbol code: a repetition code with a one-symbol decoding
// overhead. The overhead keeps the instance's readiness and the
// decoder's success distinct, as they are under a rateless code.
pub(crate) struct StubSymbolEncoder {
    num_chunks: usize,
    num_source_chunks: usize,
    symbol_len: usize,
}

impl StubSymbolEncoder {
    // the caller must ensure num_source_chunks > 0
    pub(crate) fn new(num_chunks: usize, num_source_chunks: usize, symbol_len: usize) -> Self {
        Self {
            num_chunks,
            num_source_chunks,
            symbol_len,
        }
    }
}

impl SymbolEncoder for StubSymbolEncoder {
    // symbol i repeats source symbol i mod k. the message is zero
    // padded to k whole symbols
    fn encode(&self, message: &[u8]) -> Vec<Bytes> {
        let mut sources = Vec::with_capacity(self.num_source_chunks);
        for i in 0..self.num_source_chunks {
            let start = (i * self.symbol_len).min(message.len());
            let end = ((i + 1) * self.symbol_len).min(message.len());
            let mut symbol = BytesMut::zeroed(self.symbol_len);
            symbol[..end - start].copy_from_slice(&message[start..end]);
            sources.push(symbol.freeze());
        }

        let mut symbols = Vec::with_capacity(self.num_chunks);
        for chunk_id in 0..self.num_chunks {
            symbols.push(sources[chunk_id % self.num_source_chunks].clone());
        }
        symbols
    }
}

pub(crate) struct StubSymbolDecoder {
    // by source position
    sources: Vec<Option<Bytes>>,
    received: usize,
    msg_len: usize,
}

impl StubSymbolDecoder {
    // the caller must ensure num_source_chunks > 0
    pub(crate) fn new(num_source_chunks: usize, msg_len: usize) -> Self {
        Self {
            sources: vec![None; num_source_chunks],
            received: 0,
            msg_len,
        }
    }
}

impl SymbolDecoder for StubSymbolDecoder {
    fn ingest(&mut self, chunk_id: ChunkId, symbol: &Bytes) {
        let position = usize::from(chunk_id) % self.sources.len();
        self.sources[position] = Some(symbol.clone());
        self.received += 1;
    }

    fn try_decode(&mut self) -> Option<Bytes> {
        if self.received <= self.sources.len() {
            return None;
        }
        let mut message = BytesMut::new();
        for source in &self.sources {
            message.put_slice(source.as_ref()?);
        }
        message.truncate(self.msg_len);
        Some(message.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbols_repeat_the_padded_sources() {
        let symbols = StubSymbolEncoder::new(5, 2, 4).encode(&[1, 2, 3, 4, 5]);
        assert_eq!(symbols[0], Bytes::from_static(&[1, 2, 3, 4]));
        assert_eq!(symbols[1], Bytes::from_static(&[5, 0, 0, 0]));
        assert_eq!(symbols[2], symbols[0]);
        assert_eq!(symbols[3], symbols[1]);
        assert_eq!(symbols[4], symbols[0]);
    }

    #[test]
    fn decoding_needs_every_source_position_and_one_extra_symbol() {
        let symbols = StubSymbolEncoder::new(5, 2, 4).encode(&[1, 2, 3, 4, 5]);
        let mut decoder = StubSymbolDecoder::new(2, 5);

        // three symbols, but no odd position
        decoder.ingest(ChunkId::unchecked(0), &symbols[0]);
        decoder.ingest(ChunkId::unchecked(2), &symbols[2]);
        decoder.ingest(ChunkId::unchecked(4), &symbols[4]);
        assert_eq!(decoder.try_decode(), None);

        decoder.ingest(ChunkId::unchecked(3), &symbols[3]);
        assert_eq!(
            decoder.try_decode(),
            Some(Bytes::from_static(&[1, 2, 3, 4, 5]))
        );
    }
}
