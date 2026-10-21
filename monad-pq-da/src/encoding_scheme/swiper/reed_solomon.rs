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

//! The Reed-Solomon symbol code: chunks 0..k carry the k source
//! symbols, the rest recovery symbols; any k symbols decode.

use std::collections::BTreeMap;

use bytes::{BufMut as _, Bytes, BytesMut};

use super::super::{super::assignment::ChunkId, SymbolDecoder, SymbolEncoder};

pub(crate) struct RsSymbolEncoder {
    num_chunks: usize,
    num_source_chunks: usize,
    symbol_len: usize,
}

impl RsSymbolEncoder {
    // the caller must ensure 0 < num_source_chunks <= num_chunks and an
    // even symbol_len
    pub(crate) fn new(num_chunks: usize, num_source_chunks: usize, symbol_len: usize) -> Self {
        Self {
            num_chunks,
            num_source_chunks,
            symbol_len,
        }
    }
}

// the message zero padded to whole symbols
fn source_symbols(message: &[u8], num_source_chunks: usize, symbol_len: usize) -> Vec<Bytes> {
    let mut sources = Vec::with_capacity(num_source_chunks);
    for i in 0..num_source_chunks {
        let start = (i * symbol_len).min(message.len());
        let end = ((i + 1) * symbol_len).min(message.len());
        let mut symbol = BytesMut::zeroed(symbol_len);
        symbol[..end - start].copy_from_slice(&message[start..end]);
        sources.push(symbol.freeze());
    }
    sources
}

impl SymbolEncoder for RsSymbolEncoder {
    fn encode(&self, message: &[u8]) -> Vec<Bytes> {
        let mut symbols = source_symbols(message, self.num_source_chunks, self.symbol_len);
        let num_recovery_chunks = self.num_chunks - self.num_source_chunks;
        if num_recovery_chunks == 0 {
            return symbols;
        }
        let recovery =
            reed_solomon_simd::encode(self.num_source_chunks, num_recovery_chunks, &symbols)
                .expect("supported shard counts and an even symbol length");
        for symbol in recovery {
            symbols.push(Bytes::from(symbol));
        }
        symbols
    }
}

pub(crate) struct RsSymbolDecoder {
    num_recovery_chunks: usize,
    msg_len: usize,
    // by source position
    sources: Vec<Option<Bytes>>,
    // by recovery position
    recoveries: BTreeMap<usize, Bytes>,
    received: usize,
}

impl RsSymbolDecoder {
    // the caller must ensure 0 < num_source_chunks <= num_chunks
    pub(crate) fn new(num_chunks: usize, num_source_chunks: usize, msg_len: usize) -> Self {
        Self {
            num_recovery_chunks: num_chunks - num_source_chunks,
            msg_len,
            sources: vec![None; num_source_chunks],
            recoveries: BTreeMap::new(),
            received: 0,
        }
    }

    fn num_source_chunks(&self) -> usize {
        self.sources.len()
    }

    fn restore_sources(&mut self) -> Option<()> {
        let mut present = Vec::new();
        for (position, source) in self.sources.iter().enumerate() {
            if let Some(source) = source {
                present.push((position, source));
            }
        }
        let recoveries = self
            .recoveries
            .iter()
            .map(|(position, symbol)| (*position, symbol));
        let restored = reed_solomon_simd::decode(
            self.num_source_chunks(),
            self.num_recovery_chunks,
            present,
            recoveries,
        )
        .ok()?;
        for (position, symbol) in restored {
            self.sources[position] = Some(Bytes::from(symbol));
        }
        Some(())
    }
}

impl SymbolDecoder for RsSymbolDecoder {
    fn ingest(&mut self, chunk_id: ChunkId, symbol: &Bytes) {
        let chunk_id = usize::from(chunk_id);
        let fresh = if chunk_id < self.num_source_chunks() {
            let slot = &mut self.sources[chunk_id];
            let fresh = slot.is_none();
            *slot = Some(symbol.clone());
            fresh
        } else {
            let position = chunk_id - self.num_source_chunks();
            self.recoveries.insert(position, symbol.clone()).is_none()
        };
        if fresh {
            self.received += 1;
        }
    }

    fn try_decode(&mut self) -> Option<Bytes> {
        if self.received < self.num_source_chunks() {
            return None;
        }
        if self.sources.iter().any(Option::is_none) {
            self.restore_sources()?;
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

    const MESSAGE: [u8; 9] = [1, 2, 3, 4, 5, 6, 7, 8, 9];

    fn symbols() -> Vec<Bytes> {
        RsSymbolEncoder::new(7, 3, 4).encode(&MESSAGE)
    }

    #[test]
    fn the_first_symbols_are_the_padded_sources() {
        let symbols = symbols();
        assert_eq!(symbols.len(), 7);
        assert_eq!(symbols[0], Bytes::from_static(&[1, 2, 3, 4]));
        assert_eq!(symbols[1], Bytes::from_static(&[5, 6, 7, 8]));
        assert_eq!(symbols[2], Bytes::from_static(&[9, 0, 0, 0]));
        for symbol in &symbols[3..] {
            assert_eq!(symbol.len(), 4);
        }
    }

    #[test]
    fn any_k_symbols_decode() {
        let symbols = symbols();
        for skip in [[0, 1, 2], [0, 3, 6], [2, 4, 5], [4, 5, 6]] {
            let mut decoder = RsSymbolDecoder::new(7, 3, MESSAGE.len());
            for (chunk_id, symbol) in symbols.iter().enumerate() {
                if skip.contains(&chunk_id) {
                    continue;
                }
                decoder.ingest(ChunkId::unchecked(chunk_id as u16), symbol);
            }
            assert_eq!(decoder.try_decode(), Some(Bytes::from_static(&MESSAGE)));
        }
    }

    #[test]
    fn fewer_than_k_distinct_symbols_do_not_decode() {
        let symbols = symbols();
        let mut decoder = RsSymbolDecoder::new(7, 3, MESSAGE.len());
        decoder.ingest(ChunkId::unchecked(0), &symbols[0]);
        decoder.ingest(ChunkId::unchecked(5), &symbols[5]);
        decoder.ingest(ChunkId::unchecked(5), &symbols[5]);
        assert_eq!(decoder.try_decode(), None);
        decoder.ingest(ChunkId::unchecked(4), &symbols[4]);
        assert_eq!(decoder.try_decode(), Some(Bytes::from_static(&MESSAGE)));
    }

    #[test]
    fn a_code_without_recovery_needs_every_source() {
        let symbols = RsSymbolEncoder::new(2, 2, 4).encode(&MESSAGE[..8]);
        assert_eq!(symbols.len(), 2);
        let mut decoder = RsSymbolDecoder::new(2, 2, 8);
        decoder.ingest(ChunkId::unchecked(1), &symbols[1]);
        assert_eq!(decoder.try_decode(), None);
        decoder.ingest(ChunkId::unchecked(0), &symbols[0]);
        assert_eq!(
            decoder.try_decode(),
            Some(Bytes::from_static(&MESSAGE[..8]))
        );
    }
}
