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

use bytes::{Bytes, BytesMut};

use super::super::assignment::ChunkId;

#[derive(Clone, Copy)]
pub(crate) struct ReedSolomon {
    num_chunks: usize,
    num_source_chunks: usize,
}

impl ReedSolomon {
    // the caller must ensure 0 < num_source_chunks <= num_chunks
    pub(crate) fn new(num_chunks: usize, num_source_chunks: usize) -> Self {
        Self {
            num_chunks,
            num_source_chunks,
        }
    }

    fn num_recovery_chunks(&self) -> usize {
        self.num_chunks - self.num_source_chunks
    }

    // one symbol per chunk, in chunk id order. the caller must ensure an
    // even symbol_len
    pub(crate) fn encode(&self, message: &[u8], symbol_len: usize) -> Vec<Bytes> {
        let mut symbols = source_symbols(message, self.num_source_chunks, symbol_len);
        let num_recovery_chunks = self.num_recovery_chunks();
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

    // the message of msg_len bytes, once the symbols are any
    // num_source_chunks of the code. the caller must pass each chunk id
    // once.
    pub(crate) fn decode<'a>(
        &self,
        symbols: impl Iterator<Item = (ChunkId, &'a Bytes)>,
        msg_len: usize,
    ) -> Option<Bytes> {
        use bytes::BufMut as _;

        // by source position
        let mut sources: Vec<Option<Bytes>> = vec![None; self.num_source_chunks];
        // by recovery position
        let mut recoveries = Vec::new();
        let mut received = 0;
        for (chunk_id, symbol) in symbols {
            let chunk_id = usize::from(chunk_id);
            if chunk_id < self.num_source_chunks {
                sources[chunk_id] = Some(symbol.clone());
            } else {
                recoveries.push((chunk_id - self.num_source_chunks, symbol));
            }
            received += 1;
        }
        if received < self.num_source_chunks {
            return None;
        }
        if sources.iter().any(Option::is_none) {
            self.restore_sources(&mut sources, recoveries)?;
        }

        let mut message = BytesMut::new();
        for source in &sources {
            message.put_slice(source.as_ref()?);
        }
        message.truncate(msg_len);
        Some(message.freeze())
    }

    fn restore_sources(
        &self,
        sources: &mut [Option<Bytes>],
        recoveries: Vec<(usize, &Bytes)>,
    ) -> Option<()> {
        let mut present = Vec::new();
        for (position, source) in sources.iter().enumerate() {
            if let Some(source) = source {
                present.push((position, source));
            }
        }
        let restored = reed_solomon_simd::decode(
            self.num_source_chunks,
            self.num_recovery_chunks(),
            present,
            recoveries,
        )
        .ok()?;
        for (position, symbol) in restored {
            sources[position] = Some(Bytes::from(symbol));
        }
        Some(())
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

#[cfg(test)]
mod tests {
    use super::*;

    const MESSAGE: [u8; 9] = [1, 2, 3, 4, 5, 6, 7, 8, 9];

    fn symbols() -> Vec<Bytes> {
        ReedSolomon::new(7, 3).encode(&MESSAGE, 4)
    }

    // the symbols of the listed chunks, as a decoder receives them
    fn received<'a>(
        symbols: &'a [Bytes],
        chunk_ids: &'a [u16],
    ) -> impl Iterator<Item = (ChunkId, &'a Bytes)> {
        chunk_ids.iter().map(|chunk_id| {
            (
                ChunkId::unchecked(*chunk_id),
                &symbols[usize::from(*chunk_id)],
            )
        })
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
        let code = ReedSolomon::new(7, 3);
        for kept in [[3, 4, 5, 6], [1, 2, 4, 5], [0, 1, 3, 6], [0, 1, 2, 3]] {
            let decoded = code.decode(received(&symbols, &kept), MESSAGE.len());
            assert_eq!(decoded, Some(Bytes::from_static(&MESSAGE)));
        }
    }

    #[test]
    fn fewer_than_k_symbols_do_not_decode() {
        let symbols = symbols();
        let code = ReedSolomon::new(7, 3);
        let decoded = code.decode(received(&symbols, &[0, 5]), MESSAGE.len());
        assert_eq!(decoded, None);
        let decoded = code.decode(received(&symbols, &[0, 4, 5]), MESSAGE.len());
        assert_eq!(decoded, Some(Bytes::from_static(&MESSAGE)));
    }

    #[test]
    fn a_code_without_recovery_needs_every_source() {
        let code = ReedSolomon::new(2, 2);
        let symbols = code.encode(&MESSAGE[..8], 4);
        assert_eq!(symbols.len(), 2);
        assert_eq!(code.decode(received(&symbols, &[1]), 8), None);
        let decoded = code.decode(received(&symbols, &[0, 1]), 8);
        assert_eq!(decoded, Some(Bytes::from_static(&MESSAGE[..8])));
    }
}
