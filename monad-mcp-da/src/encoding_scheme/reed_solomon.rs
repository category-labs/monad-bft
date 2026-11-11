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
    symbol_len: usize,
    msg_len: usize,
}

impl ReedSolomon {
    // the arguments must pass the asserted preconditions
    pub(crate) fn new(
        num_chunks: usize,
        num_source_chunks: usize,
        symbol_len: usize,
        msg_len: usize,
    ) -> Self {
        assert!(num_source_chunks > 0 && num_source_chunks <= num_chunks);
        assert!(symbol_len > 0 && symbol_len.is_multiple_of(2));
        assert!(msg_len <= num_source_chunks * symbol_len);
        Self {
            num_chunks,
            num_source_chunks,
            symbol_len,
            msg_len,
        }
    }

    pub(crate) fn num_chunks(&self) -> usize {
        self.num_chunks
    }

    pub(crate) fn num_source_chunks(&self) -> usize {
        self.num_source_chunks
    }

    pub(crate) fn symbol_len(&self) -> usize {
        self.symbol_len
    }

    fn num_recovery_chunks(&self) -> usize {
        self.num_chunks - self.num_source_chunks
    }

    // one symbol per chunk, in chunk id order. the caller must pass a
    // message of msg_len bytes
    pub(crate) fn encode(&self, message: &Bytes) -> Vec<Bytes> {
        debug_assert_eq!(message.len(), self.msg_len);
        let mut symbols = source_symbols(message, self.num_source_chunks, self.symbol_len);
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

    // the caller must pass at least K symbols, each chunk id once
    pub(crate) fn decode<'a>(
        &self,
        symbols: impl ExactSizeIterator<Item = (ChunkId, &'a Bytes)>,
    ) -> Bytes {
        debug_assert!(symbols.len() >= self.num_source_chunks);

        use bytes::BufMut as _;

        // by source position
        let mut sources: Vec<Option<Bytes>> = vec![None; self.num_source_chunks];
        // by recovery position
        let mut recoveries = Vec::with_capacity(symbols.len());
        for (chunk_id, symbol) in symbols {
            let chunk_id = usize::from(chunk_id);
            if chunk_id < self.num_source_chunks {
                sources[chunk_id] = Some(symbol.clone());
            } else {
                recoveries.push((chunk_id - self.num_source_chunks, symbol));
            }
        }
        if sources.iter().any(Option::is_none) {
            self.restore_sources(&mut sources, recoveries);
        }

        let mut message = BytesMut::with_capacity(self.num_source_chunks * self.symbol_len);
        for source in &sources {
            let source = source.as_ref().expect("received or restored");
            message.put_slice(source);
        }
        message.truncate(self.msg_len);
        message.freeze()
    }

    fn restore_sources(&self, sources: &mut [Option<Bytes>], recoveries: Vec<(usize, &Bytes)>) {
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
        );

        let restored = restored.expect("caller ensures passing enough well-formed symbols");
        for (position, symbol) in restored {
            sources[position] = Some(Bytes::from(symbol));
        }
    }
}

// the message zero padded to whole symbols. A symbol the message fills
// is a view into it, not a copy; only a partial last symbol is copied
// and padded.
fn source_symbols(message: &Bytes, num_source_chunks: usize, symbol_len: usize) -> Vec<Bytes> {
    let mut sources = Vec::with_capacity(num_source_chunks);
    for i in 0..num_source_chunks {
        let start = (i * symbol_len).min(message.len());
        let end = ((i + 1) * symbol_len).min(message.len());
        if end - start == symbol_len {
            sources.push(message.slice(start..end));
            continue;
        }
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

    fn code() -> ReedSolomon {
        ReedSolomon::new(7, 3, 4, MESSAGE.len())
    }

    fn symbols() -> Vec<Bytes> {
        code().encode(&Bytes::from_static(&MESSAGE))
    }

    // the symbols of the listed chunks, as a decoder receives them
    fn received<'a>(
        symbols: &'a [Bytes],
        chunk_ids: &'a [u16],
    ) -> impl ExactSizeIterator<Item = (ChunkId, &'a Bytes)> {
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
    fn whole_sources_are_views_into_the_message() {
        let message = Bytes::from_static(&MESSAGE);
        let symbols = code().encode(&message);
        assert_eq!(symbols[0].as_ptr(), message.as_ptr());
        assert_eq!(symbols[1].as_ptr(), message[4..].as_ptr());
        // the padded last source is a copy
        assert_ne!(symbols[2].as_ptr(), message[8..].as_ptr());
    }

    #[test]
    fn any_k_symbols_decode() {
        let symbols = symbols();
        for kept in [[3, 4, 5, 6], [1, 2, 4, 5], [0, 1, 3, 6], [0, 1, 2, 3]] {
            let decoded = code().decode(received(&symbols, &kept));
            assert_eq!(decoded, Bytes::from_static(&MESSAGE));
        }
    }

    #[test]
    fn a_code_without_recovery_decodes_from_every_source() {
        let code = ReedSolomon::new(2, 2, 4, 8);
        let symbols = code.encode(&Bytes::from_static(&MESSAGE[..8]));
        assert_eq!(symbols.len(), 2);
        let decoded = code.decode(received(&symbols, &[0, 1]));
        assert_eq!(decoded, Bytes::from_static(&MESSAGE[..8]));
    }
}
