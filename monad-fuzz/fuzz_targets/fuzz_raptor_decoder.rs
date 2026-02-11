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

// Fuzz runner config:
//
// CORPUS_FILTER=*.raptor.bin
// TIMEOUT_QUICK=5m
//
// Environments:
//
// AFL_HANG_TMOUT=100
// AFL_EXIT_ON_TIME=300000
// AFL_INPUT_LEN_MAX=1500

use arbitrary::{Arbitrary, Unstructured};
use monad_raptor::ManagedDecoder;
use monad_raptorcast::{
    message::MAX_MESSAGE_SIZE,
    udp::{MAX_REDUNDANCY, MIN_CHUNK_LENGTH},
};

struct ManagedDecoderInput {
    num_source_symbols: usize,
    encoded_symbol_capacity: usize,
    symbol_len: usize,
    symbols: Vec<(usize, Vec<u8>)>,
}

impl ManagedDecoderInput {
    fn arbitrary_small(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let msg_len = u.int_in_range(1usize..=1024)?; // 1..1024
        let symbol_len = u.int_in_range(20..=100)?;
        let redundancy = u.int_in_range(1..=2)?;

        let num_source_symbols = msg_len.div_ceil(symbol_len);
        let encoded_symbol_capacity = redundancy * num_source_symbols;
        let mut symbols = vec![];
        let num_symbols = u.int_in_range(0..=(encoded_symbol_capacity * 2))?;

        for i in 0..num_symbols {
            let mut esi = i;
            if u.ratio(1, 10000)? {
                esi = u.int_in_range(0..=(num_symbols * 2))?;
            };
            let symbol = u.bytes(symbol_len)?.to_owned();
            symbols.push((esi, symbol));
        }

        // Fisher-Yates shuffle
        if !symbols.is_empty() {
            for i in 0..(symbols.len() - 1) {
                let swap_idx = i.wrapping_add(u.choose_index(symbols.len() - i)?);
                symbols.swap(i, swap_idx);
            }
        }

        let input = Self {
            num_source_symbols,
            encoded_symbol_capacity,
            symbol_len,
            symbols,
        };
        Ok(input)
    }

    fn arbitrary_normal(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let msg_len = u.int_in_range(1..=MAX_MESSAGE_SIZE)?; // 1..3MiB
        let symbol_len = u.int_in_range(MIN_CHUNK_LENGTH..=(MIN_CHUNK_LENGTH * 3 / 2))?; // 960..=1440
        let redundancy = if u.ratio(1, 3)? {
            u.int_in_range(1..=(MAX_REDUNDANCY.to_f32().ceil() as usize))?
        } else {
            u.int_in_range(1..=3)?
        };
        let num_source_symbols = msg_len.div_ceil(symbol_len);
        let encoded_symbol_capacity = redundancy * num_source_symbols;
        let mut symbols = vec![];
        let num_symbols = u.int_in_range(0..=(encoded_symbol_capacity * 2))?;

        for i in 0..num_symbols {
            let mut esi = i;
            if u.ratio(1, 10000)? {
                esi = u.int_in_range(0..=(num_symbols * 2))?;
            };
            let symbol = u.bytes(symbol_len)?.to_owned();
            symbols.push((esi, symbol));
        }

        // Fisher-Yates shuffle
        if !symbols.is_empty() {
            for i in 0..(symbols.len() - 1) {
                let swap_idx = i.wrapping_add(u.choose_index(symbols.len() - i)?);
                symbols.swap(i, swap_idx);
            }
        }

        let input = Self {
            num_source_symbols,
            encoded_symbol_capacity,
            symbol_len,
            symbols,
        };
        Ok(input)
    }
}

impl Arbitrary<'_> for ManagedDecoderInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        if u.ratio(1, 100)? {
            Self::arbitrary_normal(u)
        } else {
            Self::arbitrary_small(u)
        }
    }
}

fn main() {
    afl::fuzz!(|input: ManagedDecoderInput| {
        let Ok(mut decoder) = ManagedDecoder::new(
            input.num_source_symbols,
            input.encoded_symbol_capacity,
            input.symbol_len,
        ) else {
            return;
        };

        for (esi, symbol) in input.symbols {
            decoder.received_encoded_symbol(&symbol, esi);
            if decoder.try_decode() {
                assert!(decoder.reconstruct_source_data().is_some());
                break;
            }
        }
    });
}
