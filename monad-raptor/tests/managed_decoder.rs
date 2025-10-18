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

// Tests the managed Raptor decoder.

use monad_raptor::{Encoder, ManagedDecoder, SOURCE_SYMBOLS_MAX};
use rand::{prelude::SliceRandom, thread_rng, Rng, RngCore};

const SYMBOL_LEN: usize = 4;

fn test_single_decode(src: Vec<u8>) {
    let encoder: Encoder = Encoder::new(&src, SYMBOL_LEN).unwrap();

    let num_source_symbols = encoder.num_source_symbols();

    let mut decoder = ManagedDecoder::new(num_source_symbols, 0, SYMBOL_LEN).unwrap();

    let mut esis: Vec<usize> = (0..2 * num_source_symbols).collect();
    esis.shuffle(&mut thread_rng());

    for esi in &esis {
        let mut buf: Box<[u8]> = vec![0; SYMBOL_LEN].into_boxed_slice();
        encoder.encode_symbol(&mut buf, *esi);

        decoder.received_encoded_symbol(&buf, *esi);

        // We feed some encoded symbols back into the decoder twice to test the
        // redundant buffer handling paths.
        if rand::thread_rng().gen_ratio(1, 100) {
            decoder.received_encoded_symbol(&buf, *esi);
        }

        if decoder.try_decode() {
            break;
        }
    }

    if !decoder.decoding_done() {
        panic!("{:#?}", decoder);
    }

    let mut reconstructed_source_data = decoder
        .reconstruct_source_data()
        .expect("Error recovering source data");

    reconstructed_source_data.truncate(src.len());

    assert_eq!(*src, *reconstructed_source_data);
}

#[test]
fn test_managed_decoder() {
    let max_bytes = if cfg!(debug_assertions) { 128 } else { 2048 };

    for bytes in 0..=max_bytes {
        println!("Testing bytes = {}", bytes);

        let mut src = vec![0u8; bytes];

        thread_rng().fill_bytes(&mut src);

        test_single_decode(src);
    }
}

#[test]
#[should_panic]
fn test_invalid_symbol_len() {
    let src = vec![0u8; 32];

    let encoder: Encoder = Encoder::new(&src, SYMBOL_LEN).unwrap();
    let num_source_symbols = encoder.num_source_symbols();
    let mut decoder = ManagedDecoder::new(num_source_symbols, 0, SYMBOL_LEN).unwrap();

    let buf = vec![0; SYMBOL_LEN + 1];

    decoder.received_encoded_symbol(buf.as_slice(), 0);
}

#[test]
fn test_oversized_symbol() {
    let num_symbols = SOURCE_SYMBOLS_MAX + 1;
    let decoder = ManagedDecoder::new(num_symbols, num_symbols * 2, SYMBOL_LEN);
    assert!(decoder.is_err());
}
