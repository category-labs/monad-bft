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

// Tests the Raptor encoder and decoder against each other.

use std::{cmp::Ordering, iter, slice};

use monad_raptor::{
    r10::{
        nonsystematic::decoder::{BufferId, Decoder},
        SOURCE_SYMBOLS_MIN,
    },
    xor_eq::xor_eq,
    Encoder,
};
use rand::{prelude::SliceRandom, thread_rng, Rng, RngCore};

const SYMBOL_LEN: usize = 4;

struct BufferSet {
    temp_buffers: Vec<Box<[u8]>>,
    rx_buffers: Vec<Box<[u8]>>,
}

impl BufferSet {
    fn new(num_temp_buffers: usize, symbol_len: usize) -> BufferSet {
        let temp_buffers: Vec<Box<[u8]>> =
            iter::repeat_n(vec![0; symbol_len].into_boxed_slice(), num_temp_buffers).collect();

        let rx_buffers: Vec<Box<[u8]>> = Vec::new();

        BufferSet {
            temp_buffers,
            rx_buffers,
        }
    }

    fn buffer(&self, buffer_id: BufferId) -> &[u8] {
        match buffer_id {
            BufferId::TempBuffer { index } => &self.temp_buffers[index],
            BufferId::ReceiveBuffer { index } => &self.rx_buffers[index],
        }
    }

    fn xor_temp_buffers(&mut self, a: usize, b: usize) {
        // Split the borrow to be able to get a mutable reference and an immutable
        // reference to different elements of the slice without using unsafe code.
        // (This essentially emulates std::slice::get_many_mut(), without using
        // unsafe code.)
        match a.cmp(&b) {
            Ordering::Less => {
                let (first, second) = self.temp_buffers.split_at_mut(b);

                xor_eq(&mut first[a], slice::from_ref(&&*second[0]));
            }
            Ordering::Greater => {
                let (first, second) = self.temp_buffers.split_at_mut(a);

                xor_eq(&mut second[0], slice::from_ref(&&*first[b]));
            }
            Ordering::Equal => panic!(),
        }
    }

    fn xor_rx_buffers(&mut self, a: usize, b: usize) {
        // Split the borrow to be able to get a mutable reference and an immutable
        // reference to different elements of the slice without using unsafe code.
        // (This essentially emulates std::slice::get_many_mut(), without using
        // unsafe code.)
        match a.cmp(&b) {
            Ordering::Less => {
                let (first, second) = self.rx_buffers.split_at_mut(b);

                xor_eq(&mut first[a], slice::from_ref(&&*second[0]));
            }
            Ordering::Greater => {
                let (first, second) = self.rx_buffers.split_at_mut(a);

                xor_eq(&mut second[0], slice::from_ref(&&*first[b]));
            }
            Ordering::Equal => panic!(),
        }
    }

    fn xor_buffers(&mut self, a: BufferId, b: BufferId) {
        match a {
            BufferId::TempBuffer { index: a_index } => match b {
                BufferId::TempBuffer { index: b_index } => {
                    self.xor_temp_buffers(a_index, b_index);
                }
                BufferId::ReceiveBuffer { index: b_index } => {
                    xor_eq(
                        &mut self.temp_buffers[a_index],
                        slice::from_ref(&&*self.rx_buffers[b_index]),
                    );
                }
            },
            BufferId::ReceiveBuffer { index: a_index } => match b {
                BufferId::TempBuffer { index: b_index } => {
                    xor_eq(
                        &mut self.rx_buffers[a_index],
                        slice::from_ref(&&*self.temp_buffers[b_index]),
                    );
                }
                BufferId::ReceiveBuffer { index: b_index } => {
                    self.xor_rx_buffers(a_index, b_index);
                }
            },
        }
    }
}

fn test_single_decode(mut src: Vec<u8>) {
    let encoder: Encoder = Encoder::new(&src, SYMBOL_LEN).unwrap();

    let num_source_symbols = encoder.num_source_symbols();

    let mut decoder = Decoder::new(num_source_symbols).unwrap();

    let mut buffer_set = BufferSet::new(decoder.num_temp_buffers_required(), SYMBOL_LEN);

    let mut esis: Vec<usize> = (0..2 * num_source_symbols).collect();
    esis.shuffle(&mut thread_rng());

    for esi in &esis {
        let mut buf: Box<[u8; SYMBOL_LEN]> = Box::new([0; SYMBOL_LEN]);
        encoder.encode_symbol(&mut buf[..], *esi);

        // We feed some encoded symbols back into the decoder twice to test the
        // Redundant buffer handling paths.
        if rand::thread_rng().gen_ratio(1, 100) {
            let buf = buf.clone();

            buffer_set.rx_buffers.push(buf);
            decoder.received_encoded_symbol(*esi, |a, b| buffer_set.xor_buffers(a, b));
        }

        buffer_set.rx_buffers.push(buf);
        decoder.received_encoded_symbol(*esi, |a, b| buffer_set.xor_buffers(a, b));

        if decoder.try_decode(num_source_symbols + (num_source_symbols / 4), |a, b| {
            buffer_set.xor_buffers(a, b)
        }) {
            break;
        }
    }

    if !decoder.decoding_done() {
        panic!("{:#?}", decoder);
    }

    // Pad `src` to an integer multiple >= SOURCE_SYMBOLS_MIN of SYMBOL_LEN bytes to
    // simplify the data consistency comparisons below.
    {
        let symbols = src.len().div_ceil(SYMBOL_LEN).max(SOURCE_SYMBOLS_MIN);

        let len = symbols * SYMBOL_LEN;

        if src.len() != len {
            src.resize(len, 0u8);
        }
    }

    for i in 0..num_source_symbols {
        match decoder.source_symbol_to_buffer_id(i) {
            None => panic!("Source symbol {} was not recovered!", i),
            Some(buffer_id) => {
                assert_eq!(
                    src[i * SYMBOL_LEN..(i + 1) * SYMBOL_LEN],
                    *buffer_set.buffer(buffer_id)
                );
            }
        }
    }
}

#[test]
fn test_encode_decode() {
    let max_bytes = if cfg!(debug_assertions) { 128 } else { 2048 };

    for bytes in 0..=max_bytes {
        println!("Testing bytes = {}", bytes);

        let mut src = vec![0u8; bytes];

        thread_rng().fill_bytes(&mut src);

        test_single_decode(src);
    }
}
