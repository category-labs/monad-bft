use std::{cmp::Ordering, io::Error, iter};

use crate::r10::nonsystematic::decoder::{BufferId, Decoder};

// We switch from doing peeling only to performing inactivation decoding when
// num_received_encoded_symbols >= (MULTIPLIER * num_source_symbols) >> SHIFT .
const INACTIVATION_SYMBOL_THRESHOLD_MULTIPLIER: usize = 384;
const INACTIVATION_SYMBOL_THRESHOLD_SHIFT: usize = 8;

#[derive(Debug)]
struct BufferSet {
    num_temp_buffers: usize,
    buffers: Vec<Box<[u8]>>,
}

impl BufferSet {
    pub fn new(num_temp_buffers: usize, symbol_len: usize) -> BufferSet {
        let buffers: Vec<Box<[u8]>> = iter::repeat(vec![0; symbol_len].into_boxed_slice())
            .take(num_temp_buffers)
            .collect();

        BufferSet {
            num_temp_buffers,
            buffers,
        }
    }

    pub fn push_buffer(&mut self, buf: Box<[u8]>) {
        self.buffers.push(buf);
    }

    fn buffer_index(&self, buffer_id: BufferId) -> usize {
        match buffer_id {
            BufferId::TempBuffer { index } => index,
            BufferId::ReceiveBuffer { index } => self.num_temp_buffers + index,
        }
    }

    pub fn xor_buffers(&mut self, a: BufferId, b: BufferId) {
        let a_index = self.buffer_index(a);
        let b_index = self.buffer_index(b);

        // Split the borrow to be able to get a mutable reference and an immutable
        // reference to different elements of the slice without using unsafe code.
        // (This essentially emulates std::slice::get_many_mut().)
        let (dst, src) = match a_index.cmp(&b_index) {
            Ordering::Less => {
                let (first, second) = self.buffers.split_at_mut(b_index);
                (&mut first[a_index], &second[0])
            }
            Ordering::Greater => {
                let (first, second) = self.buffers.split_at_mut(a_index);
                (&mut second[0], &first[b_index])
            }
            Ordering::Equal => panic!("xor_buffers: Was asked to XOR buffer with itself"),
        };

        let len = dst.len();

        assert_eq!(len, src.len());

        for i in 0..len {
            dst[i] ^= src[i];
        }
    }

    pub fn buffer(&self, buffer_id: BufferId) -> &[u8] {
        &self.buffers[self.buffer_index(buffer_id)]
    }
}

#[derive(Debug)]
pub struct ManagedDecoder {
    num_source_symbols: usize,
    symbol_len: usize,
    decoder: Decoder,
    buffer_set: BufferSet,
}

impl ManagedDecoder {
    pub fn new(num_source_symbols: usize, symbol_len: usize) -> Result<ManagedDecoder, Error> {
        let decoder = Decoder::new(num_source_symbols)?;

        let buffer_set = BufferSet::new(decoder.num_temp_buffers_required(), symbol_len);

        Ok(ManagedDecoder {
            num_source_symbols,
            symbol_len,
            decoder,
            buffer_set,
        })
    }

    // TODO: Explore accepting Bytes as data, making rx_buffers a vector of enums
    // designating either an owned Box<[u8]> or an un-owned Bytes, and converting
    // un-owned to owned buffers whenever they are targeted for XORing.
    pub fn received_encoded_symbol(&mut self, data: &[u8], encoding_symbol_id: usize) {
        let buf: Box<[u8]> = data.into();

        self.buffer_set.push_buffer(buf);

        self.decoder
            .received_encoded_symbol(encoding_symbol_id, |a, b| self.buffer_set.xor_buffers(a, b));
    }

    pub fn try_decode(&mut self) -> bool {
        let inactivation_symbol_threshold = (INACTIVATION_SYMBOL_THRESHOLD_MULTIPLIER
            * self.num_source_symbols)
            >> INACTIVATION_SYMBOL_THRESHOLD_SHIFT;

        self.decoder
            .try_decode(inactivation_symbol_threshold, |a, b| {
                self.buffer_set.xor_buffers(a, b)
            })
    }

    pub fn decoding_done(&self) -> bool {
        self.decoder.decoding_done()
    }

    pub fn reconstruct_source_data(&self) -> Option<Vec<u8>> {
        let mut data = Vec::with_capacity(self.num_source_symbols * self.symbol_len);

        for i in 0..self.num_source_symbols {
            match self.decoder.source_symbol_to_buffer_id(i) {
                None => {
                    return None;
                }
                Some(buffer_id) => {
                    data.extend_from_slice(self.buffer_set.buffer(buffer_id));
                }
            }
        }

        Some(data)
    }
}
