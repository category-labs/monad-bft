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

// Decoder for non-systematic R10 code.

mod buffer;
mod buffer_id;
mod buffer_state;
mod buffer_weight_map;
mod check;
mod decode;
mod decode_finished;
mod decode_inactivate;
mod decode_inactive_gaussian;
mod decode_peel;
mod decode_reactivate;
mod init;
mod intermediate_symbol;
mod managed_decoder;
mod receive_symbol;

pub use buffer::Buffer;
pub use buffer_id::BufferId;
pub use buffer_state::BufferState;
pub use buffer_weight_map::BufferWeightMap;
pub use intermediate_symbol::IntermediateSymbol;
pub use managed_decoder::ManagedDecoder;

use crate::r10::CodeParameters;

#[derive(Debug)]
pub struct Decoder {
    params: CodeParameters,

    // For each buffer, a list of intermediate symbols XORd into that buffer, plus some other
    // bookkeeping information.
    buffer_state: Vec<Buffer>,

    // For each intermediate symbol, a list of buffers that that intermediate symbol is XORd
    // into, plus some information about whether this intermediate symbol has been inactivated
    // or recovered.
    intermediate_symbol_state: Vec<IntermediateSymbol>,

    // Usable and Active buffers ordered according to their Active/Used intermediate symbol weight.
    buffers_active_usable: BufferWeightMap,

    // Inactivated buffers ordered according to their total intermediate symbol weight.
    buffers_inactivated: BufferWeightMap,

    // The number of buffers we have that are in the Redundant state.
    num_redundant_buffers: u16,

    // Number of source symbols recovered.  We are done decoding if this is equal to
    // params.num_source_symbols().
    num_source_symbols_paired: usize,
}

impl Decoder {
    fn num_redundant_intermediate_symbols(&self) -> usize {
        self.params.num_ldpc_symbols() + self.params.num_half_symbols()
    }

    pub fn num_temp_buffers_required(&self) -> usize {
        self.num_redundant_intermediate_symbols()
    }

    pub fn num_encoded_symbols_received(&self) -> usize {
        self.buffer_state.len() - self.num_redundant_intermediate_symbols()
    }
}
