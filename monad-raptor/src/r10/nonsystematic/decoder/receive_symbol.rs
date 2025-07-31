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

use std::num::NonZeroU16;

use crate::r10::{
    nonsystematic::decoder::{Buffer, BufferId, Decoder},
    MAX_DEGREE,
};

impl Decoder {
    pub fn received_encoded_symbol(
        &mut self,
        encoding_symbol_id: usize,
        mut xor_buffers: impl FnMut(BufferId, BufferId),
    ) {
        let buffer_index: u16 = self.buffer_state.len().try_into().unwrap();

        let mut buffer = Buffer::new();

        let mut used_buffer_indices = Vec::with_capacity(MAX_DEGREE);

        // Create initial buffer to intermediate symbol mapping.
        self.params
            .lt_sequence_op(encoding_symbol_id, |intermediate_symbol_id| {
                let symbol = &self.intermediate_symbol_state[intermediate_symbol_id];

                buffer.append_intermediate_symbol_id(
                    intermediate_symbol_id,
                    !symbol.is_inactivated(),
                );

                if let Some(used_buffer_index) = symbol.is_used_buffer_index() {
                    used_buffer_indices.push(used_buffer_index);
                }
            });

        // Reduce this buffer by all intermediate symbols that have already been recovered.
        for used_buffer_index in used_buffer_indices {
            buffer.xor_eq(&self.buffer_state[usize::from(used_buffer_index)]);

            // The buffer we are reducing by has active_used_weight == 1.
            buffer.active_used_weight -= 1;

            xor_buffers(
                self.buffer_index_to_buffer_id(buffer_index),
                self.buffer_index_to_buffer_id(used_buffer_index),
            );
        }

        // Fix up intermediate symbol to buffer index accounting.
        for intermediate_symbol_id in &buffer.intermediate_symbol_ids {
            self.intermediate_symbol_state[usize::from(*intermediate_symbol_id)]
                .active_inactivated_push(buffer_index);
        }

        let weight = buffer.intermediate_symbol_ids.len();
        let active_used_weight = buffer.active_used_weight;

        self.buffer_state.push(buffer);

        if active_used_weight > 0 {
            self.buffers_active_usable.insert_buffer_weight(
                usize::from(buffer_index),
                NonZeroU16::new(active_used_weight).unwrap(),
            );
        } else if weight > 0 {
            self.buffers_inactivated.insert_buffer_weight(
                usize::from(buffer_index),
                NonZeroU16::new(weight.try_into().unwrap()).unwrap(),
            );
        } else {
            self.num_redundant_buffers += 1;
        }

        self.check();
    }

    pub fn num_redundant_encoded_symbols(&self) -> usize {
        self.num_redundant_buffers.into()
    }
}
