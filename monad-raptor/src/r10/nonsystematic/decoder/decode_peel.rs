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

use crate::r10::nonsystematic::decoder::{BufferId, BufferState, Decoder};

impl Decoder {
    fn buffer_peel_xor_eq(&mut self, a: u16, b: u16) {
        let (aref, bref) =
            Self::get_two_mut(&mut self.buffer_state, usize::from(a), usize::from(b));

        debug_assert!(aref.state() == BufferState::Active || aref.state() == BufferState::Usable);

        debug_assert!(bref.state() == BufferState::Used);

        for intermediate_symbol_id in &bref.intermediate_symbol_ids {
            let symbol = &mut self.intermediate_symbol_state[usize::from(*intermediate_symbol_id)];

            if !symbol.is_inactivated() {
                let ret = aref.intermediate_symbol_ids.remove(intermediate_symbol_id);
                debug_assert!(ret);
            } else if aref
                .intermediate_symbol_ids
                .insert_or_remove(*intermediate_symbol_id)
            {
                symbol.inactivated_insert(a);
            } else {
                symbol.inactivated_remove(a);
            }
        }
    }

    // Check for buffers that contain exactly one non-inactivated intermediate symbol, and use
    // such a buffer to reduce that intermediate symbol from every other buffer that contains it.
    pub fn try_peel(&mut self, xor_buffers: &mut impl FnMut(BufferId, BufferId)) -> bool {
        let mut made_progress = false;

        while let Some((reducing_buffer_index, active_used_weight)) =
            self.buffers_active_usable.peek_min()
        {
            if self.decoding_done() || active_used_weight.get() != 1 {
                break;
            }

            // This buffer is transitioning from Usable to Used, and will therefore no longer
            // be represented in `self.buffers_active_usable`.
            let weight = self
                .buffers_active_usable
                .remove_buffer_weight(usize::from(reducing_buffer_index));
            debug_assert!(weight == Some(active_used_weight));

            let intermediate_symbol_id =
                self.buffer_first_active_intermediate_symbol(reducing_buffer_index);

            self.buffer_state[usize::from(reducing_buffer_index)].used = true;

            let reducee_buffer_indices = self.intermediate_symbol_state
                [usize::from(intermediate_symbol_id)]
            .active_make_used(reducing_buffer_index);

            for reducee_buffer_index in reducee_buffer_indices {
                if reducee_buffer_index != reducing_buffer_index {
                    self.buffer_peel_xor_eq(reducee_buffer_index, reducing_buffer_index);

                    // Buffer `reducing_buffer_index` has active_used_weight == 1, so we
                    // decrement the weight of buffer `reducee_buffer_index` by 1.
                    self.decrement_buffer_weight(reducee_buffer_index);

                    xor_buffers(
                        self.buffer_index_to_buffer_id(reducee_buffer_index),
                        self.buffer_index_to_buffer_id(reducing_buffer_index),
                    );
                }
            }

            if self.buffer_state[usize::from(reducing_buffer_index)]
                .intermediate_symbol_ids
                .len()
                == 1
                && usize::from(intermediate_symbol_id) < self.params.num_source_symbols()
            {
                self.num_source_symbols_paired += 1;
            }

            self.check();

            made_progress = true;
        }

        made_progress
    }
}
