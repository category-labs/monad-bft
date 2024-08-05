use std::num::NonZeroU16;

use crate::r10::nonsystematic::decoder::{BufferId, Decoder};

impl Decoder {
    // Check if we have any previously inactivated intermediate symbols for which there is
    // now a buffer that contains only that symbol -- if so, we can reactivate that symbol
    // and use it to reduce the other buffers that contain this symbol.
    pub fn try_reactivate_symbols(
        &mut self,
        xor_buffers: &mut impl FnMut(BufferId, BufferId),
    ) -> bool {
        let mut made_progress = false;

        while let Some((reducing_buffer_index, weight)) = self.buffers_inactivated.peek_min() {
            if self.decoding_done() || weight.get() != 1 {
                break;
            }

            let reducing_buffer = &mut self.buffer_state[usize::from(reducing_buffer_index)];

            self.buffers_inactivated
                .remove_buffer_weight(usize::from(reducing_buffer_index));

            let intermediate_symbol_id = reducing_buffer.first_intermediate_symbol_id();

            reducing_buffer.active_used_weight = 1;
            reducing_buffer.used = true;

            let reducee_buffer_indices = self.intermediate_symbol_state
                [usize::from(intermediate_symbol_id)]
            .inactivated_make_used(reducing_buffer_index);

            for reducee_buffer_index in reducee_buffer_indices {
                if reducee_buffer_index != reducing_buffer_index {
                    let reducee_buffer = &mut self.buffer_state[usize::from(reducee_buffer_index)];

                    let ret = reducee_buffer
                        .intermediate_symbol_ids
                        .remove(&intermediate_symbol_id);
                    debug_assert!(ret);

                    if reducee_buffer.active_used_weight == 0 {
                        let weight = reducee_buffer.intermediate_symbol_ids.len();

                        if weight > 0 {
                            self.buffers_inactivated.update_buffer_weight(
                                usize::from(reducee_buffer_index),
                                NonZeroU16::new(weight.try_into().unwrap()).unwrap(),
                            );
                        } else {
                            self.buffers_inactivated
                                .remove_buffer_weight(usize::from(reducee_buffer_index));

                            self.num_redundant_buffers += 1;
                        }
                    }

                    if reducee_buffer.is_paired()
                        && usize::from(reducee_buffer.first_intermediate_symbol_id())
                            < self.params.num_source_symbols()
                    {
                        self.num_source_symbols_paired += 1;
                    }

                    xor_buffers(
                        self.buffer_index_to_buffer_id(reducee_buffer_index),
                        self.buffer_index_to_buffer_id(reducing_buffer_index),
                    );
                }
            }

            if usize::from(intermediate_symbol_id) < self.params.num_source_symbols() {
                self.num_source_symbols_paired += 1;
            }

            self.check();

            made_progress = true;
        }

        made_progress
    }
}
