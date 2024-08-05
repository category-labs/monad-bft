use crate::r10::nonsystematic::decoder::{BufferId, Decoder};

impl Decoder {
    pub fn decoding_done(&self) -> bool {
        self.num_source_symbols_paired == self.params.num_source_symbols()
    }

    pub fn source_symbol_to_buffer_id(&self, source_symbol_id: usize) -> Option<BufferId> {
        if source_symbol_id < self.params.num_source_symbols() {
            match self.intermediate_symbol_state[source_symbol_id].is_used_buffer_index() {
                None => None,
                Some(buffer_index) => {
                    let buffer = &self.buffer_state[usize::from(buffer_index)];

                    if buffer.intermediate_symbol_ids.len() == 1 {
                        debug_assert!(buffer.is_paired());

                        Some(self.buffer_index_to_buffer_id(buffer_index))
                    } else {
                        None
                    }
                }
            }
        } else {
            None
        }
    }
}
