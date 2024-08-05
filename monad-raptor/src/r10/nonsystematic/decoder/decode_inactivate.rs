use crate::r10::nonsystematic::decoder::Decoder;

impl Decoder {
    pub fn try_inactivate_one_symbol(&mut self) -> bool {
        let mut made_progress = false;

        if let Some((buffer_index, active_used_weight)) = self.buffers_active_usable.peek_min() {
            if active_used_weight.get() > 1 {
                made_progress = true;

                // TODO: Make a smarter choice here.
                let intermediate_symbol_id =
                    self.buffer_first_active_intermediate_symbol(buffer_index);

                self.intermediate_symbol_state[usize::from(intermediate_symbol_id)]
                    .active_inactivate();

                for buffer_index in self.intermediate_symbol_state
                    [usize::from(intermediate_symbol_id)]
                .inactivated_values()
                .clone()
                {
                    self.decrement_buffer_weight(buffer_index);
                }

                self.check();
            }
        }

        made_progress
    }
}
