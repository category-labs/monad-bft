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
