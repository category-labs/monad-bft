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

#[derive(Debug)]
pub enum BufferId {
    // One of the temporary buffers allocated by the caller for pre-code symbol recovery.
    TempBuffer { index: usize },

    // A buffer in which an encoded symbol was originally received from the encoder.
    ReceiveBuffer { index: usize },
}

impl Decoder {
    pub fn buffer_index_to_buffer_id(&self, index: u16) -> BufferId {
        let index = usize::from(index);

        let num_redundant_intermediate_symbols = self.num_redundant_intermediate_symbols();

        if index < num_redundant_intermediate_symbols {
            BufferId::TempBuffer { index }
        } else {
            BufferId::ReceiveBuffer {
                index: index - num_redundant_intermediate_symbols,
            }
        }
    }
}
