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

use crate::r10::nonsystematic::decoder::Buffer;

// Buffer state is not encoded explicitly; it is a function of the explicitly encoded state.
#[derive(Debug, Eq, PartialEq)]
pub enum BufferState {
    // weight > 1, active_used_weight > 1
    Active,

    // weight >= 1, active_used_weight == 1, and the corresponding single active intermediate
    // symbol is Active.
    Usable,

    // weight >= 1, active_used_weight == 1, and the corresponding single active intermediate
    // symbol is Used.
    //
    // If weight == 1, the buffer is paired with its corresponding intermediate symbol.
    Used,

    // weight > 0, active_used_weight == 0
    //
    // Contains only inactivated intermediate symbols.  This buffer can be used in phase 2 of
    // the inactivation decoding process, where we perform Gaussian elimination on the set of
    // Inactivated buffers to try to reduce the weights of those buffers to 1.
    //
    // If weight == 1, we can reactivate the corresponding intermediate symbol and transition
    // this buffer to Usable/Used.
    Inactivated,

    // weight == 0, active_used_weight == 0
    //
    // This buffer corresponds to a constrant symbol or encoded symbol that was linearly
    // dependent on another set of constraint symbols and/or encoded symbols, and is not
    // contributing to the decoding process.
    Redundant,
}

impl Buffer {
    pub fn state(&self) -> BufferState {
        if self.active_used_weight > 1 {
            BufferState::Active
        } else if self.active_used_weight == 1 && !self.used {
            BufferState::Usable
        } else if self.active_used_weight == 1 && self.used {
            BufferState::Used
        } else if !self.intermediate_symbol_ids.is_empty() {
            BufferState::Inactivated
        } else {
            BufferState::Redundant
        }
    }

    pub fn is_paired(&self) -> bool {
        self.state() == BufferState::Used && self.intermediate_symbol_ids.len() == 1
    }

    pub fn is_reactivatable(&self) -> bool {
        self.state() == BufferState::Inactivated && self.intermediate_symbol_ids.len() == 1
    }
}
