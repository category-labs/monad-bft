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

use crate::r10::CodeParameters;

impl CodeParameters {
    // Generate the elements for G_LDPC according to RFC 5053 section 5.4.2.3.
    pub fn ldpc_triple(&self, source_symbol: usize) -> (usize, usize, usize) {
        let a = 1 + ((source_symbol / self.num_ldpc_symbols()) % (self.num_ldpc_symbols() - 1));
        let b1 = source_symbol % self.num_ldpc_symbols();
        let b2 = (b1 + a) % self.num_ldpc_symbols();
        let b3 = (b2 + a) % self.num_ldpc_symbols();

        (b1, b2, b3)
    }

    // Implant the elements for G_LDPC according to RFC 5053 section 5.4.2.3.
    pub fn g_ldpc(&self, mut set_element: impl FnMut(usize, usize)) {
        for i in 0..self.num_source_symbols() {
            let mut b: [usize; 3] = self.ldpc_triple(i).into();

            b.sort();

            for el in b {
                set_element(el, i);
            }
        }
    }
}
