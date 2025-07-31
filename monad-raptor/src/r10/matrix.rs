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

use crate::{matrix::DenseMatrix, r10::CodeParameters};

impl CodeParameters {
    pub fn a_systematic_intermediate_matrix(&self) -> DenseMatrix {
        let mut a = DenseMatrix::from_element(
            self.num_intermediate_symbols(),
            self.num_intermediate_symbols(),
            false,
        );

        self.a_systematic_intermediate(|i, j| {
            a[(i, j)] = true;
        });

        a
    }
}
