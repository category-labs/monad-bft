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

use std::{collections::BTreeSet, num::NonZeroU16};

use crate::{
    matrix::{DenseMatrix, RowOperation},
    r10::nonsystematic::decoder::{BufferId, BufferState, Decoder},
};

impl Decoder {
    fn buffer_inactivated_xor_eq(&mut self, a: u16, b: u16) {
        let (aref, bref) =
            Self::get_two_mut(&mut self.buffer_state, usize::from(a), usize::from(b));

        debug_assert!(aref.state() == BufferState::Inactivated);

        debug_assert!(bref.state() == BufferState::Inactivated);

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

    // Attempt Gaussian elimination on the inactivated intermediate symbols.
    pub fn try_inactive_gaussian(
        &mut self,
        xor_buffers: &mut impl FnMut(BufferId, BufferId),
    ) -> bool {
        // TODO: Don't perform Gaussian elimination while there are Active intermediate symbols?

        if self.buffers_inactivated.is_empty() {
            // Nothing to eliminate.
            return false;
        }

        if self.buffers_inactivated.peek_min().unwrap().1.get() == 1 {
            // There is an inactivated intermediate symbol we can reactivate, so there is no
            // need to perform Gaussian elimination at this point.
            return true;
        }

        let mut inactivated_buffer_indices: Vec<u16> = Vec::new();

        self.buffers_inactivated
            .enumerate(|buffer_index, _weight| inactivated_buffer_indices.push(buffer_index));

        // TODO: Consider pre-computing part of this.
        let inactivated_intermediate_symbol_ids: BTreeSet<u16> = inactivated_buffer_indices
            .iter()
            .flat_map(|buffer_index| {
                self.buffer_state[usize::from(*buffer_index)]
                    .intermediate_symbol_ids
                    .iter()
                    .copied()
            })
            .collect();

        if inactivated_buffer_indices.len() < inactivated_intermediate_symbol_ids.len() {
            // We need at least as many buffers as intermediate symbols for Gaussian
            // elimination to be successful.
            return false;
        }

        let inactivated_intermediate_symbol_ids: Vec<u16> =
            inactivated_intermediate_symbol_ids.into_iter().collect();

        let mat = DenseMatrix::from_fn(
            inactivated_buffer_indices.len(),
            inactivated_intermediate_symbol_ids.len(),
            |i, j| {
                let buffer_index = inactivated_buffer_indices[i];
                let intermediate_symbol_id = inactivated_intermediate_symbol_ids[j];

                self.buffer_state[usize::from(buffer_index)]
                    .intermediate_symbol_ids
                    .contains(&intermediate_symbol_id)
            },
        );

        let _ = mat.rowwise_elimination_gaussian_full_pivot(|op| match op {
            RowOperation::SubAssign { i, j } => {
                let reducee_buffer_index = inactivated_buffer_indices[i];
                let reducing_buffer_index = inactivated_buffer_indices[j];

                self.buffer_inactivated_xor_eq(reducee_buffer_index, reducing_buffer_index);

                xor_buffers(
                    self.buffer_index_to_buffer_id(reducee_buffer_index),
                    self.buffer_index_to_buffer_id(reducing_buffer_index),
                );
            }
        });

        for buffer_index in &inactivated_buffer_indices {
            let weight = self.buffer_state[usize::from(*buffer_index)]
                .intermediate_symbol_ids
                .len();

            if weight > 0 {
                self.buffers_inactivated.update_buffer_weight(
                    usize::from(*buffer_index),
                    NonZeroU16::new(weight.try_into().unwrap()).unwrap(),
                );
            } else {
                self.buffers_inactivated
                    .remove_buffer_weight(usize::from(*buffer_index));

                self.num_redundant_buffers += 1;
            }
        }

        self.check();

        true
    }
}
