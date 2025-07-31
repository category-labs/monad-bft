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

use std::collections::BTreeMap;

use crate::{
    matrix::DenseMatrix,
    r10::nonsystematic::decoder::{BufferState, Decoder, IntermediateSymbol},
};

impl Decoder {
    // Determine the current state of the buffer-to-intermediate-symbol mapping according to
    // the self.buffer_state elements.
    fn buffer_state_to_mapping(&self) -> DenseMatrix {
        let mut m = DenseMatrix::from_element(
            self.buffer_state.len(),
            self.intermediate_symbol_state.len(),
            false,
        );

        for i in 0..self.buffer_state.len() {
            for j in &self.buffer_state[i].intermediate_symbol_ids {
                m[(i, usize::from(*j))] = true;
            }
        }

        m
    }

    // Determine the current state of the buffer-to-intermediate-symbol mapping according to
    // the self.intermediate_symbol_state elements.
    fn intermediate_symbol_state_to_mapping(&self) -> DenseMatrix {
        let mut m = DenseMatrix::from_element(
            self.buffer_state.len(),
            self.intermediate_symbol_state.len(),
            false,
        );

        for j in 0..self.intermediate_symbol_state.len() {
            match &self.intermediate_symbol_state[j] {
                IntermediateSymbol::Active { buffer_indices } => {
                    for i in buffer_indices {
                        m[(usize::from(*i), j)] = true;
                    }
                }
                IntermediateSymbol::Used { buffer_index } => {
                    m[(usize::from(*buffer_index), j)] = true;
                }
                IntermediateSymbol::Inactivated { buffer_indices } => {
                    for i in buffer_indices {
                        m[(usize::from(*i), j)] = true;
                    }
                }
            }
        }

        m
    }

    fn check_elements_match(&self) {
        let left = self.buffer_state_to_mapping();
        let right = self.intermediate_symbol_state_to_mapping();

        if left != right {
            panic!(
                "buffer state mapping = {}, intermediate symbol state mapping = {}",
                left, right
            );
        }
    }

    fn check_used(&self) {
        for i in 0..self.buffer_state.len() {
            if self.buffer_state[i].used {
                assert_eq!(self.buffer_state[i].active_used_weight, 1);

                let active_used_intermediate_symbol_ids: Vec<u16> = self.buffer_state[i]
                    .intermediate_symbol_ids
                    .iter()
                    .filter(|intermediate_symbol_id| {
                        !self.intermediate_symbol_state[usize::from(**intermediate_symbol_id)]
                            .is_inactivated()
                    })
                    .copied()
                    .collect();

                assert_eq!(active_used_intermediate_symbol_ids.len(), 1);

                assert!(self.intermediate_symbol_state
                    [usize::from(active_used_intermediate_symbol_ids[0])]
                .is_used());
            }
        }

        for j in 0..self.intermediate_symbol_state.len() {
            if let Some(buffer_index) = self.intermediate_symbol_state[j].is_used_buffer_index() {
                assert_eq!(
                    self.buffer_state[usize::from(buffer_index)].state(),
                    BufferState::Used
                );
            }
        }
    }

    fn check_active_usable(&self) {
        let mut self_buffers_active_usable: BTreeMap<u16, u16> = BTreeMap::new();

        self.buffers_active_usable
            .enumerate(|buffer_index, active_used_weight| {
                self_buffers_active_usable.insert(buffer_index, active_used_weight.get());
            });

        let mut buffers_active_usable: BTreeMap<u16, u16> = BTreeMap::new();

        for i in 0..self.buffer_state.len() {
            let mut active_used_weight = 0;

            for j in &self.buffer_state[i].intermediate_symbol_ids {
                if !self.intermediate_symbol_state[usize::from(*j)].is_inactivated() {
                    active_used_weight += 1;
                }
            }

            assert_eq!(self.buffer_state[i].active_used_weight, active_used_weight);

            if self.buffer_state[i].state() == BufferState::Active
                || self.buffer_state[i].state() == BufferState::Usable
            {
                buffers_active_usable.insert(i.try_into().unwrap(), active_used_weight);
            }
        }

        assert_eq!(self_buffers_active_usable, buffers_active_usable);
    }

    fn check_inactivated(&self) {
        let mut self_buffers_inactivated: BTreeMap<u16, u16> = BTreeMap::new();

        self.buffers_inactivated
            .enumerate(|buffer_index, active_used_weight| {
                self_buffers_inactivated.insert(buffer_index, active_used_weight.get());
            });

        let mut buffers_inactivated: BTreeMap<u16, u16> = BTreeMap::new();

        for i in 0..self.buffer_state.len() {
            let weight = self.buffer_state[i].intermediate_symbol_ids.len();

            if self.buffer_state[i].state() == BufferState::Inactivated {
                buffers_inactivated.insert(i.try_into().unwrap(), weight.try_into().unwrap());
            }
        }

        assert_eq!(self_buffers_inactivated, buffers_inactivated);
    }

    fn check_num_redundant_buffers(&self) {
        let mut num_redundant_buffers = 0;

        for i in 0..self.buffer_state.len() {
            if self.buffer_state[i].state() == BufferState::Redundant {
                num_redundant_buffers += 1;
            }
        }

        assert_eq!(self.num_redundant_buffers, num_redundant_buffers);
    }

    fn check_num_source_symbols_paired(&self) {
        let mut num_source_symbols_paired = 0;

        for i in 0..self.buffer_state.len() {
            if self.buffer_state[i].is_paired()
                && usize::from(self.buffer_state[i].first_intermediate_symbol_id())
                    < self.params.num_source_symbols()
            {
                num_source_symbols_paired += 1;
            }
        }

        assert_eq!(self.num_source_symbols_paired, num_source_symbols_paired);
    }

    pub fn check_force(&self) {
        self.check_elements_match();
        self.check_used();
        self.check_active_usable();
        self.check_inactivated();
        self.check_num_redundant_buffers();
        self.check_num_source_symbols_paired();
    }

    pub fn check(&self) {
        if cfg!(debug_assertions) {
            self.check_force();
        }
    }
}
