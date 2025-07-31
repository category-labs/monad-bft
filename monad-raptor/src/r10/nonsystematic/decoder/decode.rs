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

use std::{cmp::Ordering, num::NonZeroU16};

use crate::r10::nonsystematic::decoder::{BufferId, Decoder};

impl Decoder {
    // Split a borrow to be able to get two mutable references to different elements of a
    // slice without using unsafe code.  (This essentially emulates std::slice::get_many_mut(),
    // without using unsafe code.)
    pub fn get_two_mut<T>(slice: &mut [T], a: usize, b: usize) -> (&mut T, &mut T) {
        match a.cmp(&b) {
            Ordering::Less => {
                let (first, second) = slice.split_at_mut(b);

                (&mut first[a], &mut second[0])
            }
            Ordering::Greater => {
                let (first, second) = slice.split_at_mut(a);

                (&mut second[0], &mut first[b])
            }
            Ordering::Equal => panic!(),
        }
    }

    pub fn buffer_first_active_intermediate_symbol(&self, buffer_index: u16) -> u16 {
        self.buffer_state[usize::from(buffer_index)]
            .intermediate_symbol_ids
            .iter()
            .find(|intermediate_symbol_id| {
                self.intermediate_symbol_state[usize::from(**intermediate_symbol_id)].is_active()
            })
            .copied()
            .unwrap()
    }

    pub fn decrement_buffer_weight(&mut self, buffer_index: u16) {
        let buffer = &mut self.buffer_state[usize::from(buffer_index)];

        buffer.active_used_weight -= 1;

        if buffer.active_used_weight > 0 {
            self.buffers_active_usable.update_buffer_weight(
                usize::from(buffer_index),
                NonZeroU16::new(buffer.active_used_weight).unwrap(),
            );
        } else {
            let weight = self
                .buffers_active_usable
                .remove_buffer_weight(usize::from(buffer_index))
                .unwrap();
            debug_assert!(weight == NonZeroU16::new(1).unwrap());

            let weight = buffer.intermediate_symbol_ids.len();

            if weight > 0 {
                self.buffers_inactivated.insert_buffer_weight(
                    buffer_index.into(),
                    NonZeroU16::new(weight.try_into().unwrap()).unwrap(),
                );
            } else {
                self.num_redundant_buffers += 1;
            }
        }
    }

    pub fn try_decode(
        &mut self,
        inactivation_symbol_threshold: usize,
        mut xor_buffers: impl FnMut(BufferId, BufferId),
    ) -> bool {
        assert!(inactivation_symbol_threshold >= self.params.num_source_symbols());

        #[derive(PartialEq)]
        enum DecodingState {
            ReactivateSymbols,
            Peeling,
            MaybeInactiveGaussian,
            InactivateSymbol,
            Done,
        }

        // If the number of (non-redundant) encoded symbols that has been received is at least
        // a configurable multiple (>= 1.0) of the number of source symbols, we will proceed
        // to using inactivation decoding to try to recover all source symbols.
        //
        // We don't want to resort to inactivation decoding too quickly, as the cost of full
        // source symbol recovery via inactivation decoding scales inversely with the number
        // of (non-redundant) encoded symbols that has been received.
        let usable_buffers = self.buffer_state.len() - usize::from(self.num_redundant_buffers);
        let try_harder = usable_buffers >= inactivation_symbol_threshold;

        // Keep trying to reactivate intermediate symbols and peeling in a loop as long as that
        // makes decoding progress.  Once those two operations stop making progress, try
        // performing Gaussian elimination on the inactive intermediate symbols, and if that
        // succeeds, go back to looping.  If Gaussian elimination on the inactive intermediate
        // symbols does not succeed, try inactivating a single intermediate symbol and then
        // re-starting from the beginning.
        let mut operation = DecodingState::ReactivateSymbols;

        while !self.decoding_done() && operation != DecodingState::Done {
            operation = match operation {
                DecodingState::ReactivateSymbols => {
                    self.try_reactivate_symbols(&mut xor_buffers);

                    DecodingState::Peeling
                }
                DecodingState::Peeling => {
                    if self.try_peel(&mut xor_buffers) {
                        DecodingState::ReactivateSymbols
                    } else {
                        DecodingState::MaybeInactiveGaussian
                    }
                }
                DecodingState::MaybeInactiveGaussian => {
                    if !try_harder {
                        DecodingState::Done
                    } else if self.try_inactive_gaussian(&mut xor_buffers) {
                        DecodingState::ReactivateSymbols
                    } else {
                        DecodingState::InactivateSymbol
                    }
                }
                DecodingState::InactivateSymbol => {
                    if !try_harder {
                        DecodingState::Done
                    } else if self.try_inactivate_one_symbol() {
                        DecodingState::ReactivateSymbols
                    } else {
                        DecodingState::Done
                    }
                }
                DecodingState::Done => panic!(),
            }
        }

        self.decoding_done()
    }
}
