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

use std::mem::replace;

use crate::ordered_set::OrderedSet;

#[derive(Debug)]
pub enum IntermediateSymbol {
    // This intermediate symbol is either:
    // - not encoded in any buffer (yet); or
    // - XORd into a single buffer that also has other active intermediate symbols XORd into it; or
    // - XORd into multiple buffers.
    Active { buffer_indices: OrderedSet },

    // This intermediate symbol has temporarily stopped counting toward buffer active_used_weight
    // counts so that we can make progress with the peeling process.
    Inactivated { buffer_indices: OrderedSet },

    // This intermediate symbol is XORd into a single buffer that has no other active intermediate
    // symbols (but maybe some inactivated intermediate symbols) XORd into it, and no other buffer
    // has this intermediate symbol XORd into it.
    Used { buffer_index: u16 },
}

impl IntermediateSymbol {
    pub fn new() -> IntermediateSymbol {
        IntermediateSymbol::Active {
            buffer_indices: OrderedSet::new(),
        }
    }
}

impl Default for IntermediateSymbol {
    fn default() -> Self {
        Self::new()
    }
}

// IntermediateSymbol::Active
impl IntermediateSymbol {
    pub fn is_active(&self) -> bool {
        match self {
            IntermediateSymbol::Active { .. } => true,
            IntermediateSymbol::Inactivated { .. } => false,
            IntermediateSymbol::Used { .. } => false,
        }
    }

    // buffer_index must be larger than any element currently in the set.
    pub fn active_push(&mut self, buffer_index: usize) {
        match self {
            IntermediateSymbol::Active { buffer_indices } => {
                buffer_indices.append(buffer_index.try_into().unwrap());
            }
            IntermediateSymbol::Inactivated { .. } => panic!(),
            IntermediateSymbol::Used { .. } => panic!(),
        }
    }

    // buffer_index must be larger than any element currently in the set.
    pub fn active_inactivated_push(&mut self, buffer_index: u16) {
        match self {
            IntermediateSymbol::Active { buffer_indices } => {
                buffer_indices.append(buffer_index);
            }
            IntermediateSymbol::Inactivated { buffer_indices } => {
                buffer_indices.append(buffer_index);
            }
            IntermediateSymbol::Used { .. } => panic!(),
        }
    }

    pub fn active_inactivate(&mut self) {
        // We temporarily stick this value in so that we can convert the vector of buffer
        // indices to a `BTreeSet<>` and put it back in.
        let buffer_indices = self.active_make_used(u16::MAX);

        *self = IntermediateSymbol::Inactivated { buffer_indices };
    }

    pub fn active_make_used(&mut self, buffer_index: u16) -> OrderedSet {
        let old = replace(self, IntermediateSymbol::Used { buffer_index });

        match old {
            IntermediateSymbol::Active { buffer_indices } => buffer_indices,
            IntermediateSymbol::Inactivated { .. } => panic!(),
            IntermediateSymbol::Used { .. } => panic!(),
        }
    }
}

// IntermediateSymbol::Inactivated
impl IntermediateSymbol {
    pub fn is_inactivated(&self) -> bool {
        match self {
            IntermediateSymbol::Active { .. } => false,
            IntermediateSymbol::Inactivated { .. } => true,
            IntermediateSymbol::Used { .. } => false,
        }
    }

    pub fn inactivated_values(&self) -> &OrderedSet {
        match self {
            IntermediateSymbol::Active { .. } => panic!(),
            IntermediateSymbol::Inactivated { buffer_indices } => buffer_indices,
            IntermediateSymbol::Used { .. } => panic!(),
        }
    }

    pub fn inactivated_insert(&mut self, buffer_index: u16) {
        match self {
            IntermediateSymbol::Active { .. } => panic!(),
            IntermediateSymbol::Inactivated { buffer_indices } => {
                let ret = buffer_indices.insert(buffer_index);
                debug_assert!(ret);
            }
            IntermediateSymbol::Used { .. } => panic!(),
        }
    }

    pub fn inactivated_remove(&mut self, buffer_index: u16) {
        match self {
            IntermediateSymbol::Active { .. } => panic!(),
            IntermediateSymbol::Inactivated { buffer_indices } => {
                let ret = buffer_indices.remove(&buffer_index);
                debug_assert!(ret);
            }
            IntermediateSymbol::Used { .. } => panic!(),
        }
    }

    pub fn inactivated_make_used(&mut self, buffer_index: u16) -> OrderedSet {
        let old = replace(self, IntermediateSymbol::Used { buffer_index });

        match old {
            IntermediateSymbol::Active { .. } => panic!(),
            IntermediateSymbol::Inactivated { buffer_indices } => buffer_indices,
            IntermediateSymbol::Used { .. } => panic!(),
        }
    }
}

// IntermediateSymbol::Used
impl IntermediateSymbol {
    pub fn is_used(&self) -> bool {
        match self {
            IntermediateSymbol::Active { .. } => false,
            IntermediateSymbol::Inactivated { .. } => false,
            IntermediateSymbol::Used { .. } => true,
        }
    }

    pub fn is_used_buffer_index(&self) -> Option<u16> {
        match self {
            IntermediateSymbol::Active { .. } => None,
            IntermediateSymbol::Inactivated { .. } => None,
            IntermediateSymbol::Used { buffer_index } => Some(*buffer_index),
        }
    }
}
