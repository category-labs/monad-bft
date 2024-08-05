use crate::ordered_set::OrderedSet;

#[derive(Debug)]
pub struct Buffer {
    // The IDs of all intermediate symbols that are currently XORd into this buffer.
    pub intermediate_symbol_ids: OrderedSet,

    // The number of elements of `intermediate_symbol_ids` that correspond to intermediate
    // symbols that are in the Active or in the Used state.  (That is, the number of
    // referenced intermediate symbols that are not in the Inactivated state.)
    pub active_used_weight: u16,

    // If this is true, intermediate_symbol_ids.len() >= 1, active_used_weight == 1, and
    // the corresponding single active intermediate symbol is in the Used state.
    pub used: bool,
}

impl Buffer {
    pub fn new() -> Buffer {
        Buffer {
            intermediate_symbol_ids: OrderedSet::new(),
            active_used_weight: 0,
            used: false,
        }
    }

    pub fn append_active_intermediate_symbol_id(&mut self, intermediate_symbol_id: usize) {
        self.append_intermediate_symbol_id(intermediate_symbol_id, true);
    }

    pub fn append_intermediate_symbol_id(
        &mut self,
        intermediate_symbol_id: usize,
        increment_active_used_weight: bool,
    ) {
        self.intermediate_symbol_ids
            .append(intermediate_symbol_id.try_into().unwrap());

        if increment_active_used_weight {
            self.active_used_weight += 1;
        }
    }

    pub fn first_intermediate_symbol_id(&self) -> u16 {
        self.intermediate_symbol_ids.first().copied().unwrap()
    }

    // Caller is responsible for dropping `self.active_used_weight` by 1 and performing
    // any other necessary bookkeeping associated with that.
    pub fn xor_eq(&mut self, other: &Buffer) {
        for intermediate_symbol_id in &other.intermediate_symbol_ids {
            self.intermediate_symbol_ids
                .insert_or_remove(*intermediate_symbol_id);
        }
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new()
    }
}
