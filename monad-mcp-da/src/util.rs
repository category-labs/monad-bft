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

use std::collections::BTreeSet;

use super::types::Slot;

// todo: share with conductor's CompletionTracker
pub struct SlotCompletion {
    cap: Slot,
    completed_slots: BTreeSet<Slot>,
}

impl SlotCompletion {
    pub fn new() -> Self {
        Self {
            cap: Slot::MIN,
            completed_slots: BTreeSet::new(),
        }
    }

    pub fn mark_completed(&mut self, slot: Slot) {
        if slot < self.cap {
            return;
        }

        self.completed_slots.insert(slot);

        while self.completed_slots.contains(&self.cap) {
            self.completed_slots.remove(&self.cap);
            self.cap = self.cap.checked_next().expect("slot cap overflow");
        }
    }

    pub fn cap(&self) -> Slot {
        self.cap
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cap_advances_through_contiguous_completions_only() {
        let mut completion = SlotCompletion::new();

        completion.mark_completed(Slot(2));
        assert_eq!(completion.cap(), Slot(0));
        completion.mark_completed(Slot(0));
        assert_eq!(completion.cap(), Slot(1));
        completion.mark_completed(Slot(1));
        assert_eq!(completion.cap(), Slot(3));

        // below the cap is already accounted for
        completion.mark_completed(Slot(0));
        assert_eq!(completion.cap(), Slot(3));
    }
}
