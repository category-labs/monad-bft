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

use super::types::Timestamp;

pub struct Timers<E> {
    pending: BTreeMap<(Timestamp, WakeId), E>,
    next_wake: WakeId,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct WakeId(u64);

impl WakeId {
    const MIN: Self = Self(0);

    fn inc(&mut self) -> Self {
        let id = *self;
        self.0 += 1;
        id
    }
}

impl<E> Default for Timers<E> {
    fn default() -> Self {
        Self {
            pending: BTreeMap::new(),
            next_wake: WakeId::MIN,
        }
    }
}

impl<E> Timers<E> {
    pub fn schedule(&mut self, at: Timestamp, event: E) {
        let id = self.next_wake.inc();
        self.pending.insert((at, id), event);
    }

    pub fn next_due(&self) -> Option<Timestamp> {
        self.pending.keys().next().map(|(at, _)| *at)
    }

    pub fn pop_due(&mut self, now: Timestamp) -> Option<E> {
        if self.next_due()? > now {
            return None;
        }
        self.pending.pop_first().map(|(_, event)| event)
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&E) -> bool) {
        self.pending.retain(|_, event| keep(event));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_by_due_time_then_arming_order() {
        let mut timers = Timers::default();
        timers.schedule(Timestamp::from_millis(20), "late");
        timers.schedule(Timestamp::from_millis(10), "first");
        timers.schedule(Timestamp::from_millis(10), "second");
        assert_eq!(timers.next_due(), Some(Timestamp::from_millis(10)));

        assert_eq!(timers.pop_due(Timestamp::from_millis(5)), None);
        assert_eq!(timers.pop_due(Timestamp::from_millis(10)), Some("first"));
        assert_eq!(timers.pop_due(Timestamp::from_millis(10)), Some("second"));
        assert_eq!(timers.pop_due(Timestamp::from_millis(10)), None);
        assert_eq!(timers.next_due(), Some(Timestamp::from_millis(20)));

        timers.retain(|event| *event != "late");
        assert_eq!(timers.next_due(), None);
    }
}
