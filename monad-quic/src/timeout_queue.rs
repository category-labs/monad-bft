use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::Duration,
};

use quinn_proto::ConnectionHandle;

#[derive(Default)]
pub(crate) struct TimeoutQueue {
    timeouts: BTreeMap<Duration, BTreeSet<ConnectionHandle>>,
    connection_timeouts: HashMap<ConnectionHandle, Duration>,
}

impl TimeoutQueue {
    pub fn peek_tick(&self) -> Option<Duration> {
        let (tick, _) = self.timeouts.first_key_value()?;
        Some(*tick)
    }

    pub fn pop(&mut self) -> Option<(Duration, ConnectionHandle)> {
        let mut entry = self.timeouts.first_entry()?;

        let timeout_tick = *entry.key();
        let handles = entry.get_mut();
        let handle = handles.pop_first().expect("invariant broken");
        self.connection_timeouts
            .remove(&handle)
            .expect("invariant broken");
        if handles.is_empty() {
            self.timeouts.pop_first().expect("invariant broken");
        }

        Some((timeout_tick, handle))
    }

    pub fn insert(&mut self, tick: Duration, handle: &ConnectionHandle) {
        let tick_timeouts = self.timeouts.entry(tick).or_default();
        if tick_timeouts.contains(handle) {
            return;
        }
        tick_timeouts.insert(*handle);
        if let Some(removed) = self.connection_timeouts.insert(*handle, tick) {
            let old_timeouts = self.timeouts.get_mut(&removed).expect("invariant broken");
            let deleted = old_timeouts.remove(handle);
            assert!(deleted);
            if old_timeouts.is_empty() {
                self.timeouts.remove(&removed).expect("invariant broken");
            }
        }
    }
}
