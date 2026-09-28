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

//! A set of at-most-one-armed timers keyed by `K` — the pacemaker pattern every
//! node uses (one live timeout per variant, re-arming resets it). Holds the
//! engine's [`CancelToken`]s; the caller schedules the actual step and hands the
//! token here.

use std::{collections::HashMap, hash::Hash};

use monad_sim::CancelToken;

/// One armed timer per key. Arming a key cancels any timer previously armed
/// under it.
pub struct Timers<K> {
    armed: HashMap<K, CancelToken>,
}

impl<K> Default for Timers<K> {
    fn default() -> Self {
        Self {
            armed: HashMap::new(),
        }
    }
}

impl<K: Eq + Hash> Timers<K> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the token of a freshly-scheduled timer, cancelling any previous
    /// timer under `key`. Schedule the step first, then pass its token here.
    pub fn arm(&mut self, key: K, token: CancelToken) {
        if let Some(previous) = self.armed.insert(key, token) {
            previous.cancel();
        }
    }

    /// Cancel and forget the timer under `key`, if any.
    pub fn reset(&mut self, key: &K) {
        if let Some(previous) = self.armed.remove(key) {
            previous.cancel();
        }
    }

    /// Forget the timer under `key` without cancelling it — use when the timer
    /// has just fired and its step is running.
    pub fn clear(&mut self, key: &K) {
        self.armed.remove(key);
    }

    /// Whether a live (uncancelled, unfired) timer is armed under `key`.
    pub fn is_armed(&self, key: &K) -> bool {
        self.armed.contains_key(key)
    }
}
