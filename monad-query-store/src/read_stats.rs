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

//! Shared read statistics for remote store backends.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// Lock-free read counters shared by the backends. The two `kinds` slots carry
/// backend-specific read-kind counts (dynamo: GetItem/Query; s3: range/full
/// GET); `items` is unused (0) by backends without an item notion.
#[derive(Debug, Default)]
pub(crate) struct ReadStats {
    started: AtomicU64,
    completed: AtomicU64,
    errors: AtomicU64,
    canceled: AtomicU64,
    kinds: [AtomicU64; 2],
    items: AtomicU64,
    bytes: AtomicU64,
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
}

pub(crate) struct ReadStatsWindow {
    pub(crate) started: u64,
    pub completed: u64,
    pub errors: u64,
    pub canceled: u64,
    pub(crate) kinds: [u64; 2],
    /// Only the dynamo backend reports items; 0 elsewhere.
    #[cfg_attr(not(feature = "dynamo"), allow(dead_code))]
    pub items: u64,
    pub(crate) bytes: u64,
    pub(crate) in_flight: u64,
    pub(crate) max_in_flight: u64,
}

impl ReadStats {
    /// Marks a read of `kind` (a `kinds` slot index) started and returns the
    /// RAII guard: `finish` records the outcome, dropping unfinished counts
    /// the read as canceled.
    pub(crate) fn start(self: &Arc<Self>, kind: usize) -> ReadGuard {
        self.started.fetch_add(1, Ordering::Relaxed);
        self.kinds[kind].fetch_add(1, Ordering::Relaxed);
        let in_flight = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        let mut prev = self.max_in_flight.load(Ordering::Relaxed);
        while in_flight > prev {
            match self.max_in_flight.compare_exchange_weak(
                prev,
                in_flight,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => prev = next,
            }
        }
        ReadGuard {
            stats: Arc::clone(self),
            finished: false,
        }
    }

    pub(crate) fn take(&self) -> ReadStatsWindow {
        ReadStatsWindow {
            started: self.started.swap(0, Ordering::Relaxed),
            completed: self.completed.swap(0, Ordering::Relaxed),
            errors: self.errors.swap(0, Ordering::Relaxed),
            canceled: self.canceled.swap(0, Ordering::Relaxed),
            kinds: [
                self.kinds[0].swap(0, Ordering::Relaxed),
                self.kinds[1].swap(0, Ordering::Relaxed),
            ],
            items: self.items.swap(0, Ordering::Relaxed),
            bytes: self.bytes.swap(0, Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            max_in_flight: self.max_in_flight.swap(0, Ordering::Relaxed),
        }
    }
}

pub(crate) struct ReadGuard {
    stats: Arc<ReadStats>,
    finished: bool,
}

impl ReadGuard {
    pub(crate) fn finish(mut self, error: bool, items: u64, bytes: u64) {
        self.finished = true;
        let stats = &self.stats;
        if error {
            stats.errors.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.completed.fetch_add(1, Ordering::Relaxed);
            stats.items.fetch_add(items, Ordering::Relaxed);
            stats.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        stats.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let stats = &self.stats;
        stats.canceled.fetch_add(1, Ordering::Relaxed);
        stats.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}
