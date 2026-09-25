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

use std::{
    sync::{
        Arc, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use monad_mcp_chorus::ledger::{LedgerError, LedgerReader, decode_batch};
use serde::Serialize;
use tracing::{debug, info, warn};

use crate::index::{Index, Inserted, LoadedBlock};

#[derive(Clone, Debug, Default)]
pub struct SharedIndex(Arc<RwLock<Index>>);

impl SharedIndex {
    pub fn new(index: Index) -> Self {
        Self(Arc::new(RwLock::new(index)))
    }

    // a panic while holding the lock leaves the index inconsistent, so it is fatal.
    pub fn read(&self) -> RwLockReadGuard<'_, Index> {
        self.0.read().expect("explorer index lock poisoned")
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, Index> {
        self.0.write().expect("explorer index lock poisoned")
    }
}

#[derive(Debug, Default)]
pub struct Progress {
    total: AtomicU64,
    done: AtomicU64,
    complete: AtomicBool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct ProgressSnapshot {
    pub done: u64,
    pub total: u64,
    pub complete: bool,
}

impl Progress {
    pub fn snapshot(&self) -> ProgressSnapshot {
        ProgressSnapshot {
            done: self.done.load(Ordering::Relaxed),
            total: self.total.load(Ordering::Relaxed),
            complete: self.complete.load(Ordering::Acquire),
        }
    }

    pub fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub enum LoadFailure {
    // deleted by the pruner, before or during the read.
    Pruned,
    // worth retrying, e.g. an io error.
    Transient(LedgerError),
    Corrupt(String),
}

fn classify(reader: &LedgerReader, slot: u64, error: LedgerError) -> LoadFailure {
    // the pruner deletes file by file, so any error on a block that is now gone is a prune.
    if matches!(error, LedgerError::NotFound(_)) || !reader.block_dir(slot).exists() {
        return LoadFailure::Pruned;
    }
    match error {
        LedgerError::Io { .. } => LoadFailure::Transient(error),
        e => LoadFailure::Corrupt(e.to_string()),
    }
}

// reads a block's meta and the lanes that carry txs.
pub fn load_block(reader: &LedgerReader, slot: u64) -> Result<LoadedBlock, LoadFailure> {
    let fail = |e| classify(reader, slot, e);
    let meta = reader.read_meta(slot).map_err(fail)?;
    let mut lanes = Vec::new();
    for lane in meta.lanes.iter().filter(|l| l.tx_count > 0) {
        let payload = reader
            .read_lane_for(&meta, lane.index)
            .map_err(fail)?
            .ok_or_else(|| {
                LoadFailure::Corrupt(format!("lane {} has txs but no root", lane.index))
            })?;
        let txs = decode_batch(&payload)
            .map_err(|e| LoadFailure::Corrupt(format!("lane {}: {e}", lane.index)))?;
        lanes.push((lane.index, txs));
    }
    LoadedBlock::new(&meta, lanes).map_err(|e| LoadFailure::Corrupt(e.to_string()))
}

#[derive(Clone, Debug)]
pub struct LoaderConfig {
    pub tail_interval: Duration,
    // slots below the newest re-probed each tick for late, out-of-order writes.
    pub hole_window: u64,
    pub boot_threads: usize,
    // blocks read per index write lock.
    pub batch: usize,
}

impl Default for LoaderConfig {
    fn default() -> Self {
        Self {
            tail_interval: Duration::from_millis(250),
            hole_window: 64,
            boot_threads: thread::available_parallelism().map_or(4, |n| n.get().min(8)),
            batch: 256,
        }
    }
}

// the full-directory rescan runs at most this often, relative to its own cost.
const RESCAN_COST_FACTOR: u32 = 20;

struct Shared {
    reader: LedgerReader,
    index: SharedIndex,
    progress: Arc<Progress>,
    config: LoaderConfig,
    stop: AtomicBool,
}

impl Shared {
    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    // loads `slots` outside the lock, then inserts them; false once the index is full below them.
    fn load_and_insert(&self, slots: impl IntoIterator<Item = u64>) -> bool {
        let slots: Vec<u64> = {
            let index = self.index.read();
            slots.into_iter().filter(|s| index.admits(*s)).collect()
        };
        if slots.is_empty() {
            return false;
        }
        let mut loaded = Vec::new();
        let mut corrupt = Vec::new();
        for slot in slots {
            match load_block(&self.reader, slot) {
                Ok(block) => loaded.push(block),
                Err(LoadFailure::Pruned) => debug!(slot, "ledger block pruned during read"),
                Err(LoadFailure::Transient(error)) => {
                    warn!(slot, %error, "failed to read ledger block, will retry")
                }
                Err(LoadFailure::Corrupt(reason)) => {
                    warn!(slot, %reason, "skipping unreadable ledger block");
                    corrupt.push(slot);
                }
            }
        }
        let mut index = self.index.write();
        let mut room = true;
        for block in loaded {
            room &= index.insert(block) != Inserted::TooOld;
        }
        for slot in corrupt {
            index.mark_skipped(slot);
        }
        room
    }

    // slots are ascending; loads the newest `retain_slots` of them, newest chunks first.
    fn boot(&self, slots: &[u64]) {
        let started = Instant::now();
        let retain = self.index.read().config().retain_slots;
        let slots = &slots[slots.len().saturating_sub(retain)..];
        self.progress
            .total
            .store(slots.len() as u64, Ordering::Relaxed);
        let chunks: Vec<&[u64]> = slots.rchunks(self.config.batch.max(1)).collect();
        let next = AtomicUsize::new(0);
        let full = AtomicBool::new(false);
        thread::scope(|scope| {
            for _ in 0..self.config.boot_threads.max(1) {
                scope.spawn(|| {
                    while !self.stopped() && !full.load(Ordering::Relaxed) {
                        let Some(chunk) = chunks.get(next.fetch_add(1, Ordering::Relaxed)) else {
                            break;
                        };
                        if !self.load_and_insert(chunk.iter().rev().copied()) {
                            full.store(true, Ordering::Relaxed);
                        }
                        self.progress
                            .done
                            .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    }
                });
            }
        });
        if !self.stopped() {
            // the rest is older than a full index keeps.
            self.progress
                .done
                .store(slots.len() as u64, Ordering::Relaxed);
            self.progress.complete.store(true, Ordering::Release);
            let index = self.index.read();
            info!(
                blocks = index.len(),
                txs = index.tx_len(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "explorer index loaded"
            );
        }
    }

    fn unknown_dirs(&self, slots: impl IntoIterator<Item = u64>) -> Vec<u64> {
        let candidates: Vec<u64> = {
            let index = self.index.read();
            slots.into_iter().filter(|s| !index.is_known(*s)).collect()
        };
        candidates
            .into_iter()
            .filter(|s| self.reader.block_dir(*s).is_dir())
            .collect()
    }

    // cheap per-tick check: late writes just below `high`, then new slots above it.
    fn probe(&self, high: &mut Option<u64>) {
        let Some(h) = *high else {
            return;
        };
        let window = self.config.hole_window;
        let holes = self.unknown_dirs(h.saturating_sub(window)..h);
        if !holes.is_empty() {
            self.load_and_insert(holes);
        }
        let mut from = h;
        loop {
            let found = self.unknown_dirs(from.saturating_add(1)..=from.saturating_add(window));
            let Some(&top) = found.last() else {
                break;
            };
            self.load_and_insert(found);
            *high = Some(top);
            from = top;
            if self.stopped() {
                break;
            }
        }
    }

    // full listing: catches anything the probe missed and drops pruned blocks.
    fn rescan(&self, high: &mut Option<u64>) {
        let slots = match self.reader.scan() {
            Ok(slots) => slots,
            Err(error) => {
                warn!(%error, "failed to list ledger");
                return;
            }
        };
        let (Some(&low), Some(&top)) = (slots.first(), slots.last()) else {
            return;
        };
        *high = Some(high.map_or(top, |h| h.max(top)));
        self.index.write().prune_below(low);
        let missing: Vec<u64> = {
            let index = self.index.read();
            slots
                .into_iter()
                .rev()
                .filter(|s| !index.is_known(*s))
                .take_while(|s| index.admits(*s))
                .take(index.config().retain_slots)
                .collect()
        };
        for chunk in missing.chunks(self.config.batch.max(1)) {
            if self.stopped() || !self.load_and_insert(chunk.iter().copied()) {
                break;
            }
        }
    }

    fn tail(&self, mut high: Option<u64>) {
        let mut last_rescan: Option<Instant> = None;
        let mut rescan_cost = Duration::ZERO;
        while !self.stopped() {
            let tick = Instant::now();
            self.probe(&mut high);
            let due = last_rescan.is_none_or(|t| {
                t.elapsed()
                    >= self
                        .config
                        .tail_interval
                        .max(rescan_cost * RESCAN_COST_FACTOR)
            });
            if self.progress.is_complete() && due {
                let started = Instant::now();
                self.rescan(&mut high);
                rescan_cost = started.elapsed();
                last_rescan = Some(Instant::now());
            }
            if let Some(rest) = self.config.tail_interval.checked_sub(tick.elapsed()) {
                thread::park_timeout(rest);
            }
        }
    }
}

// indexes the ledger in the background: a boot load of the newest blocks, then a tail.
#[derive(Debug)]
pub struct Loader {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("reader", &self.reader)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Loader {
    pub fn spawn(
        reader: LedgerReader,
        index: SharedIndex,
        progress: Arc<Progress>,
        config: LoaderConfig,
    ) -> Self {
        let shared = Arc::new(Shared {
            reader,
            index,
            progress,
            config,
            stop: AtomicBool::new(false),
        });
        let s = shared.clone();
        let thread = thread::Builder::new()
            .name("explorer-loader".into())
            .spawn(move || {
                let slots = loop {
                    match s.reader.scan() {
                        Ok(slots) => break slots,
                        Err(error) => warn!(%error, "failed to list ledger, retrying"),
                    }
                    thread::park_timeout(s.config.tail_interval);
                    if s.stopped() {
                        return;
                    }
                };
                let high = slots.last().copied();
                thread::scope(|scope| {
                    scope.spawn(|| s.boot(&slots));
                    s.tail(high);
                });
            })
            .expect("failed to spawn explorer loader");
        Self {
            shared,
            thread: Some(thread),
        }
    }

    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.shared.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            if thread.join().is_err() {
                warn!("explorer loader panicked");
            }
        }
    }
}

impl Drop for Loader {
    fn drop(&mut self) {
        self.shutdown();
    }
}
