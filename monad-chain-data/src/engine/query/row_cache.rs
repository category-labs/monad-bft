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

//! Decoded-row caches for the query materialization path: query-invariant
//! stored rows keyed `(block_number, idx_in_block)`, inserted only for rows a
//! query actually materialized. A hit skips the backend read, decompress, and
//! RLP decode. Rows are write-once (published regions are immutable), so there
//! is no invalidation; weights come from the decompressed frame length, which
//! the decoded value cannot reconstruct later.

use std::sync::Arc;

use crate::{
    logs::StoredLog,
    store::cache::{weigh_weighted, CachedInner, Weighted},
    traces::StoredTrace,
    txs::StoredTxEnvelope,
};

/// `(block_number, idx_in_block)`. Row indices fit `u32` (the tx index is
/// already stored as one); a wider index simply bypasses the cache.
type RowKey = (u64, u32);

/// One family's decoded-row cache. A thin typed wrapper over the shared
/// weighted-LRU + single-flight core.
pub struct RowCache<T> {
    inner: Arc<CachedInner<RowKey, Weighted<Arc<T>>>>,
}

impl<T: Send + Sync + 'static> RowCache<T> {
    /// `budget_bytes == 0` disables the cache (probes miss, inserts no-op).
    fn new(budget_bytes: usize) -> Self {
        Self {
            inner: CachedInner::with_weigher(budget_bytes, weigh_weighted),
        }
    }

    /// Cache-only probe; promotes the row's LRU position on a hit.
    pub fn probe(&self, block_number: u64, idx_in_block: usize) -> Option<Arc<T>> {
        let idx = u32::try_from(idx_in_block).ok()?;
        self.inner.probe(&(block_number, idx)).map(|w| w.value)
    }

    /// Seeds a row the caller decoded itself. `weight` is the decompressed
    /// frame length the row was decoded from.
    pub fn insert(&self, block_number: u64, idx_in_block: usize, value: Arc<T>, weight: usize) {
        let Ok(idx) = u32::try_from(idx_in_block) else {
            return;
        };
        self.inner
            .insert((block_number, idx), Weighted { value, weight });
    }

    fn take_window_stats(&self) -> (u64, u64) {
        self.inner.take_window_stats()
    }
}

/// The per-family decoded-row caches, held on `Tables`. Transfers share the
/// trace cache (they decode the same `StoredTrace` frames).
pub struct RowCaches {
    pub logs: RowCache<StoredLog>,
    pub txs: RowCache<StoredTxEnvelope>,
    pub traces: RowCache<StoredTrace>,
}

impl RowCaches {
    /// `budget_bytes` is the TOTAL row-cache byte budget, split evenly across
    /// the three families so a hot family cannot starve the others. `0`
    /// disables all three.
    pub fn new(budget_bytes: usize) -> Self {
        let per_family = budget_bytes / 3;
        Self {
            logs: RowCache::new(per_family),
            txs: RowCache::new(per_family),
            traces: RowCache::new(per_family),
        }
    }

    /// Appends per-family `(label, hits, misses)` window stats, skipping idle
    /// caches to keep the stats line legible.
    pub fn collect_window_stats(&self, out: &mut Vec<(&'static str, u64, u64)>) {
        for (name, (h, m)) in [
            ("row:log", self.logs.take_window_stats()),
            ("row:tx", self.txs.take_window_stats()),
            ("row:trace", self.traces.take_window_stats()),
        ] {
            if h != 0 || m != 0 {
                out.push((name, h, m));
            }
        }
    }
}
