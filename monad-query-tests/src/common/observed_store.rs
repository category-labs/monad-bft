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

//! Observable in-memory meta/blob store proxies: counting, failure injection, delays.

#![allow(dead_code)]

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use monad_query_errors::{QueryError, Result};
use monad_query_store::{
    BlobStore, BlobTableId, BlobWriteOp, InMemoryBlobStore, InMemoryMetaStore, MetaStore,
    MetaWriteOp, ScannableTableId, TableId,
};

#[derive(Default)]
pub struct OpCounters {
    pub get: AtomicUsize,
    pub scan_get: AtomicUsize,
    pub scan_keys: AtomicUsize,
    get_by_table: Mutex<HashMap<TableId, usize>>,
}

/// Point-in-time copy of the [`OpCounters`] tallies (all counters monotone).
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct CountersSnapshot {
    pub get: usize,
    pub scan_get: usize,
    pub scan_keys: usize,
}

impl OpCounters {
    pub fn snapshot(&self) -> CountersSnapshot {
        CountersSnapshot {
            get: self.get.load(Ordering::Relaxed),
            scan_get: self.scan_get.load(Ordering::Relaxed),
            scan_keys: self.scan_keys.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
pub struct OpTimings {
    pub apply_started_at: Mutex<Option<Instant>>,
    pub apply_finished_at: Mutex<Option<Instant>>,
}

impl OpTimings {
    /// Runs `fut` with the observation choreography: record start (if
    /// `record`), sleep `delay`, await `fut`, record finish.
    async fn observe<T>(&self, record: bool, delay: Duration, fut: impl Future<Output = T>) -> T {
        if record {
            *self.apply_started_at.lock().unwrap() = Some(Instant::now());
        }
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let r = fut.await;
        if record {
            *self.apply_finished_at.lock().unwrap() = Some(Instant::now());
        }
        r
    }

    /// The recorded `apply_writes` interval; panics if none was recorded.
    pub fn window(&self) -> (Instant, Instant) {
        (
            self.apply_started_at.lock().unwrap().expect("apply ran"),
            self.apply_finished_at
                .lock()
                .unwrap()
                .expect("apply finished"),
        )
    }
}

#[derive(Default)]
pub struct ObserveMode {
    pub count_reads: bool,
    pub count_gets_by_table: AtomicBool,
    pub fail_next_apply: AtomicBool,
    pub apply_delay: Duration,
    pub record_apply_timings: bool,
}

impl ObserveMode {
    pub fn fail_apply_writes_once(&self) {
        self.fail_next_apply.store(true, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct ObservedMetaStore {
    inner: InMemoryMetaStore,
    pub mode: Arc<ObserveMode>,
    pub counters: Arc<OpCounters>,
    pub timings: Arc<OpTimings>,
}

impl Default for ObservedMetaStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ObservedMetaStore {
    fn build(inner: InMemoryMetaStore, mode: ObserveMode) -> Self {
        Self {
            inner,
            mode: Arc::new(mode),
            counters: Arc::new(OpCounters::default()),
            timings: Arc::new(OpTimings::default()),
        }
    }

    pub fn new() -> Self {
        Self::build(InMemoryMetaStore::default(), ObserveMode::default())
    }

    pub fn counting() -> Self {
        Self::build(
            InMemoryMetaStore::default(),
            ObserveMode {
                count_reads: true,
                ..ObserveMode::default()
            },
        )
    }

    pub fn timed(delay: Duration) -> Self {
        Self::build(
            InMemoryMetaStore::default(),
            ObserveMode {
                apply_delay: delay,
                record_apply_timings: true,
                ..ObserveMode::default()
            },
        )
    }

    /// Wraps an already-populated store; the in-memory backing is clone-shared,
    /// so reads observe everything the original (or the engine) wrote.
    pub fn over(inner: InMemoryMetaStore) -> Self {
        Self::build(inner, ObserveMode::default())
    }

    /// Clears the per-table `get` tallies and starts counting. With the
    /// relevant cache disabled each page load issues exactly one `get` on its
    /// table, so a table's tally is a faithful per-fetch counter.
    pub fn start_counting(&self) {
        self.counters.get_by_table.lock().unwrap().clear();
        self.mode.count_gets_by_table.store(true, Ordering::SeqCst);
    }

    /// `get` calls observed on `table` since [`Self::start_counting`].
    pub fn get_calls(&self, table: TableId) -> usize {
        self.counters
            .get_by_table
            .lock()
            .unwrap()
            .get(&table)
            .copied()
            .unwrap_or(0)
    }
}

impl MetaStore for ObservedMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Bytes>> {
        if self.mode.count_reads {
            self.counters.get.fetch_add(1, Ordering::Relaxed);
        }
        if self.mode.count_gets_by_table.load(Ordering::SeqCst) {
            *self
                .counters
                .get_by_table
                .lock()
                .unwrap()
                .entry(table)
                .or_insert(0) += 1;
        }
        self.inner.get(table, key).await
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Bytes>> {
        if self.mode.count_reads {
            self.counters.scan_get.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.scan_get(table, partition, clustering).await
    }

    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<()> {
        self.inner.put(table, key, value).await
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<()> {
        self.inner
            .scan_put(table, partition, clustering, value)
            .await
    }

    async fn scan_keys(&self, table: ScannableTableId, partition: &[u8]) -> Result<Vec<Vec<u8>>> {
        if self.mode.count_reads {
            self.counters.scan_keys.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.scan_keys(table, partition).await
    }

    async fn apply_writes(&self, writes: Vec<MetaWriteOp>) -> Result<()> {
        if self.mode.fail_next_apply.swap(false, Ordering::Relaxed) {
            return Err(QueryError::Backend(
                "ObservedMetaStore: injected meta failure".into(),
            ));
        }
        self.timings
            .observe(
                self.mode.record_apply_timings,
                self.mode.apply_delay,
                self.inner.apply_writes(writes),
            )
            .await
    }
}

#[derive(Clone, Default)]
pub struct ObservedBlobStore {
    inner: InMemoryBlobStore,
    pub timings: Arc<OpTimings>,
    pub apply_delay: Duration,
    pub record_apply_timings: bool,
}

impl ObservedBlobStore {
    pub fn timed(delay: Duration) -> Self {
        Self {
            inner: InMemoryBlobStore::default(),
            timings: Arc::new(OpTimings::default()),
            apply_delay: delay,
            record_apply_timings: true,
        }
    }
}

impl BlobStore for ObservedBlobStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        self.inner.put_blob(table, key, value).await
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get_blob(table, key).await
    }

    async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> Result<()> {
        self.inner.delete_blob(table, key).await
    }

    async fn apply_writes(&self, writes: Vec<BlobWriteOp>) -> Result<()> {
        self.timings
            .observe(
                self.record_apply_timings,
                self.apply_delay,
                self.inner.apply_writes(writes),
            )
            .await
    }
}
