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

//! Shared meta/blob store proxies used by cache and session tests. Each test
//! file used to define its own near-identical CountingMeta / FailingMeta /
//! TimingMeta variants; this module folds them into one observable store
//! whose behavior the caller dials in via [`ObserveMode`].

#![allow(dead_code)]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use monad_chain_data::{
    error::{MonadChainDataError, Result},
    store::{
        BlobStore, BlobTableId, BlobWriteOp, InMemoryBlobStore, InMemoryMetaStore, MetaStore,
        MetaWriteOp, Page, ScannableTableId, TableId,
    },
};

#[derive(Default)]
pub struct OpCounters {
    pub get: AtomicUsize,
    pub scan_get: AtomicUsize,
    pub scan_list: AtomicUsize,
}

impl OpCounters {
    pub fn snapshot(&self) -> (usize, usize, usize) {
        (
            self.get.load(Ordering::Relaxed),
            self.scan_get.load(Ordering::Relaxed),
            self.scan_list.load(Ordering::Relaxed),
        )
    }
}

#[derive(Default)]
pub struct OpTimings {
    pub apply_started_at: Mutex<Option<Instant>>,
    pub apply_finished_at: Mutex<Option<Instant>>,
}

#[derive(Default)]
pub struct ObserveMode {
    pub count_reads: bool,
    pub fail_next_apply: AtomicUsize,
    pub apply_delay: Duration,
    pub record_apply_timings: bool,
}

impl ObserveMode {
    pub fn fail_apply_writes_once(&self) {
        self.fail_next_apply.store(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct ObservedMetaStore {
    inner: InMemoryMetaStore,
    pub mode: Arc<ObserveMode>,
    pub counters: Arc<OpCounters>,
    pub timings: Arc<OpTimings>,
}

impl ObservedMetaStore {
    pub fn new() -> Self {
        Self {
            inner: InMemoryMetaStore::default(),
            mode: Arc::new(ObserveMode::default()),
            counters: Arc::new(OpCounters::default()),
            timings: Arc::new(OpTimings::default()),
        }
    }

    pub fn with_mode(mode: ObserveMode) -> Self {
        Self {
            inner: InMemoryMetaStore::default(),
            mode: Arc::new(mode),
            counters: Arc::new(OpCounters::default()),
            timings: Arc::new(OpTimings::default()),
        }
    }

    pub fn counting() -> Self {
        Self::with_mode(ObserveMode {
            count_reads: true,
            ..ObserveMode::default()
        })
    }

    pub fn timed(delay: Duration) -> Self {
        Self::with_mode(ObserveMode {
            apply_delay: delay,
            record_apply_timings: true,
            ..ObserveMode::default()
        })
    }
}

impl MetaStore for ObservedMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Bytes>> {
        if self.mode.count_reads {
            self.counters.get.fetch_add(1, Ordering::Relaxed);
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

    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        if self.mode.count_reads {
            self.counters.scan_list.fetch_add(1, Ordering::Relaxed);
        }
        self.inner
            .scan_list(table, partition, prefix, cursor, limit)
            .await
    }

    async fn apply_writes(&self, writes: Vec<MetaWriteOp>) -> Result<()> {
        if self.mode.fail_next_apply.swap(0, Ordering::Relaxed) > 0 {
            return Err(MonadChainDataError::Backend(
                "ObservedMetaStore: injected meta failure".into(),
            ));
        }
        if self.mode.record_apply_timings {
            *self.timings.apply_started_at.lock().unwrap() = Some(Instant::now());
        }
        if !self.mode.apply_delay.is_zero() {
            tokio::time::sleep(self.mode.apply_delay).await;
        }
        let r = self.inner.apply_writes(writes).await;
        if self.mode.record_apply_timings {
            *self.timings.apply_finished_at.lock().unwrap() = Some(Instant::now());
        }
        r
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

    async fn apply_writes(&self, writes: Vec<BlobWriteOp>) -> Result<()> {
        if self.record_apply_timings {
            *self.timings.apply_started_at.lock().unwrap() = Some(Instant::now());
        }
        if !self.apply_delay.is_zero() {
            tokio::time::sleep(self.apply_delay).await;
        }
        let r = self.inner.apply_writes(writes).await;
        if self.record_apply_timings {
            *self.timings.apply_finished_at.lock().unwrap() = Some(Instant::now());
        }
        r
    }
}
