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
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use monad_chain_data::{
    engine::tables::{PublicationTables, Tables},
    error::MonadChainDataError,
    store::{
        BlobStore, BlobTableId, BlobWriteOp, CacheConfig, CasOutcome, CasVersion,
        InMemoryBlobStore, InMemoryMetaStore, MetaStore, MetaStoreCas, MetaWriteOp, Page,
        PublicationCasParams, ScannableTableId, TableId,
    },
};

const T_KV: TableId = TableId::new("session_test_kv");
const T_BLOB: BlobTableId = BlobTableId::new("session_test_blob");

#[derive(Clone)]
struct FailingMeta {
    inner: InMemoryMetaStore,
    fail_next: Arc<AtomicUsize>,
}

impl FailingMeta {
    fn new() -> Self {
        Self {
            inner: InMemoryMetaStore::default(),
            fail_next: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn fail_apply_writes_once(&self) {
        self.fail_next.store(1, Ordering::Relaxed);
    }
}

impl MetaStore for FailingMeta {
    async fn get(&self, table: TableId, key: &[u8]) -> monad_chain_data::error::Result<Option<Bytes>> {
        self.inner.get(table, key).await
    }
    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> monad_chain_data::error::Result<Option<Bytes>> {
        self.inner.scan_get(table, partition, clustering).await
    }
    async fn put(
        &self,
        table: TableId,
        key: &[u8],
        value: Bytes,
    ) -> monad_chain_data::error::Result<()> {
        self.inner.put(table, key, value).await
    }
    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> monad_chain_data::error::Result<()> {
        self.inner.scan_put(table, partition, clustering, value).await
    }
    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> monad_chain_data::error::Result<Page> {
        self.inner.scan_list(table, partition, prefix, cursor, limit).await
    }
    async fn apply_writes(&self, writes: Vec<MetaWriteOp>) -> monad_chain_data::error::Result<()> {
        if self.fail_next.swap(0, Ordering::Relaxed) > 0 {
            return Err(MonadChainDataError::Backend(
                "FailingMeta: injected meta failure".into(),
            ));
        }
        self.inner.apply_writes(writes).await
    }
    async fn apply_writes_with_cas(
        &self,
        writes: Vec<MetaWriteOp>,
        cas: PublicationCasParams,
    ) -> monad_chain_data::error::Result<CasOutcome> {
        if self.fail_next.swap(0, Ordering::Relaxed) > 0 {
            return Err(MonadChainDataError::Backend(
                "FailingMeta: injected cas failure".into(),
            ));
        }
        self.inner.apply_writes_with_cas(writes, cas).await
    }
}

impl MetaStoreCas for FailingMeta {
    async fn cas_get(
        &self,
        table: TableId,
        key: &[u8],
    ) -> monad_chain_data::error::Result<Option<(CasVersion, Bytes)>> {
        self.inner.cas_get(table, key).await
    }
    async fn cas_put(
        &self,
        table: TableId,
        key: &[u8],
        expected: Option<CasVersion>,
        value: Bytes,
    ) -> monad_chain_data::error::Result<CasOutcome> {
        self.inner.cas_put(table, key, expected, value).await
    }
}

#[derive(Clone, Default)]
struct TimingBlob {
    inner: InMemoryBlobStore,
    apply_started_at: Arc<std::sync::Mutex<Option<Instant>>>,
    apply_finished_at: Arc<std::sync::Mutex<Option<Instant>>>,
    delay: Duration,
}

impl BlobStore for TimingBlob {
    async fn put_blob(
        &self,
        table: BlobTableId,
        key: &[u8],
        value: Bytes,
    ) -> monad_chain_data::error::Result<()> {
        self.inner.put_blob(table, key, value).await
    }
    async fn get_blob(
        &self,
        table: BlobTableId,
        key: &[u8],
    ) -> monad_chain_data::error::Result<Option<Bytes>> {
        self.inner.get_blob(table, key).await
    }
    async fn apply_writes(&self, writes: Vec<BlobWriteOp>) -> monad_chain_data::error::Result<()> {
        *self.apply_started_at.lock().unwrap() = Some(Instant::now());
        tokio::time::sleep(self.delay).await;
        let r = self.inner.apply_writes(writes).await;
        *self.apply_finished_at.lock().unwrap() = Some(Instant::now());
        r
    }
}

#[derive(Clone)]
struct TimingMeta {
    inner: InMemoryMetaStore,
    apply_started_at: Arc<std::sync::Mutex<Option<Instant>>>,
    apply_finished_at: Arc<std::sync::Mutex<Option<Instant>>>,
    delay: Duration,
}

impl TimingMeta {
    fn new(delay: Duration) -> Self {
        Self {
            inner: InMemoryMetaStore::default(),
            apply_started_at: Arc::new(std::sync::Mutex::new(None)),
            apply_finished_at: Arc::new(std::sync::Mutex::new(None)),
            delay,
        }
    }
}

impl MetaStore for TimingMeta {
    async fn get(&self, table: TableId, key: &[u8]) -> monad_chain_data::error::Result<Option<Bytes>> {
        self.inner.get(table, key).await
    }
    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> monad_chain_data::error::Result<Option<Bytes>> {
        self.inner.scan_get(table, partition, clustering).await
    }
    async fn put(
        &self,
        table: TableId,
        key: &[u8],
        value: Bytes,
    ) -> monad_chain_data::error::Result<()> {
        self.inner.put(table, key, value).await
    }
    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> monad_chain_data::error::Result<()> {
        self.inner.scan_put(table, partition, clustering, value).await
    }
    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> monad_chain_data::error::Result<Page> {
        self.inner.scan_list(table, partition, prefix, cursor, limit).await
    }
    async fn apply_writes(&self, writes: Vec<MetaWriteOp>) -> monad_chain_data::error::Result<()> {
        *self.apply_started_at.lock().unwrap() = Some(Instant::now());
        tokio::time::sleep(self.delay).await;
        let r = self.inner.apply_writes(writes).await;
        *self.apply_finished_at.lock().unwrap() = Some(Instant::now());
        r
    }
    async fn apply_writes_with_cas(
        &self,
        writes: Vec<MetaWriteOp>,
        cas: PublicationCasParams,
    ) -> monad_chain_data::error::Result<CasOutcome> {
        self.inner.apply_writes_with_cas(writes, cas).await
    }
}

impl MetaStoreCas for TimingMeta {
    async fn cas_get(
        &self,
        table: TableId,
        key: &[u8],
    ) -> monad_chain_data::error::Result<Option<(CasVersion, Bytes)>> {
        self.inner.cas_get(table, key).await
    }
    async fn cas_put(
        &self,
        table: TableId,
        key: &[u8],
        expected: Option<CasVersion>,
        value: Bytes,
    ) -> monad_chain_data::error::Result<CasOutcome> {
        self.inner.cas_put(table, key, expected, value).await
    }
}

fn cache() -> CacheConfig {
    CacheConfig {
        block_record_entries: 64,
        block_header_entries: 64,
        block_hash_to_number_entries: 64,
        dir_by_block_entries: 64,
        dir_bucket_entries: 64,
        bitmap_by_block_entries: 64,
        bitmap_page_meta_entries: 64,
        bitmap_page_blob_entries: 64,
        open_bitmap_stream_entries: 64,
        tx_hash_index_entries: 64,
        block_blob_entries: 64,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn closure_error_does_not_flush() {
    let meta = InMemoryMetaStore::default();
    let blob = InMemoryBlobStore::default();
    let tables: Tables<InMemoryMetaStore, InMemoryBlobStore> =
        Tables::with_cache_config(meta.clone(), blob, cache());

    let result = tables
        .with_writes(|w| {
            Box::pin(async move {
                w.put(T_KV, b"k", Bytes::from_static(b"v"));
                Err(MonadChainDataError::Backend("intentional".into()))
            })
        })
        .await;
    assert!(result.is_err());
    assert!(meta.get(T_KV, b"k").await.unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn closure_error_evicts_populated_cache_entries() {
    let meta = InMemoryMetaStore::default();
    let blob = InMemoryBlobStore::default();
    let tables: Tables<InMemoryMetaStore, InMemoryBlobStore> =
        Tables::with_cache_config(meta, blob, cache());
    let header = monad_chain_data::EvmBlockHeader {
        number: 1,
        ..Default::default()
    };
    let header_ref = &header;

    let _ = tables
        .with_writes(|w| {
            Box::pin(async move {
                w.tables().blocks().stage_header(w, 1, header_ref);
                Err(MonadChainDataError::Backend("intentional".into()))
            })
        })
        .await;

    let read = tables.blocks().load_header(1).await.unwrap();
    assert!(
        read.is_none(),
        "cache must be invalidated after closure error"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn partial_flush_failure_evicts_cache() {
    let meta = FailingMeta::new();
    let blob = InMemoryBlobStore::default();
    meta.fail_apply_writes_once();
    let tables: Tables<FailingMeta, InMemoryBlobStore> =
        Tables::with_cache_config(meta, blob, cache());
    let header = monad_chain_data::EvmBlockHeader {
        number: 1,
        ..Default::default()
    };
    let header_ref = &header;

    let result = tables
        .with_writes(|w| {
            Box::pin(async move {
                w.tables().blocks().stage_header(w, 1, header_ref);
                Ok(())
            })
        })
        .await;
    assert!(result.is_err());

    let read = tables.blocks().load_header(1).await.unwrap();
    assert!(read.is_none(), "partial flush failure must evict cache");
}

#[tokio::test(flavor = "current_thread")]
async fn closure_error_then_retry_is_idempotent() {
    let meta = InMemoryMetaStore::default();
    let blob = InMemoryBlobStore::default();
    let tables: Tables<InMemoryMetaStore, InMemoryBlobStore> =
        Tables::with_cache_config(meta.clone(), blob, cache());

    let _ = tables
        .with_writes(|w| {
            Box::pin(async move {
                w.put(T_KV, b"k", Bytes::from_static(b"v1"));
                Err(MonadChainDataError::Backend("intentional".into()))
            })
        })
        .await;

    tables
        .with_writes(|w| {
            Box::pin(async move {
                w.put(T_KV, b"k", Bytes::from_static(b"v1"));
                Ok(())
            })
        })
        .await
        .expect("retry succeeds");

    assert_eq!(
        meta.get(T_KV, b"k").await.unwrap().as_deref(),
        Some(&b"v1"[..])
    );
}

#[tokio::test(flavor = "current_thread")]
async fn with_writes_and_cas_folds_atomically() {
    let meta = InMemoryMetaStore::default();
    let blob = InMemoryBlobStore::default();
    let tables: Tables<InMemoryMetaStore, InMemoryBlobStore> =
        Tables::with_cache_config(meta.clone(), blob, cache());

    let cas = PublicationCasParams {
        table: PublicationTables::<InMemoryMetaStore>::PUBLICATION_STATE_TABLE,
        key: PublicationTables::<InMemoryMetaStore>::PUBLICATION_STATE_KEY.to_vec(),
        // Wrong expected: row absent but we expect a version → Conflict.
        expected: Some(CasVersion(999)),
        value: Bytes::from_static(b"new_state"),
    };

    let outcome = tables
        .with_writes_and_cas(cas, |w| {
            Box::pin(async move {
                w.put(T_KV, b"data_under_cas", Bytes::from_static(b"x"));
                Ok(())
            })
        })
        .await
        .expect("call ok, outcome is Conflict");
    assert!(matches!(outcome, CasOutcome::Conflict { .. }));

    assert!(
        meta.get(T_KV, b"data_under_cas").await.unwrap().is_none(),
        "data writes do not land on CAS conflict"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cas_conflict_evicts_populated_cache_entries() {
    let meta = InMemoryMetaStore::default();
    let blob = InMemoryBlobStore::default();
    let tables: Tables<InMemoryMetaStore, InMemoryBlobStore> =
        Tables::with_cache_config(meta, blob, cache());
    let header = monad_chain_data::EvmBlockHeader {
        number: 1,
        ..Default::default()
    };
    let header_ref = &header;

    let cas = PublicationCasParams {
        table: PublicationTables::<InMemoryMetaStore>::PUBLICATION_STATE_TABLE,
        key: PublicationTables::<InMemoryMetaStore>::PUBLICATION_STATE_KEY.to_vec(),
        expected: Some(CasVersion(999)),
        value: Bytes::from_static(b"v"),
    };
    let outcome = tables
        .with_writes_and_cas(cas, |w| {
            Box::pin(async move {
                w.tables().blocks().stage_header(w, 1, header_ref);
                Ok(())
            })
        })
        .await
        .expect("conflict still returns Ok(outcome)");
    assert!(matches!(outcome, CasOutcome::Conflict { .. }));

    let read = tables.blocks().load_header(1).await.unwrap();
    assert!(read.is_none(), "cache must be invalidated on CAS conflict");
}

#[tokio::test(flavor = "current_thread")]
async fn panic_in_closure_drops_pending() {
    let meta = InMemoryMetaStore::default();
    let blob = InMemoryBlobStore::default();
    let tables: Tables<InMemoryMetaStore, InMemoryBlobStore> =
        Tables::with_cache_config(meta.clone(), blob, cache());

    let panicked = std::panic::AssertUnwindSafe(async {
        tables
            .with_writes(|w| {
                Box::pin(async move {
                    w.put(T_KV, b"panic_key", Bytes::from_static(b"v"));
                    panic!("intentional panic");
                    #[allow(unreachable_code)]
                    Ok(())
                })
            })
            .await
    });

    let r = futures::FutureExt::catch_unwind(panicked).await;
    assert!(r.is_err(), "panic must propagate");
    assert!(
        meta.get(T_KV, b"panic_key").await.unwrap().is_none(),
        "pending writes dropped on panic"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn parallel_meta_and_blob_flush() {
    let delay = Duration::from_millis(50);
    let meta = TimingMeta::new(delay);
    let blob = TimingBlob {
        inner: InMemoryBlobStore::default(),
        apply_started_at: Arc::new(std::sync::Mutex::new(None)),
        apply_finished_at: Arc::new(std::sync::Mutex::new(None)),
        delay,
    };
    let blob_for_check = blob.clone();
    let meta_for_check = meta.clone();
    let tables: Tables<TimingMeta, TimingBlob> =
        Tables::with_cache_config(meta, blob, cache());

    tables
        .with_writes(|w| {
            Box::pin(async move {
                w.put(T_KV, b"k", Bytes::from_static(b"v"));
                w.put_blob(T_BLOB, b"k", Bytes::from_static(b"v"));
                Ok(())
            })
        })
        .await
        .expect("with_writes");

    let meta_start = meta_for_check.apply_started_at.lock().unwrap().unwrap();
    let meta_end = meta_for_check.apply_finished_at.lock().unwrap().unwrap();
    let blob_start = blob_for_check.apply_started_at.lock().unwrap().unwrap();
    let blob_end = blob_for_check.apply_finished_at.lock().unwrap().unwrap();

    let overlap_start = meta_start.max(blob_start);
    let overlap_end = meta_end.min(blob_end);
    assert!(
        overlap_end > overlap_start,
        "meta and blob apply_writes must overlap"
    );
}

