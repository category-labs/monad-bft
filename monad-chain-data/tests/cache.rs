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

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use bytes::Bytes;
use monad_chain_data::{
    engine::{family::Family, tables::Tables},
    store::{
        CacheConfig, CasOutcome, InMemoryBlobStore, InMemoryMetaStore, MetaStore, MetaStoreCas,
        MetaWriteOp, Page, PublicationCasParams, ScannableTableId, TableId,
    },
};

#[derive(Default)]
struct Counters {
    get: AtomicU64,
    scan_get: AtomicU64,
    scan_list: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.get.load(Ordering::Relaxed),
            self.scan_get.load(Ordering::Relaxed),
            self.scan_list.load(Ordering::Relaxed),
        )
    }
}

#[derive(Clone)]
struct CountingMeta {
    inner: InMemoryMetaStore,
    counters: Arc<Counters>,
}

impl CountingMeta {
    fn new() -> Self {
        Self {
            inner: InMemoryMetaStore::default(),
            counters: Arc::new(Counters::default()),
        }
    }
}

impl MetaStore for CountingMeta {
    async fn get(&self, table: TableId, key: &[u8]) -> monad_chain_data::error::Result<Option<Bytes>> {
        self.counters.get.fetch_add(1, Ordering::Relaxed);
        self.inner.get(table, key).await
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> monad_chain_data::error::Result<Option<Bytes>> {
        self.counters.scan_get.fetch_add(1, Ordering::Relaxed);
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
        self.counters.scan_list.fetch_add(1, Ordering::Relaxed);
        self.inner.scan_list(table, partition, prefix, cursor, limit).await
    }

    async fn apply_writes(&self, writes: Vec<MetaWriteOp>) -> monad_chain_data::error::Result<()> {
        self.inner.apply_writes(writes).await
    }

    async fn apply_writes_with_cas(
        &self,
        writes: Vec<MetaWriteOp>,
        cas: PublicationCasParams,
    ) -> monad_chain_data::error::Result<CasOutcome> {
        self.inner.apply_writes_with_cas(writes, cas).await
    }
}

impl MetaStoreCas for CountingMeta {
    async fn cas_get(
        &self,
        table: TableId,
        key: &[u8],
    ) -> monad_chain_data::error::Result<Option<(monad_chain_data::store::CasVersion, Bytes)>> {
        self.inner.cas_get(table, key).await
    }

    async fn cas_put(
        &self,
        table: TableId,
        key: &[u8],
        expected: Option<monad_chain_data::store::CasVersion>,
        value: Bytes,
    ) -> monad_chain_data::error::Result<CasOutcome> {
        self.inner.cas_put(table, key, expected, value).await
    }
}

fn make_tables(cache: CacheConfig) -> (Tables<CountingMeta, InMemoryBlobStore>, Arc<Counters>) {
    let meta = CountingMeta::new();
    let blob = InMemoryBlobStore::default();
    let counters = meta.counters.clone();
    let tables = Tables::with_cache_config(meta, blob, cache);
    (tables, counters)
}

fn small_cache() -> CacheConfig {
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
async fn put_then_get_on_kv_table_serves_from_cache() {
    let (tables, counters) = make_tables(small_cache());
    let header = monad_chain_data::EvmBlockHeader {
        number: 1,
        ..Default::default()
    };
    let header_ref = &header;
    tables
        .with_writes(|w| {
            Box::pin(async move {
                w.tables().blocks().stage_header(w, 1, header_ref);
                Ok(())
            })
        })
        .await
        .expect("with_writes");

    let before = counters.snapshot();
    let v = tables.blocks().load_header(1).await.expect("load_header");
    assert!(v.is_some());
    let after = counters.snapshot();
    assert_eq!(
        after.0, before.0,
        "cached put → get must not call backend get"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn scan_put_then_scan_get_serves_from_cache() {
    let (tables, counters) = make_tables(small_cache());
    tables
        .with_writes(|w| {
            Box::pin(async move {
                w.tables()
                    .family(Family::Log)
                    .dir()
                    .stage_block_fragment(w, 1, 0, 1);
                Ok(())
            })
        })
        .await
        .expect("with_writes");

    let fam = tables.family(Family::Log);
    let before = counters.snapshot();
    let fragments = fam.load_bucket_fragments(0).await.expect("load");
    assert_eq!(fragments.len(), 1);
    let after = counters.snapshot();
    // load_bucket_fragments issues exactly one scan_list (uncached) plus one
    // scan_get per clustering. The scan_gets must hit the cache.
    assert_eq!(after.1, before.1, "scan_gets must hit cache");
    assert_eq!(
        after.2.saturating_sub(before.2),
        1,
        "list_prefix is uncached and runs exactly once"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cache_visible_inside_same_closure() {
    let (tables, counters) = make_tables(small_cache());
    let header = monad_chain_data::EvmBlockHeader {
        number: 1,
        ..Default::default()
    };
    let header_ref = &header;

    tables
        .with_writes(|w| {
            Box::pin(async move {
                w.tables().blocks().stage_header(w, 1, header_ref);
                let read = w.tables().blocks().load_header(1).await?;
                assert!(read.is_some(), "populate visible inside closure");
                Ok(())
            })
        })
        .await
        .expect("with_writes");

    let inside_calls = counters.snapshot().0;
    assert_eq!(inside_calls, 0, "in-closure read must hit cache");
}

#[tokio::test(flavor = "current_thread")]
async fn cache_eviction_is_not_correctness_bug() {
    let cache = CacheConfig {
        block_header_entries: 2,
        ..small_cache()
    };
    let (tables, _counters) = make_tables(cache);

    for i in 1..=5u64 {
        let header = monad_chain_data::EvmBlockHeader {
            number: i,
            ..Default::default()
        };
        let header_ref = &header;
        tables
            .with_writes(|w| {
                Box::pin(async move {
                    w.tables().blocks().stage_header(w, i, header_ref);
                    Ok(())
                })
            })
            .await
            .expect("with_writes");
    }

    for i in 1..=5u64 {
        let v = tables.blocks().load_header(i).await.expect("load");
        assert!(v.is_some(), "value at block {i} must still be reachable");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn zero_size_cache_skips_lru() {
    let cache = CacheConfig {
        block_header_entries: 0,
        ..small_cache()
    };
    let (tables, counters) = make_tables(cache);
    let header = monad_chain_data::EvmBlockHeader {
        number: 1,
        ..Default::default()
    };
    let header_ref = &header;
    tables
        .with_writes(|w| {
            Box::pin(async move {
                w.tables().blocks().stage_header(w, 1, header_ref);
                Ok(())
            })
        })
        .await
        .expect("with_writes");

    let before = counters.snapshot();
    let _ = tables.blocks().load_header(1).await.expect("load");
    let after = counters.snapshot();
    assert_eq!(
        after.0.saturating_sub(before.0),
        1,
        "zero-size cache disables caching: read must hit backend"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn populate_keys_match_scan_get_keys() {
    let (tables, counters) = make_tables(small_cache());

    tables
        .with_writes(|w| {
            Box::pin(async move {
                let dir = w.tables().family(Family::Log).dir();
                dir.stage_block_fragment(w, 1, 0, 5);
                dir.stage_block_fragment(w, 2, 5, 7);
                Ok(())
            })
        })
        .await
        .expect("stage");

    let fam = tables.family(Family::Log);
    let before = counters.snapshot();
    let fragments = fam.load_bucket_fragments(0).await.expect("load");
    assert_eq!(fragments.len(), 2);
    let after = counters.snapshot();
    assert_eq!(
        after.1, before.1,
        "per-clustering scan_gets must observe ZERO backend calls"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_reader_during_populate() {
    let (tables, _counters) = make_tables(small_cache());
    let tables = Arc::new(tables);

    let tables_w = tables.clone();
    let writer = tokio::spawn(async move {
        for i in 1..=50u64 {
            let header = monad_chain_data::EvmBlockHeader {
                number: i,
                ..Default::default()
            };
            let header_ref = &header;
            tables_w
                .with_writes(|w| {
                    Box::pin(async move {
                        w.tables().blocks().stage_header(w, i, header_ref);
                        Ok(())
                    })
                })
                .await
                .expect("write");
        }
    });

    let tables_r = tables.clone();
    let reader = tokio::spawn(async move {
        for _ in 0..200 {
            let _ = tables_r.blocks().load_header(1).await;
            tokio::task::yield_now().await;
        }
    });

    writer.await.expect("writer");
    reader.await.expect("reader");
}

#[tokio::test(flavor = "current_thread")]
async fn cache_hit_ratio_metric_resets_between_windows() {
    let (tables, _counters) = make_tables(small_cache());
    let header = monad_chain_data::EvmBlockHeader {
        number: 1,
        ..Default::default()
    };
    let header_ref = &header;
    tables
        .with_writes(|w| {
            Box::pin(async move {
                w.tables().blocks().stage_header(w, 1, header_ref);
                Ok(())
            })
        })
        .await
        .expect("with_writes");

    for _ in 0..3 {
        let _ = tables.blocks().load_header(1).await;
    }
    let first = tables.take_cache_window_stats();
    let hits: u64 = first.iter().map(|(_, h, _)| *h).sum();
    assert!(hits >= 3, "first window must record at least 3 hits");

    let second = tables.take_cache_window_stats();
    assert!(
        second.is_empty(),
        "second take after no activity must be empty (counters reset)"
    );

    let _ = tables.blocks().load_header(1).await;
    let third = tables.take_cache_window_stats();
    let third_hits: u64 = third.iter().map(|(_, h, _)| *h).sum();
    assert_eq!(third_hits, 1, "third window observes exactly the new hit");
}

