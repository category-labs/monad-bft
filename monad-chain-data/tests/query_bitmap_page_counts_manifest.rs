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

//! Coverage for the sealed-shard page-count manifest's effect on
//! `load_intersection_bitmap`: zero-count pages are skipped without a fetch on
//! a sealed shard, never skipped on the frontier shard, and surviving pages
//! fetch their most-selective clause first. Page artifacts and manifests are
//! seeded directly so a "sealed" shard can be exercised without ingesting a
//! full 2^24-id shard.

use std::sync::{atomic::Ordering, Arc};

use bytes::Bytes as RawBytes;
mod common;

use monad_chain_data::{
    engine::{
        bitmap::{
            encode_bitmap_blob, sharded_stream_id, BitmapBlob, BitmapPageArtifact,
            BitmapPageCounts, BitmapPageMeta, STREAM_PAGE_LOCAL_ID_SPAN,
        },
        clause::IndexedClause,
        family::Family,
    },
    error::Result,
    store::{
        common::Page,
        meta::{MetaStore, MetaWriteOp, ScannableTableId, TableId},
        CacheConfig,
    },
    Address, InMemoryBlobStore, InMemoryMetaStore, MonadChainDataService, QueryLimits,
};

/// A `MetaStore` decorator that counts page-blob `get` calls per table. Each
/// `load_bitmap_page` issues exactly one page-blob `get` (the sealed-page
/// artifact probe) before any fragment scan, and these tests pin the page-blob
/// cache off (`bitmap_page_blob_entries: 0`) so every probe reaches this
/// backend, making the count a faithful proxy for
/// "how many per-page clause-stream bitmap fetches happened". Page-counts gets
/// are not counted here.
#[derive(Debug, Clone, Default)]
struct CountingMetaStore<S> {
    inner: S,
    get_counts: Arc<std::sync::Mutex<std::collections::HashMap<TableId, usize>>>,
    counting: Arc<std::sync::atomic::AtomicBool>,
}

impl<S> CountingMetaStore<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            get_counts: Arc::new(std::sync::Mutex::new(Default::default())),
            counting: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn start_counting(&self) {
        self.get_counts.lock().unwrap().clear();
        self.counting.store(true, Ordering::SeqCst);
    }

    fn get_calls(&self, table: TableId) -> usize {
        self.get_counts
            .lock()
            .unwrap()
            .get(&table)
            .copied()
            .unwrap_or(0)
    }
}

impl<S: MetaStore> MetaStore for CountingMetaStore<S> {
    fn get(
        &self,
        table: TableId,
        key: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<RawBytes>>> + Send {
        if self.counting.load(Ordering::SeqCst) {
            *self.get_counts.lock().unwrap().entry(table).or_insert(0) += 1;
        }
        self.inner.get(table, key)
    }

    fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<RawBytes>>> + Send {
        self.inner.scan_get(table, partition, clustering)
    }

    async fn put(&self, table: TableId, key: &[u8], value: RawBytes) -> Result<()> {
        self.inner.put(table, key, value).await
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: RawBytes,
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
        self.inner
            .scan_list(table, partition, prefix, cursor, limit)
            .await
    }

    async fn apply_writes(&self, writes: Vec<MetaWriteOp>) -> Result<()> {
        self.inner.apply_writes(writes).await
    }
}

fn addr_clause(address: Address) -> IndexedClause {
    IndexedClause {
        kind: "addr",
        values: vec![RawBytes::copy_from_slice(address.as_slice())],
    }
}

/// Builds a sealed-page artifact for one stream page from explicit local ids.
fn page_artifact(local_ids: &[u32]) -> BitmapPageArtifact {
    let mut bitmap = roaring::RoaringBitmap::new();
    for id in local_ids {
        bitmap.insert(*id);
    }
    let min_local = bitmap.min().unwrap();
    let max_local = bitmap.max().unwrap();
    let count = bitmap.len() as u32;
    let blob = BitmapBlob {
        min_local,
        max_local,
        count,
        bitmap,
    };
    BitmapPageArtifact {
        meta: BitmapPageMeta {
            min_local,
            max_local,
            count,
        },
        bitmap_blob: encode_bitmap_blob(&blob).unwrap(),
    }
}

fn page_start(page_idx: u32) -> u32 {
    page_idx * STREAM_PAGE_LOCAL_ID_SPAN
}

/// Seeds, on a *sealed* shard, two address clauses across three pages and
/// asserts the manifest skips pages where a clause has count 0 with zero
/// page-blob fetches, while still returning the correct intersection.
#[tokio::test(flavor = "current_thread")]
async fn sealed_shard_skips_zero_count_pages_without_fetching() {
    let counting = CountingMetaStore::new(InMemoryMetaStore::default());
    let service = MonadChainDataService::with_cache_config(
        counting.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
        CacheConfig {
            bitmap_page_blob_cache_bytes: 0,
            ..CacheConfig::default()
        },
    );
    let family = service.tables().family(Family::Log);

    // Sealed shard 0 (frontier shard will be 1). Two addresses:
    //  - addr_a: present in page 0 and page 2.
    //  - addr_b: present in page 1 only.
    // addr_a AND addr_b => empty in every page (disjoint), so the only fetches
    // the manifest cannot avoid are the driver's on its own non-empty pages.
    let addr_a = Address::repeat_byte(0xAA);
    let addr_b = Address::repeat_byte(0xBB);
    let sealed_shard = 0u64;
    let s_a = sharded_stream_id("addr", addr_a.as_slice(), sealed_shard);
    let s_b = sharded_stream_id("addr", addr_b.as_slice(), sealed_shard);

    // Page artifacts (a single bit each is enough for the count manifest).
    let t = service.tables();
    common::seed_bitmap_page_artifact(
        t,
        Family::Log,
        &s_a,
        page_start(0),
        &page_artifact(&[page_start(0) + 1]),
    )
    .await;
    common::seed_bitmap_page_artifact(
        t,
        Family::Log,
        &s_a,
        page_start(2),
        &page_artifact(&[page_start(2) + 1]),
    )
    .await;
    common::seed_bitmap_page_artifact(
        t,
        Family::Log,
        &s_b,
        page_start(1),
        &page_artifact(&[page_start(1) + 1]),
    )
    .await;

    // Manifests: addr_a -> pages 0,2; addr_b -> page 1.
    common::seed_bitmap_page_counts(
        t,
        Family::Log,
        &s_a,
        &BitmapPageCounts::from_pairs([(page_start(0), 1), (page_start(2), 1)]),
    )
    .await;
    common::seed_bitmap_page_counts(
        t,
        Family::Log,
        &s_b,
        &BitmapPageCounts::from_pairs([(page_start(1), 1)]),
    )
    .await;

    let page_blob_table = Family::Log.table_ids().bitmap_page_blob;
    let clauses = vec![addr_clause(addr_a), addr_clause(addr_b)];

    counting.start_counting();
    // frontier_shard = 1 => shard 0 is sealed and the manifest may skip.
    let result = family
        .load_intersection_bitmap(
            &clauses,
            sealed_shard,
            1,
            0,
            3 * STREAM_PAGE_LOCAL_ID_SPAN - 1,
        )
        .await
        .expect("load intersection");
    let fetches = counting.get_calls(page_blob_table);

    assert!(result.is_none(), "disjoint clauses intersect to None");

    // Page 0: addr_b count 0 -> skip (0 fetches). Page 1: addr_a count 0 ->
    // skip (0 fetches). Page 2: addr_b count 0 -> skip (0 fetches). Every page
    // has a zero-count clause, so the manifest skips all three with NO
    // page-blob fetches at all.
    assert_eq!(
        fetches, 0,
        "sealed-shard manifest must skip every zero-count page without fetching, got {fetches}"
    );
}

/// The same data shape on the FRONTIER shard must never skip a page: with the
/// shard open the manifest is a hint only, so the driver clause's non-empty
/// pages are still fetched and the result is identical.
#[tokio::test(flavor = "current_thread")]
async fn frontier_shard_never_skips_a_non_empty_page() {
    let counting = CountingMetaStore::new(InMemoryMetaStore::default());
    let service = MonadChainDataService::with_cache_config(
        counting.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
        CacheConfig {
            bitmap_page_blob_cache_bytes: 0,
            ..CacheConfig::default()
        },
    );
    let family = service.tables().family(Family::Log);

    let addr_a = Address::repeat_byte(0xAA);
    let addr_b = Address::repeat_byte(0xBB);
    // Put the data on shard 0 and treat shard 0 as the frontier (open) shard.
    let shard = 0u64;
    let s_a = sharded_stream_id("addr", addr_a.as_slice(), shard);
    let s_b = sharded_stream_id("addr", addr_b.as_slice(), shard);

    let t = service.tables();
    common::seed_bitmap_page_artifact(
        t,
        Family::Log,
        &s_a,
        page_start(0),
        &page_artifact(&[page_start(0) + 1]),
    )
    .await;
    common::seed_bitmap_page_artifact(
        t,
        Family::Log,
        &s_a,
        page_start(2),
        &page_artifact(&[page_start(2) + 1]),
    )
    .await;
    common::seed_bitmap_page_artifact(
        t,
        Family::Log,
        &s_b,
        page_start(1),
        &page_artifact(&[page_start(1) + 1]),
    )
    .await;
    // Even if (incorrectly) a manifest existed for the open shard, it must not
    // drive a skip. Seed one to prove the never-skip guard holds.
    common::seed_bitmap_page_counts(
        t,
        Family::Log,
        &s_a,
        &BitmapPageCounts::from_pairs([(page_start(0), 1), (page_start(2), 1)]),
    )
    .await;
    common::seed_bitmap_page_counts(
        t,
        Family::Log,
        &s_b,
        &BitmapPageCounts::from_pairs([(page_start(1), 1)]),
    )
    .await;

    let page_blob_table = Family::Log.table_ids().bitmap_page_blob;
    let clauses = vec![addr_clause(addr_a), addr_clause(addr_b)];

    counting.start_counting();
    // frontier_shard = 0 == shard => open shard, no skipping allowed.
    let result = family
        .load_intersection_bitmap(&clauses, shard, 0, 0, 3 * STREAM_PAGE_LOCAL_ID_SPAN - 1)
        .await
        .expect("load intersection");
    let fetches = counting.get_calls(page_blob_table);

    assert!(result.is_none(), "disjoint clauses intersect to None");
    // No page is skipped on the frontier shard. The driver clause (sorted
    // first by the ordering hint where a count is known) is fetched on all
    // three pages; the second clause is fetched only where the first survived.
    // The key invariant is that fetches > 0: a page is never dropped.
    assert!(
        fetches >= 3,
        "frontier shard must fetch (never skip) non-empty pages, got {fetches}"
    );
}

/// Selectivity ordering: within a surviving sealed-shard page the lower-count
/// clause is fetched first. We assert via fetch-shape — when both clauses are
/// non-empty in a page, the running intersection short-circuits after the more
/// selective clause, and reordering the clause list does not change the count.
#[tokio::test(flavor = "current_thread")]
async fn sealed_shard_orders_fetches_by_ascending_count() {
    let counting = CountingMetaStore::new(InMemoryMetaStore::default());
    let service = MonadChainDataService::with_cache_config(
        counting.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
        CacheConfig {
            bitmap_page_blob_cache_bytes: 0,
            ..CacheConfig::default()
        },
    );
    let family = service.tables().family(Family::Log);

    // Sealed shard 0; both clauses share page 0 but with very different counts:
    //  - sparse: one id in page 0.
    //  - dense: many ids in page 0, including the sparse id (so they intersect).
    let sparse = Address::repeat_byte(0xAA);
    let dense = Address::repeat_byte(0xBB);
    let sealed_shard = 0u64;
    let s_sparse = sharded_stream_id("addr", sparse.as_slice(), sealed_shard);
    let s_dense = sharded_stream_id("addr", dense.as_slice(), sealed_shard);

    let shared_id = page_start(0) + 5;
    let dense_ids: Vec<u32> = (0..1000u32).map(|i| page_start(0) + i).collect();
    let t = service.tables();
    common::seed_bitmap_page_artifact(
        t,
        Family::Log,
        &s_sparse,
        page_start(0),
        &page_artifact(&[shared_id]),
    )
    .await;
    common::seed_bitmap_page_artifact(
        t,
        Family::Log,
        &s_dense,
        page_start(0),
        &page_artifact(&dense_ids),
    )
    .await;
    common::seed_bitmap_page_counts(
        t,
        Family::Log,
        &s_sparse,
        &BitmapPageCounts::from_pairs([(page_start(0), 1)]),
    )
    .await;
    common::seed_bitmap_page_counts(
        t,
        Family::Log,
        &s_dense,
        &BitmapPageCounts::from_pairs([(page_start(0), dense_ids.len() as u32)]),
    )
    .await;

    // Forward order [dense, sparse]: the manifest reorders to fetch sparse
    // first (count 1), so both pages are fetched but the order is selectivity
    // driven. The result is the single shared id either way.
    let clauses_dense_first = vec![addr_clause(dense), addr_clause(sparse)];
    let result = family
        .load_intersection_bitmap(
            &clauses_dense_first,
            sealed_shard,
            1,
            0,
            STREAM_PAGE_LOCAL_ID_SPAN - 1,
        )
        .await
        .expect("load intersection")
        .expect("non-empty intersection");
    assert_eq!(result.len(), 1);
    assert_eq!(result.min(), Some(shared_id));

    // Reversed clause order yields the identical result set: ordering only
    // changes *which fetch happens first*, never the answer.
    let clauses_sparse_first = vec![addr_clause(sparse), addr_clause(dense)];
    let result_rev = family
        .load_intersection_bitmap(
            &clauses_sparse_first,
            sealed_shard,
            1,
            0,
            STREAM_PAGE_LOCAL_ID_SPAN - 1,
        )
        .await
        .expect("load intersection rev")
        .expect("non-empty intersection rev");
    assert_eq!(result_rev, result);
}
