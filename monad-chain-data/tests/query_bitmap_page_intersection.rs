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

//! Direct unit-style coverage for the page-outer / clause-inner intersection
//! in `engine::query::bitmap::load_intersection_bitmap`, including evidence
//! that the per-page short-circuit skips later clauses' fetches.

use std::sync::{atomic::Ordering, Arc};

use bytes::Bytes as RawBytes;
use monad_chain_data::{
    engine::{bitmap::STREAM_PAGE_LOCAL_ID_SPAN, clause::IndexedClause, family::Family},
    error::Result,
    store::{
        common::Page,
        meta::{
            CasOutcome, CasVersion, MetaStore, MetaStoreCas, MetaWriteOp, PublicationCasParams,
            ScannableTableId, TableId,
        },
    },
    Address, Bytes, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log, LogData,
    MonadChainDataService, QueryLimits, Topic, B256,
};

mod common;

use common::test_header;

/// A `MetaStore` decorator that counts `get` calls per kv table. Every
/// `load_bitmap_page` call begins with exactly one `get` on the family's
/// `*_bitmap_page_blob` table (the sealed-page artifact probe) before it
/// decides whether to also scan open fragments — and those page-blob gets
/// bypass the cache by default (`bitmap_page_blob_entries == 0`). Counting
/// page-blob gets is therefore a sealing-independent proxy for "how many
/// per-page clause-stream fetches actually happened": one per
/// `load_bitmap_page`, regardless of whether the page is sealed or open.
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

    async fn apply_writes_with_cas(
        &self,
        writes: Vec<MetaWriteOp>,
        cas: PublicationCasParams,
    ) -> Result<CasOutcome> {
        self.inner.apply_writes_with_cas(writes, cas).await
    }
}

impl<S: MetaStoreCas> MetaStoreCas for CountingMetaStore<S> {
    async fn cas_get(&self, table: TableId, key: &[u8]) -> Result<Option<(CasVersion, RawBytes)>> {
        self.inner.cas_get(table, key).await
    }

    async fn cas_put(
        &self,
        table: TableId,
        key: &[u8],
        expected: Option<CasVersion>,
        value: RawBytes,
    ) -> Result<CasOutcome> {
        self.inner.cas_put(table, key, expected, value).await
    }
}

fn log(address: Address, topics: Vec<Topic>) -> Log {
    Log {
        address,
        data: LogData::new_unchecked(topics, Bytes::from(vec![1, 2, 3])),
    }
}

fn addr_clause(address: Address) -> IndexedClause {
    IndexedClause {
        kind: "addr",
        values: vec![RawBytes::copy_from_slice(address.as_slice())],
    }
}

fn topic0_clause(topic: Topic) -> IndexedClause {
    IndexedClause {
        kind: "topic0",
        values: vec![RawBytes::copy_from_slice(topic.as_slice())],
    }
}

/// Two clauses whose matches live in disjoint pages: per page the intersection
/// is empty, so the whole-shard intersection is empty (`None`).
#[tokio::test(flavor = "current_thread")]
async fn intersection_is_empty_when_clauses_match_in_disjoint_pages() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let addr_a = Address::repeat_byte(0xAA);
    let addr_b = Address::repeat_byte(0xBB);
    let topic = B256::repeat_byte(0x11);

    // Block 1: enough logs to fill page 0 and spill into page 1. Page 0 carries
    // `addr_a` matches; page 1 carries `addr_b` matches. The shared topic spans
    // both pages, but the two address clauses never coincide within a page.
    let page_span = usize::try_from(STREAM_PAGE_LOCAL_ID_SPAN).expect("page span fits usize");
    let mut logs = Vec::with_capacity(page_span + 8);
    for _ in 0..page_span {
        logs.push(log(addr_a, vec![topic]));
    }
    for _ in 0..8 {
        logs.push(log(addr_b, vec![topic]));
    }

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![logs],
            txs: Vec::new(),
            traces: vec![],
        })
        .await
        .expect("ingest block 1");

    // addr_a (page 0 only) AND addr_b (page 1 only): disjoint per page => empty.
    let clauses = vec![addr_clause(addr_a), addr_clause(addr_b)];
    let family = service.tables().family(Family::Log);
    // Cover both pages of the shard.
    let result = family
        .load_intersection_bitmap(&clauses, 0, 0, 0, STREAM_PAGE_LOCAL_ID_SPAN + 7)
        .await
        .expect("load intersection");
    assert!(
        result.is_none(),
        "clauses matching in disjoint pages must intersect to None"
    );
}

/// A clause that coincides with the driver clause in some pages but not others.
/// The surviving set must be exactly the ids where both clauses match.
#[tokio::test(flavor = "current_thread")]
async fn intersection_keeps_only_ids_present_in_every_clause_per_page() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let addr = Address::repeat_byte(0xAA);
    let topic_shared = B256::repeat_byte(0x11);
    let topic_other = B256::repeat_byte(0x22);

    // Page 0: every log has addr + topic_shared (clauses coincide here).
    // Page 1: logs have addr but topic_other (addr clause matches, topic clause
    // does not) => page 1 contributes nothing to addr AND topic_shared.
    let page_span = usize::try_from(STREAM_PAGE_LOCAL_ID_SPAN).expect("page span fits usize");
    let mut logs = Vec::with_capacity(page_span + 8);
    for _ in 0..page_span {
        logs.push(log(addr, vec![topic_shared]));
    }
    for _ in 0..8 {
        logs.push(log(addr, vec![topic_other]));
    }

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![logs],
            txs: Vec::new(),
            traces: vec![],
        })
        .await
        .expect("ingest block 1");

    let clauses = vec![addr_clause(addr), topic0_clause(topic_shared)];
    let family = service.tables().family(Family::Log);
    let result = family
        .load_intersection_bitmap(&clauses, 0, 0, 0, STREAM_PAGE_LOCAL_ID_SPAN + 7)
        .await
        .expect("load intersection")
        .expect("non-empty intersection");

    // Surviving ids are exactly page 0's local ids [0, page_span).
    assert_eq!(result.len() as usize, page_span);
    assert_eq!(result.min(), Some(0));
    assert_eq!(result.max(), Some(STREAM_PAGE_LOCAL_ID_SPAN - 1));
    assert!(
        result.iter().all(|id| id < STREAM_PAGE_LOCAL_ID_SPAN),
        "no page-1 ids may survive"
    );
}

/// Fetch-reduction evidence: when the driver (first) clause is empty in a page,
/// the second clause's page fetch for that page is skipped. We count page-blob
/// `get`s — one per `load_bitmap_page`, independent of whether a page sealed —
/// and drive a case where clause[0] matches in only one of three pages.
#[tokio::test(flavor = "current_thread")]
async fn per_page_short_circuit_skips_later_clause_fetches() {
    let counting = CountingMetaStore::new(InMemoryMetaStore::default());
    let service = MonadChainDataService::new(
        counting.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let driver_addr = Address::repeat_byte(0xAA); // sparse: page 1 only
    let other_addr = Address::repeat_byte(0xBB); // dense: every page
    let topic = B256::repeat_byte(0x11);

    let page_span = usize::try_from(STREAM_PAGE_LOCAL_ID_SPAN).expect("page span fits usize");
    // Three pages worth of `other_addr`, with `driver_addr` appearing only in
    // page 1.
    let mut logs = Vec::with_capacity(3 * page_span);
    for page in 0..3usize {
        for _ in 0..page_span {
            let address = if page == 1 { driver_addr } else { other_addr };
            logs.push(log(address, vec![topic]));
        }
    }

    // Split across a few blocks to keep each ingest reasonable, but all on the
    // open frontier so nothing seals.
    let h1 = test_header(1, B256::ZERO);
    service
        .ingest_block(FinalizedBlock {
            header: h1.clone(),
            logs_by_tx: vec![logs],
            txs: Vec::new(),
            traces: vec![],
        })
        .await
        .expect("ingest block 1");

    let page_blob_table = Family::Log.table_ids().bitmap_page_blob;

    // clause order [driver_addr, other_addr]: driver is empty in pages 0 and 2,
    // so other_addr's page fetch for those pages must be skipped.
    let clauses = vec![addr_clause(driver_addr), addr_clause(other_addr)];

    counting.start_counting();
    let result = service
        .tables()
        .family(Family::Log)
        .load_intersection_bitmap(&clauses, 0, 0, 0, 3 * STREAM_PAGE_LOCAL_ID_SPAN - 1)
        .await
        .expect("load intersection");
    let pruned_fetches = counting.get_calls(page_blob_table);

    // Intersection is empty (driver and other never share a page).
    assert!(result.is_none());

    // Baseline: the old clause-outer loop fetched every clause's page for every
    // page in the shard range, i.e. 2 clauses x 3 pages = 6 page fetches. The
    // page-outer loop fetches the driver on all 3 pages, but `other_addr` only
    // on page 1 (the single page where the driver is non-empty); the driver's
    // empty result on pages 0 and 2 short-circuits those, so 3 + 1 = 4.
    assert_eq!(
        pruned_fetches, 4,
        "expected 3 driver fetches + 1 second-clause fetch (page 1 only), got {pruned_fetches}"
    );

    // Sanity: with the clauses reversed so the dense clause drives, the second
    // (driver) clause is fetched on the two pages where `other_addr` is
    // non-empty (pages 0 and 2), but skipped on page 1: 3 + 2 = 5 — still under
    // the clause-outer baseline of 6, and a different shape than the forward
    // run, pinning the win to the per-page short-circuit rather than caching.
    let clauses_rev = vec![addr_clause(other_addr), addr_clause(driver_addr)];
    counting.start_counting();
    let _ = service
        .tables()
        .family(Family::Log)
        .load_intersection_bitmap(&clauses_rev, 0, 0, 0, 3 * STREAM_PAGE_LOCAL_ID_SPAN - 1)
        .await
        .expect("load intersection rev");
    let rev_fetches = counting.get_calls(page_blob_table);
    assert_eq!(
        rev_fetches, 5,
        "dense-first ordering skips the driver only on page 1, got {rev_fetches}"
    );
}
