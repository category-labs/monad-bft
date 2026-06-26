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

//! Page-outer / clause-inner intersection in `load_intersection_ids`,
//! including the per-page short-circuit on later clauses' fetches.

use bytes::Bytes as RawBytes;
use monad_query_tests::prelude::*;
/// Lossless `usize` view of the compile-time page span.
const PAGE_SPAN: usize = STREAM_PAGE_ID_SPAN as usize;

fn addr_clause(address: Address) -> IndexedClause {
    IndexedClause {
        kind: IndexKind::Addr,
        values: vec![RawBytes::copy_from_slice(address.as_slice())],
    }
}

fn topic0_clause(topic: Topic) -> IndexedClause {
    IndexedClause {
        kind: IndexKind::Topic0,
        values: vec![RawBytes::copy_from_slice(topic.as_slice())],
    }
}

#[tokio::test(flavor = "current_thread")]
async fn intersection_is_empty_when_clauses_match_in_disjoint_pages() {
    let addr_a = Address::repeat_byte(0xAA);
    let addr_b = Address::repeat_byte(0xBB);
    let topic = B256::repeat_byte(0x11);

    // addr_a fills page 0; addr_b spills into page 1.
    let mut logs = repeated_logs(addr_a, vec![topic], PAGE_SPAN);
    logs.extend(repeated_logs(addr_b, vec![topic], 8));
    let frontier_id = logs.len() as u64; // still in page group 0 => frontier group

    let store = populate::populate_via_engine(vec![block_with_logs(
        test_header(1, B256::ZERO),
        vec![logs],
    )])
    .await;
    let service = store.reader();

    let clauses = vec![addr_clause(addr_a), addr_clause(addr_b)];
    let family = service.tables().family(Family::Log);
    let result = family
        .load_intersection_ids(
            &clauses,
            frontier_id,
            0,
            u64::from(STREAM_PAGE_ID_SPAN) + 7,
            QueryOrder::Ascending,
        )
        .await
        .expect("load intersection");
    assert!(
        result.is_none(),
        "clauses matching in disjoint pages must intersect to None"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn intersection_keeps_only_ids_present_in_every_clause_per_page() {
    let addr = Address::repeat_byte(0xAA);
    let topic_shared = B256::repeat_byte(0x11);
    let topic_other = B256::repeat_byte(0x22);

    // Page 0: addr + topic_shared coincide. Page 1: addr + topic_other only.
    let mut logs = repeated_logs(addr, vec![topic_shared], PAGE_SPAN);
    logs.extend(repeated_logs(addr, vec![topic_other], 8));
    let frontier_id = logs.len() as u64;

    let store = populate::populate_via_engine(vec![block_with_logs(
        test_header(1, B256::ZERO),
        vec![logs],
    )])
    .await;
    let service = store.reader();

    let clauses = vec![addr_clause(addr), topic0_clause(topic_shared)];
    let family = service.tables().family(Family::Log);
    let result = family
        .load_intersection_ids(
            &clauses,
            frontier_id,
            0,
            u64::from(STREAM_PAGE_ID_SPAN) + 7,
            QueryOrder::Ascending,
        )
        .await
        .expect("load intersection")
        .expect("non-empty intersection");

    // Survivors are exactly page 0's ids [0, PAGE_SPAN), ascending.
    assert_eq!(result.len(), PAGE_SPAN);
    assert_eq!(result.first(), Some(&0));
    assert_eq!(result.last(), Some(&(u64::from(STREAM_PAGE_ID_SPAN) - 1)));
    assert!(
        result.iter().all(|&id| id < u64::from(STREAM_PAGE_ID_SPAN)),
        "no page-1 ids may survive"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn per_page_short_circuit_skips_later_clause_fetches() {
    let driver_addr = Address::repeat_byte(0xAA); // sparse: page 1 only
    let other_addr = Address::repeat_byte(0xBB); // dense: every page
    let topic = B256::repeat_byte(0x11);

    let logs: Vec<_> = (0..3)
        .flat_map(|page| {
            let address = if page == 1 { driver_addr } else { other_addr };
            repeated_logs(address, vec![topic], PAGE_SPAN)
        })
        .collect();
    let frontier_id = logs.len() as u64;

    let store = populate::populate_via_engine(vec![block_with_logs(
        test_header(1, B256::ZERO),
        vec![logs],
    )])
    .await;

    let counting = ObservedMetaStore::over(store.meta.clone());
    let service = MonadChainDataService::with_all_configs(
        counting.clone(),
        store.blob.clone(),
        QueryLimits::UNLIMITED,
        CacheConfig {
            bitmap_page_blob_cache_bytes: 0,
            ..CacheConfig::default()
        },
        DictConfig::default(),
        QueryRuntimeConfig::default(),
    );

    let page_blob_table = Family::Log.table_ids().bitmap_page_blob;
    let clauses = vec![addr_clause(driver_addr), addr_clause(other_addr)];
    let id_to = 3 * u64::from(STREAM_PAGE_ID_SPAN) - 1;

    counting.start_counting();
    let result = service
        .tables()
        .family(Family::Log)
        .load_intersection_ids(&clauses, frontier_id, 0, id_to, QueryOrder::Ascending)
        .await
        .expect("load intersection");
    let pruned_fetches = counting.get_calls(page_blob_table);

    assert!(result.is_none());
    // Driver fetched on all 3 pages; other_addr only on page 1 where the
    // driver is non-empty: 3 + 1 = 4, vs the naive 6.
    assert_eq!(
        pruned_fetches, 4,
        "expected 3 driver fetches + 1 second-clause fetch (page 1 only), got {pruned_fetches}"
    );

    // Reversed ordering yields 3 + 2 = 5 — a different shape, pinning the win
    // to the per-page short-circuit rather than caching.
    let clauses_rev = vec![addr_clause(other_addr), addr_clause(driver_addr)];
    counting.start_counting();
    let _ = service
        .tables()
        .family(Family::Log)
        .load_intersection_ids(&clauses_rev, frontier_id, 0, id_to, QueryOrder::Ascending)
        .await
        .expect("load intersection rev");
    let rev_fetches = counting.get_calls(page_blob_table);
    assert_eq!(
        rev_fetches, 5,
        "dense-first ordering skips the driver only on page 1, got {rev_fetches}"
    );
}

/// Encodes page-relative offsets into a bitmap blob + its meta.
fn page_bitmap_blob(offsets: &[u32]) -> (BitmapPageMeta, RawBytes) {
    let bitmap: roaring::RoaringBitmap = offsets.iter().copied().collect();
    let meta = BitmapPageMeta {
        min_offset: bitmap.min().expect("non-empty page"),
        max_offset: bitmap.max().expect("non-empty page"),
        count: bitmap.len() as u32,
    };
    let blob = DecodedBitmapFragment {
        min_offset: meta.min_offset,
        max_offset: meta.max_offset,
        count: meta.count,
        bitmap,
    };
    (meta, encode_bitmap_blob(&blob).expect("encode bitmap blob"))
}

/// A query on the OPEN page must not probe the page's (absent) artifact, and
/// the seal-tail bits — minted between the page's last flush and its seal,
/// so present ONLY in the sealed artifact, never in any fragment — must be
/// returned by a post-seal query through the SAME tables instance. A stale
/// cached artifact miss planted by the pre-seal query used to route post-seal
/// queries to the incomplete fragment union forever.
#[tokio::test(flavor = "current_thread")]
async fn open_page_query_then_seal_returns_seal_tail_ids() {
    let addr = Address::repeat_byte(0xAA);
    let stream = render_stream_id("addr", addr.as_slice());

    let counting = ObservedMetaStore::new();
    // Default cache config: page-blob cache ON, as in production readers.
    let service = MonadChainDataService::with_all_configs(
        counting.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
        CacheConfig::default(),
        DictConfig::default(),
        QueryRuntimeConfig::default(),
    );
    let t = service.tables();
    let family = t.family(Family::Log);
    let page_blob_table = Family::Log.table_ids().bitmap_page_blob;
    let clauses = vec![addr_clause(addr)];
    let page_span = u64::from(STREAM_PAGE_ID_SPAN);

    // Open page 0: one flushed fragment with offsets 0..10; frontier mid-page.
    let flushed: Vec<u32> = (0..10).collect();
    let (_, fragment_blob) = page_bitmap_blob(&flushed);
    seed_bitmap_page_fragment(t, Family::Log, &stream, 0, 1, fragment_blob).await;

    // Pre-seal query: an open page's bits all live in fragments, so the
    // artifact is never probed (nothing to plant a stale miss with).
    counting.start_counting();
    let ids = family
        .load_intersection_ids(&clauses, 10, 0, page_span - 1, QueryOrder::Ascending)
        .await
        .expect("pre-seal intersection")
        .expect("pre-seal ids");
    assert_eq!(ids, (0..10).collect::<Vec<u64>>());
    assert_eq!(
        counting.get_calls(page_blob_table),
        0,
        "an open page must never probe its artifact"
    );

    // Seal page 0: the artifact folds the flushed bits together with the
    // seal-tail bits 10..20, which never got a fragment.
    let sealed: Vec<u32> = (0..20).collect();
    let (meta, bitmap_blob) = page_bitmap_blob(&sealed);
    seed_bitmap_page_artifact(
        t,
        Family::Log,
        &stream,
        0,
        &BitmapPageArtifact { meta, bitmap_blob },
    )
    .await;

    // Post-seal query through the SAME tables: the sealed page reads its
    // artifact and the seal-tail ids appear.
    let ids = family
        .load_intersection_ids(&clauses, page_span, 0, page_span - 1, QueryOrder::Ascending)
        .await
        .expect("post-seal intersection")
        .expect("post-seal ids");
    assert_eq!(
        ids,
        (0..20).collect::<Vec<u64>>(),
        "post-seal query must return the seal-tail ids, which exist only in the artifact"
    );
}
