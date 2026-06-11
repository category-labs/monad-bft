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

//! Page-count manifests skip zero-count pages in sealed page groups only,
//! including across a group boundary into the frontier group.

use bytes::Bytes as RawBytes;
mod common;

use common::observed_store::ObservedMetaStore;
use monad_chain_data::{
    engine::{
        bitmap::{
            encode_bitmap_blob, render_stream_id, BitmapPageArtifact, BitmapPageCounts,
            BitmapPageMeta, DecodedBitmapFragment, IndexKind, PAGE_GROUP_ID_SPAN,
            STREAM_PAGE_ID_SPAN,
        },
        clause::IndexedClause,
        family::Family,
        tables::{DictConfig, QueryRuntimeConfig},
    },
    store::CacheConfig,
    Address, InMemoryBlobStore, MonadChainDataService, QueryLimits, QueryOrder,
};

const PAGE_SPAN: u64 = STREAM_PAGE_ID_SPAN as u64;

fn addr_clause(address: Address) -> IndexedClause {
    IndexedClause {
        kind: IndexKind::Addr,
        values: vec![RawBytes::copy_from_slice(address.as_slice())],
    }
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

fn page_artifact(offsets: &[u32]) -> BitmapPageArtifact {
    let (meta, bitmap_blob) = page_bitmap_blob(offsets);
    BitmapPageArtifact { meta, bitmap_blob }
}

fn page_start(page_idx: u32) -> u64 {
    u64::from(page_idx) * PAGE_SPAN
}

/// Seeds two clauses in page group 0: addr_a in pages 0 and 2, addr_b in page
/// 1, so every page has a zero-count clause and the intersection is empty.
async fn seed_disjoint_clauses(
    service: &MonadChainDataService<ObservedMetaStore, InMemoryBlobStore>,
    addr_a: Address,
    addr_b: Address,
) {
    let s_a = render_stream_id("addr", addr_a.as_slice());
    let s_b = render_stream_id("addr", addr_b.as_slice());

    let t = service.tables();
    common::seed_bitmap_page_artifact(t, Family::Log, &s_a, page_start(0), &page_artifact(&[1]))
        .await;
    common::seed_bitmap_page_artifact(t, Family::Log, &s_a, page_start(2), &page_artifact(&[1]))
        .await;
    common::seed_bitmap_page_artifact(t, Family::Log, &s_b, page_start(1), &page_artifact(&[1]))
        .await;
    common::seed_bitmap_page_counts(
        t,
        Family::Log,
        &s_a,
        0,
        &BitmapPageCounts::from_pairs([(page_start(0) as u32, 1), (page_start(2) as u32, 1)]),
    )
    .await;
    common::seed_bitmap_page_counts(
        t,
        Family::Log,
        &s_b,
        0,
        &BitmapPageCounts::from_pairs([(page_start(1) as u32, 1)]),
    )
    .await;
}

fn service_without_page_cache(
    counting: ObservedMetaStore,
) -> MonadChainDataService<ObservedMetaStore, InMemoryBlobStore> {
    MonadChainDataService::with_all_configs(
        counting,
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
        CacheConfig {
            bitmap_page_blob_cache_bytes: 0,
            ..CacheConfig::default()
        },
        DictConfig::default(),
        QueryRuntimeConfig::default(),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn sealed_group_skips_zero_count_pages_without_fetching() {
    let counting = ObservedMetaStore::new();
    let service = service_without_page_cache(counting.clone());
    let family = service.tables().family(Family::Log);

    let addr_a = Address::repeat_byte(0xAA);
    let addr_b = Address::repeat_byte(0xBB);
    seed_disjoint_clauses(&service, addr_a, addr_b).await;

    let page_blob_table = Family::Log.table_ids().bitmap_page_blob;
    let clauses = vec![addr_clause(addr_a), addr_clause(addr_b)];

    counting.start_counting();
    // Frontier in group 1 => group 0 is sealed and the manifest may skip.
    let result = family
        .load_intersection_ids(
            &clauses,
            PAGE_GROUP_ID_SPAN,
            0,
            3 * PAGE_SPAN - 1,
            QueryOrder::Ascending,
        )
        .await
        .expect("load intersection");
    let fetches = counting.get_calls(page_blob_table);

    assert!(result.is_none(), "disjoint clauses intersect to None");
    assert_eq!(
        fetches, 0,
        "sealed-group manifest must skip every zero-count page without fetching, got {fetches}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn frontier_group_never_skips_a_non_empty_page() {
    let counting = ObservedMetaStore::new();
    let service = service_without_page_cache(counting.clone());
    let family = service.tables().family(Family::Log);

    let addr_a = Address::repeat_byte(0xAA);
    let addr_b = Address::repeat_byte(0xBB);
    // Same data shape as the sealed test, manifests included, so the contrast
    // pins skipping to seal state rather than data shape.
    seed_disjoint_clauses(&service, addr_a, addr_b).await;

    let page_blob_table = Family::Log.table_ids().bitmap_page_blob;
    let clauses = vec![addr_clause(addr_a), addr_clause(addr_b)];

    counting.start_counting();
    // Frontier still inside group 0 => frontier group, no skipping allowed.
    // It sits above the three seeded pages, so each page is sealed and reads
    // its artifact rather than fragments.
    let frontier_id = 3 * PAGE_SPAN;
    let result = family
        .load_intersection_ids(
            &clauses,
            frontier_id,
            0,
            3 * PAGE_SPAN - 1,
            QueryOrder::Ascending,
        )
        .await
        .expect("load intersection");
    let fetches = counting.get_calls(page_blob_table);

    assert!(result.is_none(), "disjoint clauses intersect to None");
    assert!(
        fetches >= 3,
        "frontier group must fetch (never skip) non-empty pages, got {fetches}"
    );
}

/// A query crossing a page-group boundary: the last page of sealed group 0
/// (artifact + manifest), the first page of frontier group 1 (open-page
/// fragment, no manifest), plus a manifest-proven-empty group-0 page in
/// range. Both orders return exactly the right ids; the empty sealed page is
/// skipped with zero fetches while the open frontier page reads its
/// fragments without probing the artifact table.
#[tokio::test(flavor = "current_thread")]
async fn query_crossing_group_boundary_returns_exact_ids_in_both_orders() {
    let counting = ObservedMetaStore::new();
    let service = service_without_page_cache(counting.clone());
    let family = service.tables().family(Family::Log);

    let addr = Address::repeat_byte(0xAA);
    let stream = render_stream_id("addr", addr.as_slice());
    let t = service.tables();

    // Sealed group 0: its LAST page holds offsets {1, 5}; the page before it
    // is empty (absent from the manifest).
    let last_page_of_group0 = PAGE_GROUP_ID_SPAN - PAGE_SPAN;
    common::seed_bitmap_page_artifact(
        t,
        Family::Log,
        &stream,
        last_page_of_group0,
        &page_artifact(&[1, 5]),
    )
    .await;
    common::seed_bitmap_page_counts(
        t,
        Family::Log,
        &stream,
        0,
        &BitmapPageCounts::from_pairs([((last_page_of_group0 % PAGE_GROUP_ID_SPAN) as u32, 2)]),
    )
    .await;

    // Frontier group 1: the first page is open — a flush fragment with
    // offset {2}, no artifact, no manifest.
    let first_page_of_group1 = PAGE_GROUP_ID_SPAN;
    let (_, fragment_blob) = page_bitmap_blob(&[2]);
    common::seed_bitmap_page_fragment(
        t,
        Family::Log,
        &stream,
        first_page_of_group1,
        7,
        fragment_blob,
    )
    .await;

    let frontier_id = PAGE_GROUP_ID_SPAN + 100; // inside group 1
    let id_from = last_page_of_group0 - PAGE_SPAN; // includes an empty sealed page
    let id_to = PAGE_GROUP_ID_SPAN + PAGE_SPAN - 1;
    let expected_ascending = vec![
        last_page_of_group0 + 1,
        last_page_of_group0 + 5,
        first_page_of_group1 + 2,
    ];

    let page_blob_table = Family::Log.table_ids().bitmap_page_blob;
    let clauses = vec![addr_clause(addr)];
    for (order, expected) in [
        (QueryOrder::Ascending, expected_ascending.clone()),
        (
            QueryOrder::Descending,
            expected_ascending.iter().rev().copied().collect(),
        ),
    ] {
        counting.start_counting();
        let ids = family
            .load_intersection_ids(&clauses, frontier_id, id_from, id_to, order)
            .await
            .expect("load intersection")
            .expect("non-empty intersection");
        assert_eq!(ids, expected, "{order:?}");
        // The empty sealed page is manifest-skipped and the open frontier
        // page never probes its artifact: one artifact get, for the sealed
        // non-empty page only.
        assert_eq!(
            counting.get_calls(page_blob_table),
            1,
            "{order:?}: only the sealed non-empty page may fetch its artifact"
        );
    }
}
