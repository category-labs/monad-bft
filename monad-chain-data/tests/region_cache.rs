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

//! Integration tests for the in-memory compressed per-(family, block) region
//! cache on the query read path. These exercise `Tables` directly with crafted
//! blob objects + headers so the assertions stay deterministic and independent
//! of the row codec: the cache slices raw frame bytes, it never decodes them.

use monad_chain_data::{
    engine::tables::Tables, store::CacheConfig, BlockBlobHeader, Family, InMemoryBlobStore,
    InMemoryMetaStore,
};

mod common;

/// Builds a `Tables` with a generous region byte budget so nothing is evicted
/// mid-test, and returns the blob fixture so the test can read its fetch count.
fn tables_with_region_cache(
    budget_bytes: usize,
) -> (Tables<InMemoryMetaStore, InMemoryBlobStore>, InMemoryBlobStore) {
    let blob = InMemoryBlobStore::default();
    let cache = CacheConfig {
        // Everything but the region cache disabled: this test only asserts on
        // region-cache fetch counts, and zero entries keeps the other caches
        // from masking backend reads.
        dir_by_block_entries: 0,
        dir_bucket_entries: 0,
        bitmap_by_block_entries: 0,
        bitmap_page_blob_entries: 0,
        bitmap_page_counts_entries: 0,
        open_bitmap_stream_entries: 0,
        block_header_entries: 0,
        block_hash_to_number_entries: 0,
        tx_hash_index_entries: 0,
        block_region_cache_bytes: budget_bytes,
        // The oversized-region test relies on this 1 MiB cap.
        block_region_max_bytes: 1024 * 1024,
    };
    let tables = Tables::with_cache_config(InMemoryMetaStore::default(), blob.clone(), cache);
    (tables, blob)
}

/// Stages a single-family shared blob object whose only region is `region`
/// (placed at `base_offset == 0`, i.e. the log slot) and returns a matching
/// header. The frame layout is two rows of `frame_len` bytes each.
async fn stage_region(
    tables: &Tables<InMemoryMetaStore, InMemoryBlobStore>,
    block_number: u64,
    region: Vec<u8>,
    declared_offsets: Vec<u32>,
) -> BlockBlobHeader {
    tables
        .with_writes(|w| {
            let region = region.clone();
            Box::pin(async move {
                tables.stage_block_blob(w, block_number, region);
                Ok(())
            })
        })
        .await
        .expect("stage shared blob");
    BlockBlobHeader {
        offsets: declared_offsets,
        dict_version: 0,
        base_offset: 0,
    }
}

/// (a) two point reads of DIFFERENT rows in the same block coalesce onto ONE
/// backend fetch, and (b) the bytes returned match the uncached single-frame
/// path exactly.
#[tokio::test(flavor = "current_thread")]
async fn point_reads_share_one_region_fetch_and_match_uncached() {
    let (tables, blob) = tables_with_region_cache(1 << 20);

    // Region = [ row0 (4 bytes) | row1 (5 bytes) ]; offsets carry the trailing
    // sentinel == region length 9.
    let region = vec![0xa0, 0xa1, 0xa2, 0xa3, 0xb0, 0xb1, 0xb2, 0xb3, 0xb4];
    let offsets = vec![0u32, 4, 9];
    let header = stage_region(&tables, 7, region.clone(), offsets.clone()).await;

    let before = blob.get_blob_calls();
    // First point read populates the region cache (one fetch).
    let row0 = tables
        .read_block_blob_frame(Family::Log, 7, &header, 0)
        .await
        .expect("frame 0")
        .expect("frame 0 present");
    // Second point read of a DIFFERENT row hits the cached region (no fetch).
    let row1 = tables
        .read_block_blob_frame(Family::Log, 7, &header, 1)
        .await
        .expect("frame 1")
        .expect("frame 1 present");
    let fetches = blob.get_blob_calls() - before;
    assert_eq!(
        fetches, 1,
        "two point reads in one block must coalesce onto a single region fetch"
    );

    // (b) Identical to the raw uncached single-frame ranges.
    assert_eq!(row0.as_ref(), &region[0..4]);
    assert_eq!(row1.as_ref(), &region[4..9]);
    let raw0 = tables
        .read_block_blob_range(7, 0, 4)
        .await
        .expect("raw range 0")
        .expect("raw range 0 present");
    let raw1 = tables
        .read_block_blob_range(7, 4, 9)
        .await
        .expect("raw range 1")
        .expect("raw range 1 present");
    assert_eq!(row0.as_ref(), raw0.as_ref());
    assert_eq!(row1.as_ref(), raw1.as_ref());

    // A whole-region read is served from the same cached entry: still no new
    // fetch beyond the one already counted.
    let before_region = blob.get_blob_calls();
    let whole = tables
        .read_block_blob_region(Family::Log, 7, &header)
        .await
        .expect("region")
        .expect("region present");
    assert_eq!(whole.as_ref(), region.as_slice());
    assert_eq!(
        blob.get_blob_calls() - before_region,
        0,
        "region read must be served from the populated cache"
    );
}

/// (c) a region larger than `REGION_CACHE_MAX_BYTES` (1 MiB) is NOT cached:
/// each point read falls back to a single-frame range read, so two reads issue
/// two fetches rather than coalescing.
#[tokio::test(flavor = "current_thread")]
async fn oversized_region_is_not_cached_and_falls_back_to_single_frame() {
    let (tables, blob) = tables_with_region_cache(1 << 20);

    // Tiny object, but a header whose sentinel CLAIMS a region far above the
    // 1 MiB cap. The large-region point-read path uses absolute ranges
    // (`abs_range`), which only touch the bytes that actually exist, so the
    // object itself stays small while the region is treated as oversized.
    let region = vec![0xc0, 0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7];
    let oversized_sentinel = (2 * 1024 * 1024) as u32;
    let offsets = vec![0u32, 4, oversized_sentinel];
    let header = stage_region(&tables, 9, region.clone(), offsets).await;

    let before = blob.get_blob_calls();
    let row0 = tables
        .read_block_blob_frame(Family::Log, 9, &header, 0)
        .await
        .expect("frame 0")
        .expect("frame 0 present");
    let row0_again = tables
        .read_block_blob_frame(Family::Log, 9, &header, 0)
        .await
        .expect("frame 0 again")
        .expect("frame 0 again present");
    let fetches = blob.get_blob_calls() - before;
    assert_eq!(
        fetches, 2,
        "oversized region must NOT be cached: each point read does its own single-frame fetch"
    );
    assert_eq!(row0.as_ref(), &region[0..4]);
    assert_eq!(row0_again.as_ref(), &region[0..4]);

    // The per-family region cache recorded only misses (no resident entry was
    // ever admitted for the oversized region).
    let stats = tables.take_cache_window_stats();
    let region_hits: u64 = stats
        .iter()
        .filter(|(name, _, _)| name.starts_with("block_region"))
        .map(|(_, h, _)| *h)
        .sum();
    assert_eq!(region_hits, 0, "oversized region must never produce a cache hit");
}

/// Three point reads of distinct rows in the SAME block resolve to ONE backend
/// fetch in total: the first populates the region, the rest are served from it
/// (or, under true overlap, coalesce onto the same in-flight fetch). Either way
/// the backend sees exactly one read.
#[tokio::test(flavor = "current_thread")]
async fn many_point_reads_in_one_block_issue_one_fetch() {
    let (tables, blob) = tables_with_region_cache(1 << 20);
    let region = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let offsets = vec![0u32, 3, 6, 10];
    let header = stage_region(&tables, 3, region.clone(), offsets).await;

    let before = blob.get_blob_calls();
    let (r0, r1, r2) = tokio::join!(
        tables.read_block_blob_frame(Family::Log, 3, &header, 0),
        tables.read_block_blob_frame(Family::Log, 3, &header, 1),
        tables.read_block_blob_frame(Family::Log, 3, &header, 2),
    );
    assert_eq!(r0.unwrap().unwrap().as_ref(), &region[0..3]);
    assert_eq!(r1.unwrap().unwrap().as_ref(), &region[3..6]);
    assert_eq!(r2.unwrap().unwrap().as_ref(), &region[6..10]);
    assert_eq!(
        blob.get_blob_calls() - before,
        1,
        "all point reads of one block must resolve to a single region fetch"
    );
}
