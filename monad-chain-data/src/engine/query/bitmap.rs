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

use roaring::RoaringBitmap;

use crate::{
    engine::{
        bitmap::{decode_bitmap_blob, page_start_local, LOCAL_ID_BITS, STREAM_PAGE_LOCAL_ID_SPAN},
        clause::IndexedClause,
        tables::FamilyTables,
    },
    error::Result,
    store::{BlobStore, MetaStore},
};

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    /// Loads the AND-intersection of all clauses for one shard, clipped to
    /// the local-id range. Returns `None` if the intersection is empty,
    /// meaning the shard cannot contribute candidates.
    ///
    /// The shard's local-id space is partitioned into disjoint 64K pages
    /// ([`STREAM_PAGE_LOCAL_ID_SPAN`]) and every clause bit in a page comes
    /// only from that page's stream rows, so intersection distributes over
    /// pages: `⋂_c (⋃_pages clause_c) = ⋃_pages (⋂_c clause_c_page)`. We walk
    /// page-outer / clause-inner rather than the other way around: for each
    /// page we fetch clause[0]'s page bitmap, and the moment the running
    /// per-page intersection empties we break — skipping every remaining
    /// clause's fetch for that page (including frontier-fragment scans). That
    /// per-page short-circuit is the win over the old clause-outer loop, which
    /// built each clause's full across-all-pages bitmap before ANDing and so
    /// could only prune when an entire clause was empty across the shard.
    pub async fn load_intersection_bitmap(
        &self,
        clauses: &[IndexedClause],
        shard: u64,
        local_from: u32,
        local_to: u32,
    ) -> Result<Option<RoaringBitmap>> {
        // Precompute each clause's shard streams once; reused across all pages.
        let clause_streams: Vec<Vec<String>> = clauses
            .iter()
            .map(|clause| clause.stream_ids_for_shard(shard))
            .collect();
        // A clause with no streams in this shard contributes an empty bitmap to
        // every page, so the whole shard intersection is empty. Bail before any
        // fetch, matching the old loop's "empty clause => None" behavior.
        if clause_streams.iter().any(|streams| streams.is_empty()) {
            return Ok(None);
        }

        let first_page_start = page_start_local(local_from);
        let last_page_start = page_start_local(local_to);
        let mut result = RoaringBitmap::new();

        let mut page_start = first_page_start;
        loop {
            // clause[0]'s page-P bitmap = OR over its stream values. If empty,
            // page P contributes nothing and we skip every remaining clause's
            // page-P fetch.
            let mut page_intersection = self
                .load_clause_page_bitmap(&clause_streams[0], page_start, local_from, local_to)
                .await?;

            for streams in &clause_streams[1..] {
                if page_intersection.is_empty() {
                    // Running per-page intersection is already empty; skip the
                    // remaining clauses' page-P fetches and move to next page.
                    break;
                }
                let clause_page = self
                    .load_clause_page_bitmap(streams, page_start, local_from, local_to)
                    .await?;
                page_intersection &= &clause_page;
            }

            // Union this page's surviving intersection into the shard result.
            // Pages are disjoint, so this OR never double-counts.
            if !page_intersection.is_empty() {
                result |= page_intersection;
            }

            if page_start == last_page_start {
                break;
            }
            page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
        }

        // Interior pages are fully inside the range; only the two boundary
        // pages can carry out-of-range bits. As before, clip the merged shard
        // result once at the end rather than per page.
        clip_bitmap_to_local_range(&mut result, local_from, local_to);
        Ok(Some(result).filter(|bitmap| !bitmap.is_empty()))
    }

    /// Loads one clause's bitmap for a single page: the OR over the clause's
    /// stream values of that stream's page-P bitmap.
    async fn load_clause_page_bitmap(
        &self,
        stream_ids: &[String],
        page_start: u32,
        local_from: u32,
        local_to: u32,
    ) -> Result<RoaringBitmap> {
        let mut clause_page = RoaringBitmap::new();
        for stream_id in stream_ids {
            clause_page |= self
                .load_bitmap_page(stream_id, page_start, local_from, local_to)
                .await?;
        }
        Ok(clause_page)
    }

    async fn load_bitmap_page(
        &self,
        stream_id: &str,
        page_start_local: u32,
        local_from: u32,
        local_to: u32,
    ) -> Result<RoaringBitmap> {
        if let Some(page) = self
            .load_bitmap_page_artifact(stream_id, page_start_local)
            .await?
        {
            if !overlaps(
                page.meta.min_local,
                page.meta.max_local,
                local_from,
                local_to,
            ) {
                return Ok(RoaringBitmap::new());
            }

            // Page loads may include out-of-range bits from a partially overlapping
            // page; the caller clips the final merged bitmap once per clause.
            return Ok(decode_bitmap_blob(page.bitmap_blob.as_ref())?.bitmap);
        }

        let mut page_bitmap = RoaringBitmap::new();
        for fragment in self
            .load_bitmap_fragments(stream_id, page_start_local)
            .await?
        {
            let fragment = decode_bitmap_blob(fragment.as_ref())?;
            if overlaps(fragment.min_local, fragment.max_local, local_from, local_to) {
                page_bitmap |= fragment.bitmap;
            }
        }

        Ok(page_bitmap)
    }
}

pub(crate) const fn max_local_id() -> u32 {
    (1u32 << LOCAL_ID_BITS) - 1
}

fn overlaps(start: u32, end: u32, query_start: u32, query_end: u32) -> bool {
    start <= query_end && end >= query_start
}

fn clip_bitmap_to_local_range(bitmap: &mut RoaringBitmap, local_from: u32, local_to: u32) {
    if local_from > 0 {
        bitmap.remove_range(0..local_from);
    }
    if local_to < u32::MAX {
        // Inclusive of `u32::MAX` so a bit at the very top of the id space is
        // also cleared; an exclusive `..u32::MAX` would leave it set. The guard
        // keeps `local_to + 1` from overflowing.
        bitmap.remove_range((local_to + 1)..=u32::MAX);
    }
}
