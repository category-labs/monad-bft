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

use std::collections::BTreeMap;

use roaring::RoaringBitmap;

use crate::{
    engine::{
        bitmap::{
            decode_bitmap_blob, page_start_local, BitmapPageCounts, LOCAL_ID_BITS,
            STREAM_PAGE_LOCAL_ID_SPAN,
        },
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
    ///
    /// `frontier_shard` is the single open shard — the shard holding the
    /// family's id frontier at the publication head (`PrimaryId::shard` of the
    /// frontier id, the same notion the directory resolver derives its
    /// `sealed_below` from). Any `shard < frontier_shard` is fully sealed and
    /// carries an immutable per-stream page-count manifest, which lets us skip
    /// a page with zero clause-matches without any fetch and order each
    /// surviving page's clause fetches most-selective-first. On the frontier
    /// shard the manifest is absent (the shard has not sealed), so it is a hint
    /// for ordering only and NEVER causes a page to be skipped — see
    /// [`Self::clause_page_order`].
    pub async fn load_intersection_bitmap(
        &self,
        clauses: &[IndexedClause],
        shard: u64,
        frontier_shard: u64,
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

        // A sealed shard is authoritative for skipping; the frontier shard's
        // manifest is absent, so its per-page counts come from the last sealed
        // shard as an ordering hint that may never drive a skip.
        let shard_sealed = shard < frontier_shard;
        let manifest_shard = if shard_sealed {
            // The shard's own manifest is authoritative for skips.
            Some(shard)
        } else if shard == frontier_shard {
            // Frontier shard: borrow the last sealed shard's manifest as an
            // ordering hint only. Never used to skip (guarded by `shard_sealed`
            // below), so a stale hint can only mis-order fetches, not drop a
            // page.
            shard.checked_sub(1)
        } else {
            // Above the frontier: no data, no manifest.
            None
        };
        let clause_counts = match manifest_shard {
            Some(manifest_shard) => {
                self.load_clause_page_counts(clauses, manifest_shard).await?
            }
            None => vec![None; clauses.len()],
        };

        let first_page_start = page_start_local(local_from);
        let last_page_start = page_start_local(local_to);
        let mut result = RoaringBitmap::new();

        let mut page_start = first_page_start;
        loop {
            // Per-clause count in this page from the manifest (`None` when the
            // manifest is unavailable for this clause/page). On a sealed shard a
            // `Some(0)` for any clause means the page cannot contribute, so we
            // skip every clause's page fetch. On the frontier shard the counts
            // only order the fetches.
            let page_counts = clause_page_counts(&clause_counts, page_start);

            if shard_sealed && page_counts.contains(&Some(0)) {
                // Guaranteed-empty page on a sealed shard: zero fetches.
                if page_start == last_page_start {
                    break;
                }
                page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
                continue;
            }

            // Fetch clauses most-selective-first so the running intersection
            // collapses earliest and short-circuits the rest. Clauses with a
            // known count sort ascending ahead of clauses with no estimate.
            let order = clause_page_order(&page_counts);

            let mut page_intersection: Option<RoaringBitmap> = None;
            for clause_idx in order {
                if matches!(&page_intersection, Some(bitmap) if bitmap.is_empty()) {
                    // Running per-page intersection is already empty; skip the
                    // remaining clauses' page-P fetches and move to next page.
                    break;
                }
                let clause_page = self
                    .load_clause_page_bitmap(
                        &clause_streams[clause_idx],
                        page_start,
                        local_from,
                        local_to,
                    )
                    .await?;
                page_intersection = Some(match page_intersection {
                    Some(mut acc) => {
                        acc &= &clause_page;
                        acc
                    }
                    None => clause_page,
                });
            }

            // Union this page's surviving intersection into the shard result.
            // Pages are disjoint, so this OR never double-counts.
            if let Some(page_intersection) = page_intersection {
                if !page_intersection.is_empty() {
                    result |= page_intersection;
                }
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

    /// Loads the page-count manifest for each clause in `manifest_shard`. A
    /// clause's per-page count is the SUM of its OR value-streams' counts, so
    /// a clause is empty in a page iff all its streams are empty there. Returns
    /// one entry per clause; `None` means no manifest was available for that
    /// clause (a missing sealed-shard manifest degrades to fetch-without-skip,
    /// see the module note), so callers must treat `None` as "unknown", never
    /// "empty".
    async fn load_clause_page_counts(
        &self,
        clauses: &[IndexedClause],
        manifest_shard: u64,
    ) -> Result<Vec<Option<BitmapPageCounts>>> {
        let mut out = Vec::with_capacity(clauses.len());
        for clause in clauses {
            let streams = clause.stream_ids_for_shard(manifest_shard);
            let mut summed: Option<BTreeMap<u32, u32>> = None;
            for stream_id in &streams {
                match self.load_bitmap_page_counts(stream_id).await? {
                    Some(counts) => {
                        let acc = summed.get_or_insert_with(BTreeMap::new);
                        for (page, count) in counts.pages {
                            let entry = acc.entry(page).or_insert(0);
                            *entry = entry.saturating_add(count);
                        }
                    }
                    None => {
                        // A stream with no manifest row leaves the clause's
                        // count unknown for this shard; abandon the estimate so
                        // we never under-count and skip a non-empty page.
                        summed = None;
                        break;
                    }
                }
            }
            out.push(summed.map(|pages| BitmapPageCounts {
                pages: pages.into_iter().collect(),
            }));
        }
        Ok(out)
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

/// Per-clause count for one page, looked up from each clause's manifest.
/// `None` for a clause means the manifest did not cover it (unknown), `Some(0)`
/// means the manifest proved the clause empty in this page.
fn clause_page_counts(
    clause_counts: &[Option<BitmapPageCounts>],
    page_start: u32,
) -> Vec<Option<u32>> {
    clause_counts
        .iter()
        .map(|counts| {
            counts.as_ref().map(|counts| {
                // A page absent from the (zero-dropped) manifest has count 0.
                counts.count_for_page(page_start).unwrap_or(0)
            })
        })
        .collect()
}

/// Orders clause indices for a page most-selective-first. Clauses with a known
/// count sort ascending and ahead of clauses with an unknown (`None`) count;
/// ties and unknowns preserve clause order (a stable sort), so with no manifest
/// this reproduces the original clause-order fetch sequence exactly.
fn clause_page_order(page_counts: &[Option<u32>]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..page_counts.len()).collect();
    order.sort_by_key(|&idx| match page_counts[idx] {
        Some(count) => (0u8, count),
        None => (1u8, u32::MAX),
    });
    order
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
