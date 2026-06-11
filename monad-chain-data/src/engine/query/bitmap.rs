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

use std::{collections::BTreeMap, sync::Arc};

use futures::future::try_join_all;
use roaring::RoaringBitmap;

use crate::{
    engine::{
        bitmap::{
            page_group_start, page_start, page_start_in_group, BitmapPageCounts, DecodedBitmapPage,
            PAGE_GROUP_ID_SPAN, STREAM_PAGE_ID_SPAN,
        },
        clause::IndexedClause,
        tables::FamilyTables,
    },
    error::Result,
    primitives::order::QueryOrder,
    store::MetaStore,
};

/// A page bitmap for intersection: a cached decoded page shared by refcount,
/// or an owned bitmap folded from fragments/clause streams. The clone in
/// [`Self::into_owned`] is paid only when a consumer mutates (boundary clipping).
pub(crate) enum PageBitmap {
    Shared(Arc<DecodedBitmapPage>),
    Owned(RoaringBitmap),
}

impl PageBitmap {
    pub(crate) fn as_bitmap(&self) -> &RoaringBitmap {
        match self {
            PageBitmap::Shared(page) => &page.bitmap,
            PageBitmap::Owned(bitmap) => bitmap,
        }
    }

    fn into_owned(self) -> RoaringBitmap {
        match self {
            PageBitmap::Shared(page) => page.bitmap.clone(),
            PageBitmap::Owned(bitmap) => bitmap,
        }
    }
}

/// Per-page-group intersection plan (clause streams + page-count manifests),
/// computed once and reused across every page of the group by both the
/// concurrent pipeline and the serial [`FamilyTables::load_intersection_ids`].
pub(crate) struct PageGroupPlan {
    /// The group this plan covers (manifest scope).
    group_start: u64,
    /// Per-clause value-streams (the OR set the clause expands to).
    clause_streams: Vec<Vec<String>>,
    /// Per-clause page-count manifest; `None` means "unknown", never "empty".
    clause_counts: Vec<Option<BitmapPageCounts>>,
    /// True iff the group is fully sealed: its manifest is authoritative for
    /// zero-fetch page skips. On the frontier group the manifest only orders
    /// fetches and never drives a skip.
    group_sealed: bool,
    /// The family's id frontier at the publication head; drives the per-PAGE
    /// sealed test ([`Self::page_sealed`]) for artifact-vs-fragment routing.
    frontier_id: u64,
}

impl<M: MetaStore> FamilyTables<M> {
    /// Builds the per-group plan shared by every page of the group starting at
    /// `group_start`. Returns `None` when a clause expands to no streams (its
    /// empty bitmap makes the whole intersection empty, so no page need be
    /// fetched).
    ///
    /// `frontier_id` is the family's id frontier at the publication head.
    /// Groups below the frontier's group are sealed and carry an immutable
    /// page-count manifest (authoritative for skips); on the frontier group
    /// the previous group's manifest serves as an ordering hint only and
    /// never skips a page.
    pub(crate) async fn build_page_group_plan(
        &self,
        clauses: &[IndexedClause],
        group_start: u64,
        frontier_id: u64,
    ) -> Result<Option<PageGroupPlan>> {
        let clause_streams: Vec<Vec<String>> =
            clauses.iter().map(IndexedClause::stream_ids).collect();
        if clause_streams.iter().any(|streams| streams.is_empty()) {
            return Ok(None);
        }

        let frontier_group_start = page_group_start(frontier_id);
        let group_sealed = group_start < frontier_group_start;
        let manifest_group = if group_sealed {
            Some(group_start)
        } else if group_start == frontier_group_start {
            // The frontier group has no manifest yet; borrow the last sealed
            // group's as a positional ordering hint (manifest page keys are
            // group-relative). Never used to skip, so a stale hint can only
            // mis-order fetches, not drop a page.
            group_start.checked_sub(PAGE_GROUP_ID_SPAN)
        } else {
            // Above the frontier: no data, no manifest.
            None
        };
        let clause_counts = match manifest_group {
            Some(manifest_group) => {
                self.load_clause_page_counts(&clause_streams, manifest_group)
                    .await?
            }
            None => vec![None; clauses.len()],
        };

        Ok(Some(PageGroupPlan {
            group_start,
            clause_streams,
            clause_counts,
            group_sealed,
            frontier_id,
        }))
    }

    /// AND-intersection of all clauses over the id range `[id_from, id_to]`,
    /// returned as primary ids in `order`; `None` if empty. Pages partition
    /// the id space, so intersection distributes over pages: this is the
    /// serial group-outer/page-inner fold over [`Self::intersect_group_page`],
    /// mirroring the pipeline's shape.
    ///
    /// `frontier_id` is the family's id frontier at the publication head;
    /// groups strictly below its group are sealed (manifest-skippable).
    ///
    /// Off the production path (the pipeline calls the per-page helpers
    /// directly); retained as the serial reference for the equivalence tests
    /// and must stay behavior-equivalent to those helpers.
    pub async fn load_intersection_ids(
        &self,
        clauses: &[IndexedClause],
        frontier_id: u64,
        id_from: u64,
        id_to: u64,
        order: QueryOrder,
    ) -> Result<Option<Vec<u64>>> {
        let first_group = page_group_start(id_from) / PAGE_GROUP_ID_SPAN;
        let last_group = page_group_start(id_to) / PAGE_GROUP_ID_SPAN;

        let mut result = Vec::new();
        for group_idx in order.iterate(first_group..=last_group) {
            let group_start = group_idx * PAGE_GROUP_ID_SPAN;
            let Some(plan) = self
                .build_page_group_plan(clauses, group_start, frontier_id)
                .await?
            else {
                return Ok(None);
            };

            let first_page = page_start(id_from.max(group_start));
            let last_page = page_start(id_to.min(group_start + (PAGE_GROUP_ID_SPAN - 1)));
            for page in order.iterate(plan.candidate_pages(first_page, last_page)) {
                let from_offset = if page == page_start(id_from) {
                    (id_from - page) as u32
                } else {
                    0
                };
                let to_offset = if page == page_start(id_to) {
                    (id_to - page) as u32
                } else {
                    STREAM_PAGE_ID_SPAN - 1
                };
                if let Some(page_intersection) = self
                    .intersect_group_page(&plan, page, from_offset, to_offset)
                    .await?
                {
                    result.extend(
                        order
                            .iterate(page_intersection.as_bitmap().iter())
                            .map(|offset| page + u64::from(offset)),
                    );
                }
            }
        }

        Ok(Some(result).filter(|ids| !ids.is_empty()))
    }

    /// Per-page clause intersection: fetch clauses most-selective-first, AND
    /// down, short-circuit when the running set empties. Returns the
    /// surviving page-relative bits clipped to `[from_offset, to_offset]`, or
    /// `None` if the page contributes nothing. The manifest skip is re-checked
    /// here so serial and concurrent callers agree even if a caller enqueues a
    /// guaranteed-empty page.
    pub(crate) async fn intersect_group_page(
        &self,
        plan: &PageGroupPlan,
        page: u64,
        from_offset: u32,
        to_offset: u32,
    ) -> Result<Option<PageBitmap>> {
        debug_assert_eq!(
            page_group_start(page),
            plan.group_start,
            "page outside the plan's group"
        );
        let page_in_group = page_start_in_group(page);
        if plan.group_sealed && manifest_proves_empty(&plan.clause_counts, page_in_group) {
            // Manifest proves the page empty in a sealed group: zero fetches.
            return Ok(None);
        }

        let page_counts = clause_page_counts(&plan.clause_counts, page_in_group);
        let order = clause_page_order(&page_counts);

        let mut page_intersection: Option<PageBitmap> = None;
        for clause_idx in order {
            if matches!(&page_intersection, Some(pb) if pb.as_bitmap().is_empty()) {
                break;
            }
            let clause_page = self
                .load_clause_page_bitmap(
                    &plan.clause_streams[clause_idx],
                    page,
                    from_offset,
                    to_offset,
                    plan.page_sealed(page),
                )
                .await?;
            page_intersection = Some(match page_intersection {
                // AND in place once owned; the first AND against a shared page
                // allocates only the intersection, never cloning the page.
                Some(PageBitmap::Owned(mut acc)) => {
                    acc &= clause_page.as_bitmap();
                    PageBitmap::Owned(acc)
                }
                Some(shared) => PageBitmap::Owned(shared.as_bitmap() & clause_page.as_bitmap()),
                None => clause_page,
            });
        }

        // Only boundary pages can carry out-of-range bits; clip only when
        // needed so a cache-shared page is returned without a copy.
        Ok(page_intersection
            .map(|pb| {
                let bitmap = pb.as_bitmap();
                let needs_clip = bitmap.min().is_some_and(|min| min < from_offset)
                    || bitmap.max().is_some_and(|max| max > to_offset);
                if needs_clip {
                    let mut owned = pb.into_owned();
                    clip_bitmap_to_offset_range(&mut owned, from_offset, to_offset);
                    PageBitmap::Owned(owned)
                } else {
                    pb
                }
            })
            .filter(|pb| !pb.as_bitmap().is_empty()))
    }

    /// Page-count manifest per clause in `manifest_group`: a clause's per-page
    /// count is the SUM of its OR value-streams' counts. `None` for a clause
    /// means no manifest covered it — callers must treat it as "unknown",
    /// never "empty".
    async fn load_clause_page_counts(
        &self,
        clause_streams: &[Vec<String>],
        manifest_group: u64,
    ) -> Result<Vec<Option<BitmapPageCounts>>> {
        let mut out = Vec::with_capacity(clause_streams.len());
        for streams in clause_streams {
            // Every stream's manifest is needed; load them concurrently.
            // A stream without a manifest row makes the clause count unknown
            // (`None`) — abandon the estimate so we never under-count and skip
            // a non-empty page. An empty stream set is likewise unknown.
            let per_stream: Option<Vec<BitmapPageCounts>> = try_join_all(
                streams
                    .iter()
                    .map(|stream_id| self.load_bitmap_page_counts(stream_id, manifest_group)),
            )
            .await?
            .into_iter()
            .collect();
            let summed = per_stream
                .filter(|streams| !streams.is_empty())
                .map(|streams| {
                    let mut acc: BTreeMap<u32, u32> = BTreeMap::new();
                    for counts in streams {
                        for (page, count) in counts.pages {
                            let entry = acc.entry(page).or_insert(0);
                            *entry = entry.saturating_add(count);
                        }
                    }
                    BitmapPageCounts {
                        pages: acc.into_iter().collect(),
                    }
                });
            out.push(summed);
        }
        Ok(out)
    }

    /// One clause's bitmap for a single page: the OR over the clause's
    /// streams of that stream's page bitmap. Every stream is needed
    /// unconditionally (an OR fold cannot short-circuit), so the pages are
    /// fetched concurrently and folded after — unlike the cross-clause AND
    /// in [`Self::intersect_group_page`], whose serial short-circuit is the
    /// point.
    async fn load_clause_page_bitmap(
        &self,
        stream_ids: &[String],
        page: u64,
        from_offset: u32,
        to_offset: u32,
        page_sealed: bool,
    ) -> Result<PageBitmap> {
        let mut pages = try_join_all(stream_ids.iter().map(|stream_id| {
            self.load_bitmap_page(stream_id, page, from_offset, to_offset, page_sealed)
        }))
        .await?
        .into_iter();
        let Some(first) = pages.next() else {
            return Ok(PageBitmap::Owned(RoaringBitmap::new()));
        };
        // Single-stream clauses (the common case) pass the page through
        // shared; only a multi-stream OR folds into an owned accumulator.
        if pages.as_slice().is_empty() {
            return Ok(first);
        }
        let mut owned = first.into_owned();
        for page in pages {
            owned |= page.as_bitmap();
        }
        Ok(PageBitmap::Owned(owned))
    }

    /// One stream's bitmap for a single page. Only a SEALED page may probe
    /// its compacted artifact: once a page seals, the bits that arrived
    /// between its last flush and the seal exist ONLY in the artifact, so
    /// "artifact absent ⇒ fragment union is complete" holds for open pages
    /// alone. An open page routes straight to fragments (where all its bits
    /// live). A sealed page without an artifact is the sparse-stream norm
    /// (`seal_family` writes artifacts only for pages that accumulated bits),
    /// so the empty fragment union is the correct answer; a bits-bearing
    /// sealed page missing its artifact is store corruption, which recovery's
    /// rebuild detects (`SealedPageMissingArtifact`), not the query path.
    async fn load_bitmap_page(
        &self,
        stream_id: &str,
        page_start: u64,
        from_offset: u32,
        to_offset: u32,
        page_sealed: bool,
    ) -> Result<PageBitmap> {
        if page_sealed {
            if let Some(page) = self
                .load_bitmap_page_artifact(stream_id, page_start)
                .await?
            {
                if !overlaps(
                    page.meta.min_offset,
                    page.meta.max_offset,
                    from_offset,
                    to_offset,
                ) {
                    return Ok(PageBitmap::Owned(RoaringBitmap::new()));
                }

                // May include out-of-range bits; the caller clips the final
                // intersection once per page. Share the cached page, don't clone.
                return Ok(PageBitmap::Shared(page));
            }
        }

        let mut page_bitmap = RoaringBitmap::new();
        for fragment in self.load_bitmap_fragments(stream_id, page_start).await? {
            if overlaps(
                fragment.min_offset,
                fragment.max_offset,
                from_offset,
                to_offset,
            ) {
                page_bitmap |= &fragment.bitmap;
            }
        }

        Ok(PageBitmap::Owned(page_bitmap))
    }
}

impl PageGroupPlan {
    /// Whether the page starting at `page` is sealed at this plan's frontier:
    /// sealed iff the page lies fully below it, mirroring `seal_family`'s
    /// ready filter (`SEAL_SPAN == PAGE_SPAN`). Per-PAGE, never per-group:
    /// the frontier group still contains sealed pages whose seal-consumed
    /// bits exist only in their artifacts.
    fn page_sealed(&self, page: u64) -> bool {
        page + u64::from(STREAM_PAGE_ID_SPAN) <= self.frontier_id
    }

    /// Page starts in `[first_page, last_page]` (inclusive, page-aligned,
    /// within this plan's group) that may contribute candidates, ascending.
    /// In a sealed group, pages the manifest proves empty for any clause are
    /// dropped without a fetch; in the frontier group every page in range is
    /// a candidate.
    pub(crate) fn candidate_pages(&self, first_page: u64, last_page: u64) -> Vec<u64> {
        debug_assert!(
            page_group_start(first_page) == self.group_start
                && page_group_start(last_page) == self.group_start,
            "page bounds outside the plan's group"
        );
        // Both bounds are page-aligned, so the inclusive end is hit exactly.
        (first_page..=last_page)
            .step_by(STREAM_PAGE_ID_SPAN as usize)
            .filter(|&page| {
                !(self.group_sealed
                    && manifest_proves_empty(&self.clause_counts, page_start_in_group(page)))
            })
            .collect()
    }
}

/// The central sealed-skip rule: the manifest proves the page empty when it
/// covers some clause (`Some`) and that clause's count for the page is 0 (a
/// page absent from the zero-dropped manifest counts as 0). `None` means the
/// manifest did not cover the clause — unknown, never empty. Equivalent to
/// `clause_page_counts(..).contains(&Some(0))` without the per-page `Vec` alloc.
fn manifest_proves_empty(
    clause_counts: &[Option<BitmapPageCounts>],
    page_start_in_group: u32,
) -> bool {
    clause_counts.iter().any(|counts| {
        counts
            .as_ref()
            .is_some_and(|counts| counts.count_for_page(page_start_in_group).unwrap_or(0) == 0)
    })
}

/// Per-clause count for one page. `None` = manifest did not cover the clause
/// (unknown); `Some(0)` = manifest proved the clause empty in this page.
fn clause_page_counts(
    clause_counts: &[Option<BitmapPageCounts>],
    page_start_in_group: u32,
) -> Vec<Option<u32>> {
    clause_counts
        .iter()
        .map(|counts| {
            counts.as_ref().map(|counts| {
                // A page absent from the (zero-dropped) manifest has count 0.
                counts.count_for_page(page_start_in_group).unwrap_or(0)
            })
        })
        .collect()
}

/// Orders clause indices most-selective-first: known counts ascending, then
/// unknowns; the stable sort preserves clause order for ties and unknowns.
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

fn clip_bitmap_to_offset_range(bitmap: &mut RoaringBitmap, from_offset: u32, to_offset: u32) {
    if from_offset > 0 {
        bitmap.remove_range(0..from_offset);
    }
    if to_offset < u32::MAX {
        // Inclusive end so a bit at `u32::MAX` is also cleared; the guard
        // keeps `to_offset + 1` from overflowing.
        bitmap.remove_range((to_offset + 1)..=u32::MAX);
    }
}
