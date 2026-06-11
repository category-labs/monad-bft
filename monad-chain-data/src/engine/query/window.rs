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

use crate::{
    engine::{
        bitmap::{
            page_group_start, page_offset, page_start, PAGE_GROUP_ID_SPAN, STREAM_PAGE_ID_SPAN,
        },
        family::Family,
        tables::BlockTables,
    },
    error::{MonadChainDataError, Result},
    primitives::{
        order::QueryOrder,
        range::ResolvedBlockWindow,
        records::{BlockRecord, PrimaryId},
    },
    store::MetaStore,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedPrimaryIdWindow {
    pub start: PrimaryId,
    pub end_inclusive: PrimaryId,
}

impl ResolvedPrimaryIdWindow {
    /// The page groups this window touches, as group starts in query order.
    pub fn group_iter(&self, order: QueryOrder) -> impl Iterator<Item = u64> {
        let first = page_group_start(self.start.as_u64()) / PAGE_GROUP_ID_SPAN;
        let last = page_group_start(self.end_inclusive.as_u64()) / PAGE_GROUP_ID_SPAN;
        order
            .iterate(first..=last)
            .map(|group_idx| group_idx * PAGE_GROUP_ID_SPAN)
    }

    /// First and last page starts the window covers within `group_start`'s
    /// group (inclusive). The caller only passes groups from
    /// [`Self::group_iter`], so the clamped range is never empty.
    pub fn page_bounds_in_group(&self, group_start: u64) -> (u64, u64) {
        let low = self.start.as_u64().max(group_start);
        let high = self
            .end_inclusive
            .as_u64()
            .min(group_start + (PAGE_GROUP_ID_SPAN - 1));
        (page_start(low), page_start(high))
    }

    /// Page-relative offset range of this window within one page: full-page
    /// `(0, span - 1)` except on the window's boundary pages.
    pub fn offsets_in_page(&self, page: u64) -> (u32, u32) {
        let from = if page_start(self.start.as_u64()) == page {
            page_offset(self.start.as_u64())
        } else {
            0
        };
        let to = if page_start(self.end_inclusive.as_u64()) == page {
            page_offset(self.end_inclusive.as_u64())
        } else {
            STREAM_PAGE_ID_SPAN - 1
        };
        (from, to)
    }
}

/// Resolves the family's primary-id window from the range's two endpoint
/// block records alone. Ingest stamps `first_primary_id` with the running
/// family frontier on EVERY block (count-0 blocks included), so the frontier
/// is monotone in block number and:
///
/// - the first in-range id is `start.first_primary_id` — if the start block
///   is empty, the frontier is unchanged until the first in-range block with
///   data, whose window begins at exactly that id;
/// - the end-exclusive id is the frontier after the end block;
/// - the range holds no rows iff the two are equal.
///
/// (A prior version walked block records inward from both ends serially —
/// one point-get per block, unbounded on data-empty spans: a filtered query
/// over a quiet 100k-block historical range cost ~60s of cold gets.)
pub(crate) async fn resolve_primary_id_window<M: MetaStore>(
    blocks: &BlockTables<M>,
    family: Family,
    block_window: &ResolvedBlockWindow,
) -> Result<Option<ResolvedPrimaryIdWindow>> {
    // The range bounds were verified present by [`ResolvedBlockWindow`], so
    // a missing endpoint record is a broken store contract — fail loud
    // rather than serve a wrongly-empty page.
    let (start_record, end_record) = futures::try_join!(
        load_record_in_range(blocks, block_window.low.number),
        load_record_in_range(blocks, block_window.high.number),
    )?;

    let start = family.window_in(&start_record).first_primary_id;
    let end_exclusive = family.window_in(&end_record).next_primary_id_exclusive()?;
    if end_exclusive <= start {
        return Ok(None);
    }

    Ok(Some(ResolvedPrimaryIdWindow {
        start,
        end_inclusive: PrimaryId::new(end_exclusive.as_u64() - 1),
    }))
}

/// Loads one block record inside the resolved range. The range bounds were
/// verified present by [`ResolvedBlockWindow`], so a missing record here is a
/// broken store contract — fail loud rather than serve a wrongly-empty page.
async fn load_record_in_range<M: MetaStore>(
    blocks: &BlockTables<M>,
    block_number: u64,
) -> Result<BlockRecord> {
    blocks
        .load_record(block_number)
        .await?
        .ok_or(MonadChainDataError::MissingData(
            "missing block record inside resolved range",
        ))
}
