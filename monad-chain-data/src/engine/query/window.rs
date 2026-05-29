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

use super::bitmap::max_local_id;
use crate::{
    engine::{family::Family, tables::BlockTables},
    error::Result,
    primitives::{
        page::QueryOrder,
        range::ResolvedBlockWindow,
        state::{FamilyWindowRecord, PrimaryId},
    },
    store::MetaStore,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedPrimaryIdWindow {
    pub start: PrimaryId,
    pub end_inclusive: PrimaryId,
}

impl ResolvedPrimaryIdWindow {
    /// Returns the local-ID range within the given shard, clipped to this
    /// window's boundaries.
    pub fn local_range_for_shard(&self, shard: u64) -> (u32, u32) {
        let local_from = if shard == self.start.shard() {
            self.start.local()
        } else {
            0
        };
        let local_to = if shard == self.end_inclusive.shard() {
            self.end_inclusive.local()
        } else {
            max_local_id()
        };
        (local_from, local_to)
    }

    /// Returns the shards covered by this window in the given query order.
    pub fn shard_iter(&self, order: QueryOrder) -> Vec<u64> {
        let mut shards: Vec<u64> = (self.start.shard()..=self.end_inclusive.shard()).collect();
        if order == QueryOrder::Descending {
            shards.reverse();
        }
        shards
    }
}

pub(crate) async fn resolve_primary_id_window<M: MetaStore>(
    blocks: &BlockTables<M>,
    family: Family,
    block_window: &ResolvedBlockWindow,
) -> Result<Option<ResolvedPrimaryIdWindow>> {
    let start_block = block_window.low.number;
    let end_block = block_window.high.number;

    let Some(start) = first_primary_id_in_range(blocks, family, start_block, end_block).await?
    else {
        return Ok(None);
    };
    let Some(end_exclusive) =
        end_primary_id_exclusive_in_range(blocks, family, start_block, end_block).await?
    else {
        return Ok(None);
    };

    Ok(Some(ResolvedPrimaryIdWindow {
        start,
        end_inclusive: PrimaryId::new(end_exclusive.as_u64().saturating_sub(1)),
    }))
}

async fn first_primary_id_in_range<M: MetaStore>(
    blocks: &BlockTables<M>,
    family: Family,
    start_block: u64,
    end_block: u64,
) -> Result<Option<PrimaryId>> {
    let mut block_number = start_block;
    while block_number <= end_block {
        let Some(record) = blocks.load_record(block_number).await? else {
            return Ok(None);
        };
        if let Some(first) = family.window_in(&record).and_then(non_empty_window_start) {
            return Ok(Some(first));
        }
        block_number = block_number.saturating_add(1);
    }

    Ok(None)
}

async fn end_primary_id_exclusive_in_range<M: MetaStore>(
    blocks: &BlockTables<M>,
    family: Family,
    start_block: u64,
    end_block: u64,
) -> Result<Option<PrimaryId>> {
    let mut block_number = end_block;
    loop {
        let Some(record) = blocks.load_record(block_number).await? else {
            return Ok(None);
        };
        if let Some(window) = family.window_in(&record) {
            if let Some(end_exclusive) = non_empty_window_end(window)? {
                return Ok(Some(end_exclusive));
            }
        }
        if block_number == start_block {
            break;
        }
        block_number = block_number.saturating_sub(1);
    }

    Ok(None)
}

fn non_empty_window_start(window: FamilyWindowRecord) -> Option<PrimaryId> {
    (window.count > 0).then_some(window.first_primary_id)
}

fn non_empty_window_end(window: FamilyWindowRecord) -> Result<Option<PrimaryId>> {
    if window.count == 0 {
        return Ok(None);
    }

    window.next_primary_id_exclusive().map(Some)
}
