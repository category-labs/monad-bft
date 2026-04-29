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
    error::{MonadChainDataError, Result},
    kernel::{
        bitmap::{decode_bitmap_blob, page_start_local, STREAM_PAGE_LOCAL_ID_SPAN},
        tables::LogTables,
    },
    logs::IndexedLogClause,
    primitives::state::LogId,
    store::{BlobStore, MetaStore},
};

pub(crate) async fn load_clause_bitmap_for_shard<M: MetaStore, B: BlobStore>(
    logs: &LogTables<M, B>,
    clause: &IndexedLogClause,
    shard: u64,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    let stream_ids = clause.stream_ids_for_shard(shard);
    if stream_ids.is_empty() {
        return Ok(RoaringBitmap::new());
    }

    let first_page_start = page_start_local(local_from);
    let last_page_start = page_start_local(local_to);
    let mut clause_bitmap = RoaringBitmap::new();

    for stream_id in stream_ids {
        let mut page_start = first_page_start;
        loop {
            clause_bitmap |=
                load_bitmap_page(logs, &stream_id, page_start, local_from, local_to).await?;

            if page_start == last_page_start {
                break;
            }
            page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
        }
    }

    clip_bitmap_to_local_range(&mut clause_bitmap, local_from, local_to);
    Ok(clause_bitmap)
}

async fn load_bitmap_page<M: MetaStore, B: BlobStore>(
    logs: &LogTables<M, B>,
    stream_id: &str,
    page_start_local: u32,
    local_from: u32,
    local_to: u32,
) -> Result<RoaringBitmap> {
    if let Some(meta) = logs
        .load_bitmap_page_meta(stream_id, page_start_local)
        .await?
    {
        if !overlaps(meta.min_local, meta.max_local, local_from, local_to) {
            return Ok(RoaringBitmap::new());
        }

        let page_blob = logs
            .load_bitmap_page_blob(stream_id, page_start_local)
            .await?
            .ok_or(MonadChainDataError::MissingData(
                "missing log bitmap page blob",
            ))?;
        // Page loads may include out-of-range bits from a partially overlapping
        // page; the caller clips the final merged bitmap once per clause.
        return Ok(decode_bitmap_blob(page_blob.as_ref())?.bitmap);
    }

    let mut page_bitmap = RoaringBitmap::new();
    for fragment in logs
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

pub(crate) const fn max_local_id() -> u32 {
    (1u32 << LogId::LOCAL_ID_BITS) - 1
}

fn overlaps(start: u32, end: u32, query_start: u32, query_end: u32) -> bool {
    start <= query_end && end >= query_start
}

fn clip_bitmap_to_local_range(bitmap: &mut RoaringBitmap, local_from: u32, local_to: u32) {
    if local_from > 0 {
        bitmap.remove_range(0..local_from);
    }
    if local_to < u32::MAX {
        bitmap.remove_range(local_to.saturating_add(1)..u32::MAX);
    }
}
