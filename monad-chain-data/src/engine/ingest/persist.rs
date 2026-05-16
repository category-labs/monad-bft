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

use bytes::Bytes;

use crate::{
    engine::{bitmap::BitmapFragmentWrite, tables::FamilyTables},
    error::Result,
    primitives::state::FamilyWindowRecord,
    store::{BlobStore, MetaStore, WriteSession},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PhaseAFragmentWriteFilter {
    pub open_dir_bucket: u64,
    pub open_bitmap_page: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PhaseAFragmentStageStats {
    pub dir_fragments_total: u64,
    pub dir_fragments_written: u64,
    pub bitmap_fragments_total: u64,
    pub bitmap_fragments_written: u64,
}

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    /// Stages one block's family Phase A artifacts via the write session:
    /// per-block blob, per-block header bytes, primary-directory fragment
    /// writes (one per overlapped bucket), and one bitmap-fragment scan_put
    /// per bitmap fragment. Pure — no I/O. Phase B compactions are planned
    /// and staged separately by `plan_*_compactions` / `stage_*_compactions`.
    pub(crate) fn stage_indexed_family_ingest(
        &self,
        w: &WriteSession<'_, M, B>,
        block_number: u64,
        block_blob: Vec<u8>,
        block_header_bytes: Bytes,
        window: FamilyWindowRecord,
        bitmap_fragments: &[BitmapFragmentWrite],
        fragment_filter: PhaseAFragmentWriteFilter,
    ) -> Result<PhaseAFragmentStageStats> {
        let first_primary_id = window.first_primary_id.as_u64();
        let mut stats = PhaseAFragmentStageStats::default();

        self.stage_block_blob(w, block_number, block_blob);
        self.stage_block_header(w, block_number, block_header_bytes);
        if window.count > 0 {
            self.dir().stage_block_fragment_filtered(
                w,
                block_number,
                first_primary_id,
                window.count,
                |bucket| {
                    stats.dir_fragments_total = stats.dir_fragments_total.saturating_add(1);
                    if bucket == fragment_filter.open_dir_bucket {
                        stats.dir_fragments_written = stats.dir_fragments_written.saturating_add(1);
                        true
                    } else {
                        false
                    }
                },
            );
        }
        let (bitmap_total, bitmap_written) = self.bitmap().stage_fragments_for_global_page(
            w,
            bitmap_fragments,
            block_number,
            fragment_filter.open_bitmap_page,
        )?;
        stats.bitmap_fragments_total = stats.bitmap_fragments_total.saturating_add(bitmap_total);
        stats.bitmap_fragments_written = stats
            .bitmap_fragments_written
            .saturating_add(bitmap_written);
        Ok(stats)
    }
}
