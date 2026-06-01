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
    engine::{bitmap::BitmapFragmentWrite, tables::FamilyTables},
    error::Result,
    primitives::state::FamilyWindowRecord,
    store::{BlobStore, MetaStore, WriteSession},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PhaseAFragmentWriteFilter {
    pub open_bitmap_page: u64,
}

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    /// Stages one block's family Phase A artifacts via the write session:
    /// per-block blob, primary-directory fragment writes (one per overlapped
    /// bucket), and one bitmap-fragment scan_put per bitmap fragment. Pure —
    /// no I/O. Phase B compactions are planned and staged separately by
    /// `plan_*_compactions` / `stage_*_compactions`.
    pub(crate) fn stage_indexed_family_ingest(
        &self,
        w: &mut WriteSession<'_, M, B>,
        block_number: u64,
        block_blob: Vec<u8>,
        window: FamilyWindowRecord,
        bitmap_fragments: &[BitmapFragmentWrite],
        fragment_filter: PhaseAFragmentWriteFilter,
    ) -> Result<()> {
        let first_primary_id = window.first_primary_id.as_u64();

        self.stage_block_blob(w, block_number, block_blob);
        self.dir().stage_block_fragment_filtered(
            w,
            block_number,
            first_primary_id,
            window.count,
            |_| true,
        );
        self.bitmap().stage_fragments_for_global_page(
            w,
            bitmap_fragments,
            block_number,
            fragment_filter.open_bitmap_page,
        )?;
        Ok(())
    }
}
