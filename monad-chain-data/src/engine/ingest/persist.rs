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
    primitives::state::FamilyWindowRecord,
    store::{BlobStore, MetaStore},
};

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    /// Stages one block's family Phase A artifacts into the given meta and
    /// blob batches: per-block blob, per-block header bytes, primary-directory
    /// fragment writes (one per overlapped bucket), and one bitmap-fragment
    /// scan_put per bitmap fragment. Pure — no I/O. Phase B compactions are
    /// planned and staged separately by `plan_*_compactions` /
    /// `stage_*_compactions`.
    pub fn stage_indexed_family_ingest(
        &self,
        meta: &mut M::Batch,
        blob: &mut B::Batch,
        block_number: u64,
        block_blob: Vec<u8>,
        block_header_bytes: Bytes,
        window: FamilyWindowRecord,
        bitmap_fragments: &[BitmapFragmentWrite],
    ) {
        let first_primary_id = window.first_primary_id.as_u64();

        self.stage_block_blob(blob, block_number, block_blob);
        self.stage_block_header(meta, block_number, block_header_bytes);
        self.dir()
            .stage_block_fragment(meta, block_number, first_primary_id, window.count);
        for fragment in bitmap_fragments {
            self.bitmap().stage_fragment(meta, fragment, block_number);
        }
    }
}
