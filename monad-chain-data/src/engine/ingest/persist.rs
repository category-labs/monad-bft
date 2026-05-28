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
    store::{BlobStore, MetaStore},
};

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    /// Persists one block's family artifacts: the per-block blob and
    /// header bytes, the primary-directory fragment, and the bitmap
    /// fragments — followed by the directory-bucket and bitmap-page
    /// compactions sealed by the ingest transition.
    pub async fn persist_indexed_family_ingest(
        &self,
        block_number: u64,
        block_blob: Vec<u8>,
        block_header_bytes: Bytes,
        window: FamilyWindowRecord,
        bitmap_fragments: &[BitmapFragmentWrite],
    ) -> Result<()> {
        let first_primary_id = window.first_primary_id.as_u64();
        let next_primary_id_exclusive = window.next_primary_id_exclusive()?.as_u64();

        self.store_block_blob(block_number, block_blob).await?;
        self.store_block_header(block_number, block_header_bytes)
            .await?;
        self.dir()
            .persist_block_fragment(block_number, first_primary_id, window.count)
            .await?;
        for fragment in bitmap_fragments {
            self.store_bitmap_fragment(fragment, block_number).await?;
        }
        self.compact_newly_sealed_directory_buckets(first_primary_id, next_primary_id_exclusive)
            .await?;
        self.compact_newly_sealed_bitmap_pages(
            bitmap_fragments,
            first_primary_id,
            next_primary_id_exclusive,
        )
        .await?;

        Ok(())
    }
}
