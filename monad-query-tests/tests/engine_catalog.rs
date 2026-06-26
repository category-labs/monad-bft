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

//! Catalog ⇄ declared-`TableId` consistency check. It spans the engine catalog
//! (`ALL_LOGICAL_TABLE_NAMES`) and the write layer (`SnapshotStore`), so it
//! lives here rather than in either single crate.

use monad_query_engine::txs::TxHashIndexTable;
use monad_query_tests::prelude::*;
use monad_query_write::ingest::SnapshotStore;

/// [`ALL_LOGICAL_TABLE_NAMES`] is the provisioning source of truth for
/// per-logical-table backends; this pins it to the declared `TableId`s.
#[test]
fn all_logical_table_names_match_declared_table_ids() {
    type Meta = InMemoryMetaStore;
    let mut declared = vec![
        PublicationTables::<Meta>::PUBLICATION_STATE_TABLE.as_str(),
        SnapshotStore::<Meta, InMemoryBlobStore>::MANIFEST_TABLE.as_str(),
        BlockTables::<Meta>::BLOCK_METADATA_TABLE.as_str(),
        BlockTables::<Meta>::BLOCK_EVM_HEADER_TABLE.as_str(),
        BlockTables::<Meta>::BLOCK_HASH_TO_NUMBER_INDEX_TABLE.as_str(),
        TxHashIndexTable::<Meta>::TABLE.as_str(),
    ];
    for family in Family::ALL {
        // Exhaustive destructure (no `..`): adding a field to
        // `FamilyTableIds` is a compile error here, forcing the canonical
        // list above to be updated rather than silently drifting.
        let FamilyTableIds {
            dict_by_version,
            dir_by_block,
            dir_bucket,
            bitmap_by_block,
            bitmap_page_blob,
            bitmap_page_counts,
            open_bitmap_stream,
            seal_chain,
            // Stats label only — nothing is provisioned under this name.
            block_metadata_cache_label: _,
        } = family.table_ids();
        declared.extend([
            dict_by_version.as_str(),
            dir_by_block.as_str(),
            dir_bucket.as_str(),
            bitmap_by_block.as_str(),
            bitmap_page_blob.as_str(),
            bitmap_page_counts.as_str(),
            open_bitmap_stream.as_str(),
            seal_chain.as_str(),
        ]);
    }

    let mut canonical = ALL_LOGICAL_TABLE_NAMES.to_vec();
    declared.sort_unstable();
    canonical.sort_unstable();
    assert_eq!(canonical, declared);
}
