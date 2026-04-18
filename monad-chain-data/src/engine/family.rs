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

use crate::store::{BlobTableId, ScannableTableId, TableId};

pub struct FamilyTableIds {
    pub block_header: TableId,
    pub block_blob: BlobTableId,
    pub dir_by_block: ScannableTableId,
    pub dir_bucket: TableId,
    pub bitmap_by_block: ScannableTableId,
    pub bitmap_page_meta: TableId,
    pub bitmap_page_blob: TableId,
    pub open_bitmap_stream: ScannableTableId,
}

macro_rules! family_table_ids {
    ($prefix:literal) => {
        FamilyTableIds {
            block_header: TableId::new(concat!($prefix, "_block_header")),
            block_blob: BlobTableId::new(concat!($prefix, "_block_blob")),
            dir_by_block: ScannableTableId::new(concat!($prefix, "_dir_by_block")),
            dir_bucket: TableId::new(concat!($prefix, "_dir_bucket")),
            bitmap_by_block: ScannableTableId::new(concat!($prefix, "_bitmap_by_block")),
            bitmap_page_meta: TableId::new(concat!($prefix, "_bitmap_page_meta")),
            bitmap_page_blob: TableId::new(concat!($prefix, "_bitmap_page_blob")),
            open_bitmap_stream: ScannableTableId::new(concat!($prefix, "_open_bitmap_stream")),
        }
    };
}

/// Indexed record family served by the engine. The variant set is the
/// canonical list of domains the engine supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Family {
    Log,
}

impl Family {
    pub const fn table_ids(self) -> FamilyTableIds {
        match self {
            Family::Log => family_table_ids!("log"),
        }
    }
}
