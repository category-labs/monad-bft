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

use serde::{Deserialize, Serialize};

use crate::{
    primitives::records::{BlockRecord, FamilyWindowRecord},
    store::{BlobTableId, ScannableTableId, TableId},
};

pub const BLOCK_BLOB_TABLE: BlobTableId = BlobTableId::new("block_blob");

pub struct FamilyTableIds {
    /// Versioned per-family row-codec dictionary store: `version (u32 BE)`
    /// -> dict bytes. Shared by transfers via [`Family::Trace`].
    pub dict_by_version: TableId,
    pub dir_by_block: ScannableTableId,
    pub dir_bucket: TableId,
    pub bitmap_by_block: ScannableTableId,
    pub bitmap_page_blob: TableId,
    pub bitmap_page_counts: TableId,
    pub open_bitmap_stream: ScannableTableId,
    /// Standby seal-chain rows: `span_start (u64 BE)` -> chained 32-byte seal
    /// digest (see [`crate::engine::digest`]). Written in the same batch as
    /// the span's sealed artifacts.
    pub seal_chain: TableId,
    /// Cache window-stats label for the per-family block-header cache, whose
    /// backing `TableId` ("block_metadata") is shared with `BlockTables`'
    /// record cache. A label only — nothing is provisioned under this name.
    pub block_metadata_cache_label: &'static str,
}

macro_rules! family_table_ids {
    ($prefix:literal) => {
        FamilyTableIds {
            dict_by_version: TableId::new(concat!($prefix, "_dict_by_version")),
            dir_by_block: ScannableTableId::new(concat!($prefix, "_dir_by_block")),
            dir_bucket: TableId::new(concat!($prefix, "_dir_bucket")),
            bitmap_by_block: ScannableTableId::new(concat!($prefix, "_bitmap_by_block")),
            bitmap_page_blob: TableId::new(concat!($prefix, "_bitmap_page_blob")),
            bitmap_page_counts: TableId::new(concat!($prefix, "_bitmap_page_counts")),
            open_bitmap_stream: ScannableTableId::new(concat!($prefix, "_open_bitmap_stream")),
            seal_chain: TableId::new(concat!($prefix, "_seal_chain")),
            block_metadata_cache_label: concat!($prefix, "_block_metadata"),
        }
    };
}

/// Indexed record family served by the engine. The variant set is the
/// canonical list of domains the engine supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Family {
    Log,
    Tx,
    Trace,
}

impl Family {
    /// Every family in the canonical `Log`, `Tx`, `Trace` order.
    pub const ALL: [Family; 3] = [Family::Log, Family::Tx, Family::Trace];

    pub const fn table_ids(self) -> FamilyTableIds {
        match self {
            Family::Log => family_table_ids!("log"),
            Family::Tx => family_table_ids!("tx"),
            Family::Trace => family_table_ids!("trace"),
        }
    }

    /// The block's primary-id window for this family. Every `BlockRecord`
    /// carries all three windows (zero-count when the block has no rows).
    pub fn window_in(self, block: &BlockRecord) -> FamilyWindowRecord {
        match self {
            Family::Log => block.logs,
            Family::Tx => block.txs,
            Family::Trace => block.traces,
        }
    }
}

/// One value per [`Family`], stored in the canonical `Log`, `Tx`, `Trace`
/// order. The typed accessors make passing the wrong family's slot alongside a
/// `Family` tag a compile-time impossibility instead of a silent bug.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PerFamily<T> {
    pub log: T,
    pub tx: T,
    pub trace: T,
}

impl<T> PerFamily<T> {
    pub fn new(log: T, tx: T, trace: T) -> Self {
        Self { log, tx, trace }
    }

    pub fn get(&self, family: Family) -> &T {
        match family {
            Family::Log => &self.log,
            Family::Tx => &self.tx,
            Family::Trace => &self.trace,
        }
    }

    pub fn get_mut(&mut self, family: Family) -> &mut T {
        match family {
            Family::Log => &mut self.log,
            Family::Tx => &mut self.tx,
            Family::Trace => &mut self.trace,
        }
    }

    /// Iterate in the fixed [`Family::ALL`] order (also the snapshot codec's
    /// field order — see `ingest::snapshot`).
    pub fn iter(&self) -> impl Iterator<Item = (Family, &T)> {
        [
            (Family::Log, &self.log),
            (Family::Tx, &self.tx),
            (Family::Trace, &self.trace),
        ]
        .into_iter()
    }

    /// Mutable [`Self::iter`], same fixed order.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (Family, &mut T)> {
        [
            (Family::Log, &mut self.log),
            (Family::Tx, &mut self.tx),
            (Family::Trace, &mut self.trace),
        ]
        .into_iter()
    }

    /// Apply `f` to every slot, preserving the family layout.
    pub fn map<U>(self, mut f: impl FnMut(T) -> U) -> PerFamily<U> {
        PerFamily {
            log: f(self.log),
            tx: f(self.tx),
            trace: f(self.trace),
        }
    }
}
