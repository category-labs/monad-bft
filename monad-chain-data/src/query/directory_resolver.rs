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

use std::collections::{hash_map::Entry, HashMap};

use crate::{
    error::Result,
    kernel::{
        primary_dir::{bucket_start, PrimaryDirFragment},
        tables::Tables,
    },
    primitives::state::LogId,
    store::{BlobStore, MetaStore},
};

/// Resolves a global log ID to its block number and position within that block.
///
/// Caches directory bucket fragments to avoid redundant lookups when
/// resolving many IDs from the same region.
pub(super) struct LogIdResolver<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
    fragment_cache: HashMap<u64, Vec<PrimaryDirFragment>>,
}

impl<'a, M: MetaStore, B: BlobStore> LogIdResolver<'a, M, B> {
    pub(super) fn new(tables: &'a Tables<M, B>) -> Self {
        Self {
            tables,
            fragment_cache: HashMap::new(),
        }
    }

    pub(super) async fn resolve(&mut self, log_id: LogId) -> Result<Option<ResolvedLogLocation>> {
        let bucket = bucket_start(log_id.as_u64());
        if let Entry::Vacant(entry) = self.fragment_cache.entry(bucket) {
            entry.insert(self.tables.logs().load_bucket_fragments(bucket).await?);
        }

        let Some(fragments) = self.fragment_cache.get(&bucket) else {
            return Ok(None);
        };
        let Some(fragment) = fragments.iter().find(|fragment| {
            log_id.as_u64() >= fragment.first_primary_id
                && log_id.as_u64() < fragment.end_primary_id_exclusive
        }) else {
            return Ok(None);
        };

        Ok(Some(ResolvedLogLocation {
            block_number: fragment.block_number,
            log_block_idx: log_id.idx_in_block(LogId::new(fragment.first_primary_id))?,
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ResolvedLogLocation {
    pub(super) block_number: u64,
    pub(super) log_block_idx: usize,
}
