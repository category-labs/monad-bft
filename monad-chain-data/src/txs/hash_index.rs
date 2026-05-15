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
    error::Result,
    family::Hash32,
    store::{
        blob::BlobStore, CacheConfig, CachedKvTable, MetaStore, TableId, WriteSession,
    },
    txs::types::TxLocation,
};

pub struct TxHashIndexTable<M: MetaStore> {
    table: CachedKvTable<M>,
}

impl<M: MetaStore> TxHashIndexTable<M> {
    pub const TABLE: TableId = TableId::new("tx_hash_index");

    pub(crate) fn new(meta_store: M, cache: CacheConfig) -> Self {
        Self {
            table: CachedKvTable::new(meta_store.table(Self::TABLE), cache.tx_hash_index_entries),
        }
    }

    pub(crate) async fn get(&self, tx_hash: &Hash32) -> Result<Option<TxLocation>> {
        let Some(bytes) = self.table.get(tx_hash.as_slice()).await? else {
            return Ok(None);
        };
        TxLocation::decode(bytes.as_ref()).map(Some)
    }

    pub(crate) fn cached_table(&self) -> &CachedKvTable<M> {
        &self.table
    }

    pub(crate) fn stage_put<B: BlobStore>(
        &self,
        w: &WriteSession<'_, M, B>,
        tx_hash: &Hash32,
        location: TxLocation,
    ) {
        w.put(
            Self::TABLE,
            tx_hash.as_slice(),
            Bytes::copy_from_slice(&location.encode()),
        );
    }
}
