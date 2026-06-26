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
    ingest_types::Hash32,
    store::{blob::BlobStore, CacheConfig, CachedKvTable, MetaStore, TableId, WriteSession},
    txs::TxLocation,
};

pub struct TxHashIndexTable<M: MetaStore> {
    table: CachedKvTable<M, TxLocation>,
}

fn decode_tx_location(bytes: Bytes) -> Result<TxLocation> {
    TxLocation::decode(bytes.as_ref())
}

impl<M: MetaStore> TxHashIndexTable<M> {
    pub const TABLE: TableId = TableId::new("tx_hash_index");

    pub fn new(meta_store: M, cache: CacheConfig) -> Self {
        Self {
            table: CachedKvTable::new(
                meta_store.table(Self::TABLE),
                cache.tx_hash_index_cache_bytes,
                decode_tx_location,
            ),
        }
    }

    pub async fn get(&self, tx_hash: &Hash32) -> Result<Option<TxLocation>> {
        self.table.get(tx_hash.as_slice()).await
    }

    pub(crate) fn collect_window_stats(&self, out: &mut Vec<(&'static str, u64, u64)>) {
        crate::engine::tables::collect_kv_stats(out, &self.table);
    }

    pub fn stage_put<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        tx_hash: &Hash32,
        location: TxLocation,
    ) {
        w.put(
            &self.table,
            tx_hash.as_slice(),
            Bytes::copy_from_slice(&location.encode()),
        );
    }
}
