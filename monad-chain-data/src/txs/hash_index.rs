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
    store::{KvTable, MetaStore, TableId},
    txs::types::TxLocation,
};

pub struct TxHashIndexTable<M: MetaStore> {
    table: KvTable<M>,
}

impl<M: MetaStore> TxHashIndexTable<M> {
    pub const TABLE: TableId = TableId::new("tx_hash_index");

    pub(crate) fn new(meta_store: M) -> Self {
        Self {
            table: meta_store.table(Self::TABLE),
        }
    }

    pub(crate) async fn get(&self, tx_hash: &Hash32) -> Result<Option<TxLocation>> {
        let Some(record) = self.table.get(tx_hash.as_slice()).await? else {
            return Ok(None);
        };
        TxLocation::decode(record.value.as_ref()).map(Some)
    }

    pub(crate) async fn put(&self, tx_hash: &Hash32, location: TxLocation) -> Result<()> {
        self.table
            .put(
                tx_hash.as_slice(),
                Bytes::copy_from_slice(&location.encode()),
            )
            .await?;
        Ok(())
    }
}
