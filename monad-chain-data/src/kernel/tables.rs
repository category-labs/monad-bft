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
    error::Result,
    logs::LogBlockHeader,
    primitives::state::{BlockRecord, PublicationState},
    store::{BlobStore, BlobTable, BlobTableId, KvTable, MetaStore, TableId},
};

pub struct Tables<M: MetaStore, B: BlobStore> {
    publication: PublicationTables<M>,
    blocks: BlockTables<M>,
    logs: LogTables<M, B>,
}

impl<M: MetaStore, B: BlobStore> Tables<M, B> {
    pub fn new(meta_store: M, blob_store: B) -> Self {
        Self {
            publication: PublicationTables::new(meta_store.clone()),
            blocks: BlockTables::new(meta_store.clone()),
            logs: LogTables::new(meta_store, blob_store),
        }
    }

    pub fn publication(&self) -> &PublicationTables<M> {
        &self.publication
    }

    pub fn blocks(&self) -> &BlockTables<M> {
        &self.blocks
    }

    pub fn logs(&self) -> &LogTables<M, B> {
        &self.logs
    }
}

pub struct PublicationTables<M: MetaStore> {
    publication_state: KvTable<M>,
}

impl<M: MetaStore> PublicationTables<M> {
    pub const PUBLICATION_STATE_TABLE: TableId = TableId::new("publication_state");
    pub const PUBLICATION_STATE_KEY: &[u8] = b"state";

    fn new(meta_store: M) -> Self {
        Self {
            publication_state: meta_store.table(Self::PUBLICATION_STATE_TABLE),
        }
    }

    pub async fn load_published_head(&self) -> Result<Option<u64>> {
        let _ = &self.publication_state;
        todo!("load publication-state metadata for the current head")
    }

    pub async fn store_state(&self, state: PublicationState) -> Result<()> {
        let _ = (&self.publication_state, state);
        todo!("store publication-state metadata")
    }
}

pub struct BlockTables<M: MetaStore> {
    block_records: KvTable<M>,
}

impl<M: MetaStore> BlockTables<M> {
    pub const BLOCK_RECORD_TABLE: TableId = TableId::new("block_record");

    fn new(meta_store: M) -> Self {
        Self {
            block_records: meta_store.table(Self::BLOCK_RECORD_TABLE),
        }
    }

    pub async fn load_record(&self, block_number: u64) -> Result<Option<BlockRecord>> {
        let _ = (&self.block_records, block_number);
        todo!("load a shared block record by block number")
    }

    pub async fn store_record(&self, block_number: u64, block_record: &BlockRecord) -> Result<()> {
        let _ = (&self.block_records, block_number, block_record);
        todo!("store a shared block record by block number")
    }
}

pub struct LogTables<M: MetaStore, B: BlobStore> {
    block_headers: KvTable<M>,
    block_blobs: BlobTable<B>,
}

impl<M: MetaStore, B: BlobStore> LogTables<M, B> {
    pub const BLOCK_LOG_HEADER_TABLE: TableId = TableId::new("block_log_header");
    pub const BLOCK_LOG_BLOB_TABLE: BlobTableId = BlobTableId::new("block_log_blob");

    fn new(meta_store: M, blob_store: B) -> Self {
        Self {
            block_headers: meta_store.table(Self::BLOCK_LOG_HEADER_TABLE),
            block_blobs: blob_store.table(Self::BLOCK_LOG_BLOB_TABLE),
        }
    }

    pub async fn load_block_header(&self, block_number: u64) -> Result<Option<LogBlockHeader>> {
        let _ = (&self.block_headers, block_number);
        todo!("load a logs block header by block number")
    }

    pub async fn store_block_header(
        &self,
        block_number: u64,
        block_log_header: &LogBlockHeader,
    ) -> Result<()> {
        let _ = (&self.block_headers, block_number, block_log_header);
        todo!("store a logs block header by block number")
    }

    pub async fn load_block_blob(&self, block_number: u64) -> Result<Option<bytes::Bytes>> {
        let _ = (&self.block_blobs, block_number);
        todo!("load a logs block blob by block number")
    }

    pub async fn store_block_blob(&self, block_number: u64, block_log_blob: Vec<u8>) -> Result<()> {
        let _ = (&self.block_blobs, block_number, block_log_blob);
        todo!("store a logs block blob by block number")
    }
}
