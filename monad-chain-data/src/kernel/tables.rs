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
    family::FinalizedBlock,
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

fn block_number_key(block_number: u64) -> [u8; 8] {
    block_number.to_be_bytes()
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
        let Some(record) = self
            .publication_state
            .get(Self::PUBLICATION_STATE_KEY)
            .await?
        else {
            return Ok(None);
        };

        Ok(Some(
            PublicationState::decode(&record.value)?.indexed_finalized_head,
        ))
    }

    pub async fn store_state(&self, state: PublicationState) -> Result<()> {
        self.publication_state
            .put(Self::PUBLICATION_STATE_KEY, Bytes::from(state.encode()))
            .await?;
        Ok(())
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
        let key = block_number_key(block_number);
        let Some(record) = self.block_records.get(&key).await? else {
            return Ok(None);
        };
        Ok(Some(BlockRecord::decode(&record.value)?))
    }

    pub async fn store_record(&self, block_number: u64, block_record: &BlockRecord) -> Result<()> {
        let key = block_number_key(block_number);
        self.block_records
            .put(&key, Bytes::from(block_record.encode()))
            .await?;
        Ok(())
    }

    pub async fn validate_continuity(
        &self,
        block: &FinalizedBlock,
        current_head: Option<u64>,
    ) -> Result<()> {
        match current_head {
            None => {
                if block.block_number != 1 {
                    return Err(crate::error::MonadChainDataError::InvalidRequest(
                        "first ingested block must be block 1 in the first pass",
                    ));
                }
            }
            Some(head) => {
                if block.block_number != head + 1 {
                    return Err(crate::error::MonadChainDataError::InvalidRequest(
                        "block_number must extend the published head contiguously",
                    ));
                }

                let previous = self.load_record(head).await?.ok_or(
                    crate::error::MonadChainDataError::MissingData("missing previous block record"),
                )?;
                if previous.block_hash != block.parent_hash {
                    return Err(crate::error::MonadChainDataError::InvalidRequest(
                        "parent_hash must match the previous published block",
                    ));
                }
            }
        }

        Ok(())
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
        let key = block_number_key(block_number);
        let Some(record) = self.block_headers.get(&key).await? else {
            return Ok(None);
        };
        Ok(Some(LogBlockHeader::decode(&record.value)?))
    }

    pub async fn store_block_header(
        &self,
        block_number: u64,
        block_log_header: &LogBlockHeader,
    ) -> Result<()> {
        let key = block_number_key(block_number);
        self.block_headers
            .put(&key, Bytes::from(block_log_header.encode()))
            .await?;
        Ok(())
    }

    pub async fn load_block_blob(
        &self,
        block_number: u64,
    ) -> Result<Option<alloy_primitives::Bytes>> {
        let key = block_number_key(block_number);
        Ok(self.block_blobs.get(&key).await?.map(Into::into))
    }

    pub async fn store_block_blob(&self, block_number: u64, block_log_blob: Vec<u8>) -> Result<()> {
        let key = block_number_key(block_number);
        self.block_blobs
            .put(&key, Bytes::from(block_log_blob))
            .await?;
        Ok(())
    }
}
