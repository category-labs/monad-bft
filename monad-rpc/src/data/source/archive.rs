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

use alloy_consensus::Header;
use alloy_primitives::BlockHash;
use async_trait::async_trait;
use monad_archive::{model::BlockDataReader, prelude::ArchiveReader};
use monad_eth_types::TxEnvelopeWithSender;
use monad_types::{BlockId, Hash};

use super::{
    BlockCommitState, BlockPointer, DataSourceError, DataSourceResult, HistoricalDataSource,
};

#[derive(Clone)]
pub struct ArchiveDataSource {
    archive_reader: ArchiveReader,
}

impl ArchiveDataSource {
    pub fn new(archive_reader: ArchiveReader) -> Self {
        Self { archive_reader }
    }
}

async fn get_block_from_archive(
    archive_reader: &ArchiveReader,
    pointer: BlockPointer,
) -> DataSourceResult<Option<alloy_consensus::Block<TxEnvelopeWithSender>>> {
    match pointer {
        // Archive cannot serve non-finalized blocks.
        BlockPointer::NonFinalized(..) => Ok(None),

        BlockPointer::Finalized(block_num, _) => {
            // Archive is indexed by block number so we prefer using that method instead of
            // retrieving by hash.
            archive_reader.try_get_block_by_number(block_num).await
        }
    }
    .map_err(|e| DataSourceError::Internal(e.to_string()))
}

#[async_trait]
impl HistoricalDataSource for ArchiveDataSource {
    async fn try_resolve_block_commit_state(
        &self,
        _commit_state: BlockCommitState,
    ) -> DataSourceResult<Option<BlockPointer>> {
        // TODO(andr-dev): Allow resolving finalized from archive

        // Archive cannot resolve non-finalized block commit states
        Ok(None)
    }

    async fn try_resolve_block_number(
        &self,
        block_number: u64,
    ) -> DataSourceResult<Option<BlockPointer>> {
        // TODO(andr-dev): Archive currently assumes all block numbers are finalized, this needs to
        // be changed to enable archive before other sources that can serve non-finalized,
        // deferring to keep things simple for now.
        Ok(Some(BlockPointer::Finalized(block_number, None)))
    }

    async fn try_resolve_block_hash(
        &self,
        block_hash: BlockHash,
    ) -> DataSourceResult<Option<BlockPointer>> {
        let Some(block_number) = self
            .archive_reader
            .try_get_block_number_by_hash(&block_hash)
            .await
            .map_err(|e| DataSourceError::Internal(e.to_string()))?
        else {
            return Ok(None);
        };

        Ok(Some(BlockPointer::Finalized(
            block_number,
            Some(BlockId(Hash(block_hash.0))),
        )))
    }

    async fn get_block(
        &self,
        pointer: BlockPointer,
    ) -> DataSourceResult<Option<(Header, Vec<TxEnvelopeWithSender>)>> {
        let block = get_block_from_archive(&self.archive_reader, pointer).await?;

        Ok(block.map(|b| (b.header, b.body.transactions)))
    }

    async fn get_block_header(&self, pointer: BlockPointer) -> DataSourceResult<Option<Header>> {
        let block = get_block_from_archive(&self.archive_reader, pointer).await?;

        Ok(block.map(|b| b.header))
    }
}
