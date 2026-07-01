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

use std::sync::Arc;

use alloy_consensus::Header;
use alloy_primitives::BlockHash;
use async_trait::async_trait;
use monad_eth_types::TxEnvelopeWithSender;
use monad_triedb_utils::triedb_env::{BlockKey, FinalizedBlockKey, ProposedBlockKey, Triedb};
use monad_types::SeqNum;

use super::{
    BlockCommitState, BlockPointer, DataSourceError, DataSourceResult, HistoricalDataSource,
    HistoricalDataSourceExt,
};

#[derive(Debug)]
pub struct TriedbDataSource<T> {
    triedb: Arc<T>,
}

impl<T> Clone for TriedbDataSource<T> {
    fn clone(&self) -> Self {
        Self {
            triedb: self.triedb.clone(),
        }
    }
}

impl<T> TriedbDataSource<T> {
    pub fn new(triedb: Arc<T>) -> Self {
        Self { triedb }
    }
}

fn resolve_block_key(pointer: &BlockPointer) -> BlockKey {
    match pointer {
        BlockPointer::Finalized(block_num) => {
            BlockKey::Finalized(FinalizedBlockKey(SeqNum(*block_num)))
        }
        BlockPointer::NonFinalized(block_num, block_id) => {
            BlockKey::Proposed(ProposedBlockKey(SeqNum(*block_num), *block_id))
        }
    }
}

fn block_key_to_block_pointer(block_key: BlockKey) -> BlockPointer {
    match block_key {
        BlockKey::Finalized(FinalizedBlockKey(SeqNum(block_num))) => {
            BlockPointer::Finalized(block_num)
        }
        BlockKey::Proposed(ProposedBlockKey(SeqNum(block_num), block_hash)) => {
            BlockPointer::NonFinalized(block_num, block_hash)
        }
    }
}

#[async_trait]
impl<T> HistoricalDataSource for TriedbDataSource<T>
where
    T: Triedb + Send + Sync,
{
    async fn try_resolve_block_commit_state(
        &self,
        commit_state: BlockCommitState,
    ) -> DataSourceResult<Option<BlockPointer>> {
        let block_key = match commit_state {
            BlockCommitState::Proposed => self.triedb.get_latest_proposed_block_key(),
            BlockCommitState::Voted => self.triedb.get_latest_voted_block_key(),
            BlockCommitState::Finalized => {
                BlockKey::Finalized(self.triedb.get_latest_finalized_block_key())
            }
        };

        Ok(Some(block_key_to_block_pointer(block_key)))
    }

    async fn try_resolve_block_number(
        &self,
        block_number: u64,
    ) -> DataSourceResult<Option<BlockPointer>> {
        Ok(self
            .triedb
            .get_block_key(SeqNum(block_number))
            .map(block_key_to_block_pointer))
    }

    async fn try_resolve_block_hash(
        &self,
        block_hash: BlockHash,
    ) -> DataSourceResult<Option<BlockPointer>> {
        let latest_block_key = self.triedb.get_latest_proposed_block_key();

        let Some(block_number) = self
            .triedb
            .get_block_number_by_hash(latest_block_key, block_hash.0)
            .await
            .map_err(DataSourceError::Internal)?
        else {
            return Ok(None);
        };

        self.try_resolve_block_number(block_number).await
    }

    async fn get_block(
        &self,
        pointer: BlockPointer,
    ) -> DataSourceResult<Option<(Header, Vec<TxEnvelopeWithSender>)>> {
        let block_key = resolve_block_key(&pointer);

        let Some(header) = self
            .triedb
            .get_block_header(block_key)
            .await
            .map_err(DataSourceError::Internal)?
        else {
            return Ok(None);
        };

        let Ok(transactions) = self.triedb.get_transactions(block_key).await else {
            // During statesync, block headers might be available in triedb while transactions are
            // not. Ignoring this error allows the DataSourceStack to continue to the next source.
            return Ok(None);
        };

        Ok(Some((header.header, transactions)))
    }

    async fn get_block_header(&self, pointer: BlockPointer) -> DataSourceResult<Option<Header>> {
        let block_key = resolve_block_key(&pointer);

        let header = self
            .triedb
            .get_block_header(block_key)
            .await
            .map_err(DataSourceError::Internal)?;

        Ok(header.map(|h| h.header))
    }
}

impl<T> HistoricalDataSourceExt for TriedbDataSource<T> where T: Triedb + Send + Sync {}
