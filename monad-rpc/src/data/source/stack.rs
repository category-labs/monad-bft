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

use async_trait::async_trait;

use super::{
    BlockCommitState, BlockPointer, DataSourceError, DataSourceResult, HistoricalDataSource,
};

#[derive(Clone)]
pub struct DataSourceStack<T> {
    sources: Box<[T]>,
}

impl<T> DataSourceStack<T> {
    pub fn new(sources: Box<[T]>) -> Self {
        Self { sources }
    }
}

async fn execute_on_sources<S, T>(
    sources: &Box<[S]>,
    f: impl AsyncFn(&S) -> DataSourceResult<Option<T>>,
) -> DataSourceResult<Option<T>> {
    for source in sources {
        match f(source).await {
            Ok(Some(value)) => return Ok(Some(value)),
            Ok(None) => continue,
            Err(err) => match err {
                DataSourceError::Internal(err) => return Err(DataSourceError::Internal(err)),
            },
        }
    }

    Ok(None)
}

#[async_trait]
impl<T> HistoricalDataSource for DataSourceStack<T>
where
    T: HistoricalDataSource + Clone,
{
    async fn try_resolve_block_commit_state(
        &self,
        commit_state: BlockCommitState,
    ) -> DataSourceResult<Option<BlockPointer>> {
        execute_on_sources(&self.sources, async move |source| {
            source.try_resolve_block_commit_state(commit_state).await
        })
        .await
    }

    async fn try_resolve_block_number(
        &self,
        block_number: u64,
    ) -> DataSourceResult<Option<BlockPointer>> {
        execute_on_sources(&self.sources, async move |source| {
            source.try_resolve_block_number(block_number).await
        })
        .await
    }

    async fn try_resolve_block_hash(
        &self,
        block_hash: alloy_primitives::BlockHash,
    ) -> DataSourceResult<Option<BlockPointer>> {
        execute_on_sources(&self.sources, async move |source| {
            source.try_resolve_block_hash(block_hash).await
        })
        .await
    }

    async fn get_block(
        &self,
        pointer: BlockPointer,
    ) -> DataSourceResult<
        Option<(
            alloy_consensus::Header,
            Vec<monad_eth_types::TxEnvelopeWithSender>,
        )>,
    > {
        execute_on_sources(&self.sources, async move |source| {
            source.get_block(pointer).await
        })
        .await
    }

    async fn get_block_header(
        &self,
        pointer: BlockPointer,
    ) -> DataSourceResult<Option<alloy_consensus::Header>> {
        execute_on_sources(&self.sources, async move |source| {
            source.get_block_header(pointer).await
        })
        .await
    }
}
