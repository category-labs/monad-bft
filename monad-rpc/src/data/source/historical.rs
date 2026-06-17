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

use std::{future::Future, pin::Pin};

use alloy_consensus::Header;
use alloy_primitives::BlockHash;
use async_trait::async_trait;
use auto_impl::auto_impl;
use dyn_clone::{clone_trait_object, DynClone};
use monad_eth_types::TxEnvelopeWithSender;

use super::{BlockCommitState, BlockPointer, DataSourceResult, DataSourceStack};
use crate::types::eth_json::{BlockTagOrHash, BlockTags};

/// Trait for read-only queries against historical chain data.
#[async_trait]
#[auto_impl(Box)]
pub trait HistoricalDataSource: DynClone + Send + Sync {
    async fn try_resolve(
        &self,
        block_tag_or_hash: BlockTagOrHash,
    ) -> DataSourceResult<Option<BlockPointer>> {
        match block_tag_or_hash {
            BlockTagOrHash::BlockTags(block_tags) => match block_tags {
                BlockTags::Number(block_number) => {
                    self.try_resolve_block_number(block_number.0).await
                }
                BlockTags::Latest => {
                    self.try_resolve_block_commit_state(BlockCommitState::Proposed)
                        .await
                }
                BlockTags::Safe => {
                    self.try_resolve_block_commit_state(BlockCommitState::Voted)
                        .await
                }
                BlockTags::Finalized => {
                    self.try_resolve_block_commit_state(BlockCommitState::Finalized)
                        .await
                }
            },
            BlockTagOrHash::Hash(hash) => {
                self.try_resolve_block_hash(BlockHash::from(hash.0)).await
            }
        }
    }

    async fn try_resolve_block_commit_state(
        &self,
        commit_state: BlockCommitState,
    ) -> DataSourceResult<Option<BlockPointer>>;

    async fn try_resolve_block_number(
        &self,
        block_number: u64,
    ) -> DataSourceResult<Option<BlockPointer>>;

    async fn try_resolve_block_hash(
        &self,
        block_hash: BlockHash,
    ) -> DataSourceResult<Option<BlockPointer>>;

    /// Fetch a block (header + transactions).
    async fn get_block(
        &self,
        pointer: BlockPointer,
    ) -> DataSourceResult<Option<(Header, Vec<TxEnvelopeWithSender>)>>;

    /// Fetch a block header.
    async fn get_block_header(&self, pointer: BlockPointer) -> DataSourceResult<Option<Header>>;
}

clone_trait_object!(HistoricalDataSource);

pub type HistoricalDataSourceStack = DataSourceStack<Box<dyn HistoricalDataSource>>;

pub trait HistoricalDataSourceExt: HistoricalDataSource + Sized {
    async fn try_resolve_and_then<T, F>(
        &self,
        block: BlockTagOrHash,
        f: F,
    ) -> DataSourceResult<Option<T>>
    where
        F: for<'s> Fn(
            &'s dyn HistoricalDataSource,
            BlockPointer,
        )
            -> Pin<Box<dyn Future<Output = DataSourceResult<Option<T>>> + Send + 's>>,
    {
        if let Some(pointer) = self.try_resolve(block).await? {
            f(self, pointer).await
        } else {
            Ok(None)
        }
    }
}
