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

use std::collections::BTreeSet;

use futures::{stream, try_join, StreamExt, TryStreamExt};

use crate::{
    engine::tables::{BlockTables, Tables},
    error::{MonadChainDataError, Result},
    ingest_types::Hash32,
    primitives::{
        limits::QueryEnvelope,
        range::ResolvedBlockWindow,
        records::BlockRecord,
        refs::{BlockRef, BlockSpan},
        EvmBlockHeader,
    },
    store::{BlobStore, MetaStore},
};

/// A block as returned by queries: the full header payload plus the block
/// hash, which the header type itself does not carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub hash: Hash32,
    pub header: EvmBlockHeader,
}

/// Public block query in queryX spec semantics: inclusive bounds whose
/// lower/upper roles depend on `order`; omitted bounds default per order.
/// `eth_queryBlocks` supports no filters or relations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryBlocksRequest {
    pub envelope: QueryEnvelope,
}

/// `span` mirrors the resolved bounds and records the last block returned.
/// An empty `blocks` means no next page; do not advance the cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryBlocksResponse {
    pub blocks: Vec<Block>,
    pub span: BlockSpan,
}

pub(crate) async fn execute_query_blocks<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryBlocksRequest,
    window: ResolvedBlockWindow,
) -> Result<QueryBlocksResponse> {
    let (request_from, request_to) = window.request_endpoints(request.envelope.order);
    let blocks_table = tables.blocks();
    let mut blocks = Vec::new();
    let mut cursor_block = request_from;

    // Every block in the window must exist, so the page is exactly
    // `min(limit, window_len)` blocks: prefetch them concurrently in window
    // order (`buffered` preserves it), keeping cursor semantics identical to
    // a serial walk.
    let mut loaded = stream::iter(
        window
            .iter(request.envelope.order)
            .take(request.envelope.limit)
            .map(|block_number| load_block_with_record(blocks_table, block_number)),
    )
    .buffered(tables.query_config().materialize_concurrency);
    while let Some((block, record)) = loaded.try_next().await? {
        cursor_block = BlockRef::from(&record);
        blocks.push(block);
    }

    Ok(QueryBlocksResponse {
        blocks,
        span: BlockSpan {
            from_block: request_from,
            to_block: request_to,
            cursor_block,
        },
    })
}

/// Loads the blocks for the given numbers in ascending order, deduped.
/// Used to fulfill the `blocks` relation on family queries.
pub(crate) async fn load_blocks_by_numbers<M: MetaStore, I: IntoIterator<Item = u64>>(
    blocks: &BlockTables<M>,
    numbers: I,
    concurrency: usize,
) -> Result<Vec<Block>> {
    let distinct: BTreeSet<u64> = numbers.into_iter().collect();
    stream::iter(
        distinct
            .into_iter()
            .map(|number| load_block(blocks, number)),
    )
    .buffered(concurrency)
    .try_collect()
    .await
}

async fn load_block<M: MetaStore>(blocks: &BlockTables<M>, number: u64) -> Result<Block> {
    Ok(load_block_with_record(blocks, number).await?.0)
}

async fn load_block_with_record<M: MetaStore>(
    blocks: &BlockTables<M>,
    number: u64,
) -> Result<(Block, BlockRecord)> {
    let (record, header) = try_join!(blocks.load_record(number), blocks.load_header(number))?;
    let record = record.ok_or(MonadChainDataError::MissingData("missing block record"))?;
    let header = header.ok_or(MonadChainDataError::MissingData("missing block header"))?;
    Ok((
        Block {
            hash: record.block_hash,
            header,
        },
        record,
    ))
}
