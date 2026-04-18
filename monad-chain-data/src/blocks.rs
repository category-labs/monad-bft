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

use crate::{
    error::{MonadChainDataError, Result},
    family::Hash32,
    kernel::tables::{BlockTables, Tables},
    logs::LogEntry,
    primitives::{
        limits::QueryEnvelope,
        range::ResolvedBlockWindow,
        refs::{BlockRef, BlockSpan},
        state::BlockRecord,
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

/// Public block query in queryX spec semantics: `from_block`/`to_block` are
/// the inclusive range start/end, with the lower/upper roles depending on
/// `order`. Omitted bounds default to `"earliest"`/`"latest"` per order.
/// The spec does not support filters or relations on `eth_queryBlocks`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryBlocksRequest {
    pub envelope: QueryEnvelope,
}

/// The span mirrors the resolved request bounds and records the last
/// block returned. When `blocks` is empty there is no next page; callers
/// should treat the response as terminal rather than advance the cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryBlocksResponse {
    pub blocks: Vec<Block>,
    pub span: BlockSpan,
}

pub async fn execute_query_blocks<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryBlocksRequest,
    window: ResolvedBlockWindow,
) -> Result<QueryBlocksResponse> {
    let (request_from, request_to) = window.request_endpoints(request.envelope.order);
    let blocks_table = tables.blocks();
    let mut blocks = Vec::new();
    let mut cursor_block = request_from;

    for block_number in window.iter(request.envelope.order) {
        if blocks.len() >= request.envelope.limit {
            break;
        }
        let (block, record) = load_block_with_record(blocks_table, block_number).await?;
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

/// Loads the blocks referenced by `logs` in ascending block-number order,
/// deduped. Used to fulfill the `blocks` relation on a logs query when the
/// caller opts in.
pub async fn load_blocks_for_logs<M: MetaStore>(
    blocks: &BlockTables<M>,
    logs: &[LogEntry],
) -> Result<Vec<Block>> {
    let distinct: BTreeSet<u64> = logs.iter().map(|l| l.block_number).collect();
    let mut out = Vec::with_capacity(distinct.len());
    for number in distinct {
        out.push(load_block(blocks, number).await?);
    }
    Ok(out)
}

pub async fn load_block<M: MetaStore>(blocks: &BlockTables<M>, number: u64) -> Result<Block> {
    Ok(load_block_with_record(blocks, number).await?.0)
}

async fn load_block_with_record<M: MetaStore>(
    blocks: &BlockTables<M>,
    number: u64,
) -> Result<(Block, BlockRecord)> {
    let record = blocks
        .load_record(number)
        .await?
        .ok_or(MonadChainDataError::MissingData("missing block record"))?;
    let header = blocks
        .load_header(number)
        .await?
        .ok_or(MonadChainDataError::MissingData("missing block header"))?;
    Ok((
        Block {
            hash: record.block_hash,
            header,
        },
        record,
    ))
}
