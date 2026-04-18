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
    blocks::{execute_query_blocks, load_blocks_for_logs, QueryBlocksRequest, QueryBlocksResponse},
    engine::{
        ingest::{
            bitmap_compaction::compact_newly_sealed_log_bitmap_pages,
            directory_compaction::compact_newly_sealed_log_directory_buckets,
        },
        tables::Tables,
    },
    error::{MonadChainDataError, Result},
    family::FinalizedBlock,
    logs::{
        execute_block_scan_query, execute_indexed_log_query, LogIngestPlan, QueryLogsRequest,
        QueryLogsResponse,
    },
    primitives::{
        limits::QueryLimits,
        range::ResolvedBlockWindow,
        state::{BlockRecord, LogId},
    },
    store::{BlobStore, MetaStore},
};

pub struct MonadChainDataService<M: MetaStore, B: BlobStore> {
    tables: Tables<M, B>,
    limits: QueryLimits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    pub indexed_finalized_head: u64,
    pub block_record: BlockRecord,
    pub written_logs: usize,
}

impl<M: MetaStore, B: BlobStore> MonadChainDataService<M, B> {
    pub fn new(meta_store: M, blob_store: B, limits: QueryLimits) -> Self {
        Self {
            tables: Tables::new(meta_store, blob_store),
            limits,
        }
    }

    pub fn tables(&self) -> &Tables<M, B> {
        &self.tables
    }

    pub fn limits(&self) -> &QueryLimits {
        &self.limits
    }

    // The publication head is the commit boundary: every artifact written before
    // `store_state` is pre-publication state and must not be treated as valid by
    // readers until the head advances. Retry/recovery should derive the next block
    // from the published head and may overwrite any matching pre-publication
    // artifacts left by an interrupted ingest.
    /// Persists one finalized block and advances the published head on success.
    pub async fn ingest_block(&self, block: FinalizedBlock) -> Result<IngestOutcome> {
        let blocks = self.tables.blocks();
        let logs = self.tables.logs();
        let current_head = self.tables.publication().load_published_head().await?;
        let previous_record = blocks.validate_continuity(&block, current_head).await?;
        let next_log_id = match previous_record {
            Some(previous) => previous.logs.next_log_id()?,
            None => LogId::ZERO,
        };

        let LogIngestPlan {
            block_record,
            block_log_header,
            block_log_blob,
            bitmap_fragments,
            written_logs,
        } = LogIngestPlan::build(&block, next_log_id)?;
        let next_log_id_exclusive = block_record.logs.next_log_id()?;
        blocks
            .store_header(block.block_number(), &block.header)
            .await?;
        logs.store_block_blob(block.block_number(), block_log_blob)
            .await?;
        logs.store_block_header(block.block_number(), Bytes::from(block_log_header.encode()))
            .await?;
        logs.dir()
            .persist_block_fragment(
                block.block_number(),
                block_record.logs.first_log_id.as_u64(),
                block_record.logs.count,
            )
            .await?;
        for fragment in &bitmap_fragments {
            logs.store_bitmap_fragment(fragment, block.block_number())
                .await?;
        }
        compact_newly_sealed_log_directory_buckets(
            logs,
            next_log_id.as_u64(),
            next_log_id_exclusive.as_u64(),
        )
        .await?;
        compact_newly_sealed_log_bitmap_pages(
            logs,
            &bitmap_fragments,
            next_log_id.as_u64(),
            next_log_id_exclusive.as_u64(),
        )
        .await?;
        blocks
            .store_hash_index(&block.block_hash(), block.block_number())
            .await?;
        blocks
            .store_record(block.block_number(), &block_record)
            .await?;
        self.tables
            .publication()
            .store_state(crate::primitives::state::PublicationState {
                indexed_finalized_head: block.block_number(),
            })
            .await?;

        Ok(IngestOutcome {
            indexed_finalized_head: block.block_number(),
            block_record,
            written_logs,
        })
    }

    /// Executes a finalized logs query over the current published head.
    /// The service's configured `QueryLimits` bound the request shape and
    /// the resolved block-range span.
    pub async fn query_logs(&self, request: QueryLogsRequest) -> Result<QueryLogsResponse> {
        self.limits.check_limit(request.envelope.limit)?;

        let head = self.load_published_head().await?;
        let window = ResolvedBlockWindow::resolve(
            &request.envelope,
            head,
            &self.limits,
            self.tables.blocks(),
        )
        .await?;

        let mut response = if request.filter.has_indexed_clause() {
            execute_indexed_log_query(&self.tables, &request, window).await?
        } else {
            execute_block_scan_query(&self.tables, &request, window).await?
        };

        if request.relations.blocks {
            response.blocks =
                Some(load_blocks_for_logs(self.tables.blocks(), &response.logs).await?);
        }

        Ok(response)
    }

    /// Executes a finalized blocks query over the current published head.
    /// The service's configured `QueryLimits` bound the request shape and
    /// the resolved block-range span.
    pub async fn query_blocks(&self, request: QueryBlocksRequest) -> Result<QueryBlocksResponse> {
        self.limits.check_limit(request.envelope.limit)?;

        let head = self.load_published_head().await?;
        let window = ResolvedBlockWindow::resolve(
            &request.envelope,
            head,
            &self.limits,
            self.tables.blocks(),
        )
        .await?;

        execute_query_blocks(&self.tables, &request, window).await
    }

    async fn load_published_head(&self) -> Result<u64> {
        self.tables
            .publication()
            .load_published_head()
            .await?
            .ok_or(MonadChainDataError::MissingData("no published blocks"))
    }
}
