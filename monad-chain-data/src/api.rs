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
    error::{MonadChainDataError, Result},
    family::FinalizedBlock,
    kernel::tables::Tables,
    logs::{LogIngestPlan, QueryLogsRequest, QueryLogsResponse},
    primitives::{
        range::ResolvedBlockWindow,
        state::{BlockRecord, LogId},
    },
    query::runner::execute_block_scan_query,
    store::{BlobStore, MetaStore},
};

pub struct MonadChainDataService<M: MetaStore, B: BlobStore> {
    tables: Tables<M, B>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    pub indexed_finalized_head: u64,
    pub block_record: BlockRecord,
    pub written_logs: usize,
}

impl<M: MetaStore, B: BlobStore> MonadChainDataService<M, B> {
    pub fn new(meta_store: M, blob_store: B) -> Self {
        Self {
            tables: Tables::new(meta_store, blob_store),
        }
    }

    pub fn tables(&self) -> &Tables<M, B> {
        &self.tables
    }

    // TODO: Individual writes are idempotent, but a partial failure leaves the block
    // incompletely written. Retry logic, logging, and metrics belong here once the
    // overall pipeline shape stabilizes.
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
            written_logs,
        } = LogIngestPlan::build(&block, next_log_id)?;
        logs.store_block_blob(block.block_number, block_log_blob)
            .await?;
        logs.store_block_header(block.block_number, &block_log_header)
            .await?;
        blocks
            .store_record(block.block_number, &block_record)
            .await?;
        self.tables
            .publication()
            .store_state(crate::primitives::state::PublicationState {
                indexed_finalized_head: block.block_number,
            })
            .await?;

        Ok(IngestOutcome {
            indexed_finalized_head: block.block_number,
            block_record,
            written_logs,
        })
    }

    pub async fn query_logs(&self, request: QueryLogsRequest) -> Result<QueryLogsResponse> {
        if request.limit == 0 {
            return Err(MonadChainDataError::InvalidRequest(
                "limit must be at least 1",
            ));
        }

        let head = self
            .tables
            .publication()
            .load_published_head()
            .await?
            .ok_or(MonadChainDataError::MissingData("no published blocks"))?;
        let window = ResolvedBlockWindow::resolve(&request, head, self.tables.blocks()).await?;

        // First pass: all log queries use the fallback block-scan path.
        // Indexed execution lands in later commits once log IDs and bitmap artifacts exist.
        execute_block_scan_query(&self.tables, &request, window).await
    }
}
