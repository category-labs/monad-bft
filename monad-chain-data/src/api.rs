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
    family::FinalizedBlock,
    kernel::tables::Tables,
    logs::{QueryLogsRequest, QueryLogsResponse},
    primitives::state::BlockRecord,
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

    pub async fn ingest_block(&self, block: FinalizedBlock) -> Result<IngestOutcome> {
        let _ = block;
        todo!("implement minimal logs-only ingest pipeline")
    }

    pub async fn query_logs(&self, request: QueryLogsRequest) -> Result<QueryLogsResponse> {
        let _ = request;
        todo!("implement minimal block-scan query path")
    }
}
