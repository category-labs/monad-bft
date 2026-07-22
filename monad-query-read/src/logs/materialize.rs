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

use std::collections::HashSet;

use alloy_primitives::{Address, B256};
use monad_query_engine::{
    bitmap::IndexKind,
    clause::{set_allows, IndexedClause, IndexedFilter},
    family::Family,
    query::{family_runner::IndexedFamilyQuery, row_cache::RowCache},
    tables::Tables,
};
use monad_query_errors::{QueryError, Result};
use monad_query_primitives::{limits::QueryEnvelope, records::BlockRecord, refs::BlockSpan};
use monad_query_store::{BlobStore, MetaStore};

use super::{LogEntry, StoredLog};
use crate::{blocks::Block, txs::TxEntry};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogFilter {
    pub address: Option<HashSet<Address>>,
    pub topics: [Option<HashSet<B256>>; 4],
}

/// Opt-in relations joined onto a logs query response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogsRelations {
    /// Populate `QueryLogsResponse::blocks` for the blocks that
    /// contributed logs in this page.
    pub blocks: bool,
    /// Populate `QueryLogsResponse::transactions` for the
    /// `(block_number, tx_index)` pairs carried on this page's logs.
    pub transactions: bool,
}

/// Public log query in queryX spec semantics: inclusive bounds whose
/// lower/upper roles depend on `order`; omitted bounds default per order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueryLogsRequest {
    pub envelope: QueryEnvelope,
    pub filter: LogFilter,
    pub relations: LogsRelations,
}

/// `span` mirrors the resolved bounds and records the last block scanned.
/// An empty `logs` means no next page; do not advance the cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLogsResponse {
    pub logs: Vec<LogEntry>,
    /// Deduped, ascending by block number; `None` unless `relations.blocks`.
    pub blocks: Option<Vec<Block>>,
    /// Deduped, ascending by `(block_number, tx_index)`; `None` unless
    /// `relations.transactions`.
    pub transactions: Option<Vec<TxEntry>>,
    pub span: BlockSpan,
}

impl IndexedFilter for LogFilter {
    type Record = LogEntry;

    fn indexed_clauses(&self) -> Vec<IndexedClause> {
        let mut clauses = Vec::new();

        if let Some(v) = &self.address {
            clauses.push(IndexedClause::from_set(IndexKind::Addr, v));
        }

        for (position, topics) in self.topics.iter().enumerate() {
            if let Some(v) = topics {
                clauses.push(IndexedClause::from_set(IndexKind::TOPICS[position], v));
            }
        }

        clauses
    }

    fn matches(&self, log: &LogEntry) -> bool {
        if !set_allows(&self.address, Some(&log.address)) {
            return false;
        }

        for (i, candidates) in self.topics.iter().enumerate() {
            if !set_allows(candidates, log.topics.get(i)) {
                return false;
            }
        }

        true
    }
}

pub struct LogMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
}

impl<'a, M: MetaStore, B: BlobStore> LogMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self { tables }
    }
}

impl<'a, M: MetaStore, B: BlobStore> IndexedFamilyQuery for LogMaterializer<'a, M, B> {
    type Meta = M;
    type Blob = B;
    type Filter = LogFilter;
    type Record = LogEntry;
    type StoredRow = StoredLog;

    fn family() -> Family {
        Family::Log
    }

    fn tables(&self) -> &Tables<M, B> {
        self.tables
    }

    fn row_cache(&self) -> &RowCache<StoredLog> {
        &self.tables.row_caches().logs
    }

    fn decode_stored(bytes: &[u8]) -> Result<StoredLog> {
        StoredLog::decode(bytes)
    }

    fn decode_external_container(
        container_idx: usize,
        row_base: usize,
        _tx_status: bool,
        bytes: &[u8],
    ) -> Result<Vec<StoredLog>> {
        // One `ReceiptWithLogIndex` item per container (= per tx).
        let tx_index =
            u32::try_from(container_idx).map_err(|_| QueryError::Decode("tx index overflow"))?;
        let first_log_index =
            u32::try_from(row_base).map_err(|_| QueryError::Decode("log index overflow"))?;
        crate::external::decode_external_receipt_logs(bytes, tx_index, first_log_index)
    }

    fn into_record_owned(
        stored: StoredLog,
        block_record: &BlockRecord,
        _idx_in_block: usize,
    ) -> Result<LogEntry> {
        Ok(stored.into_log_entry(block_record.block_number, block_record.block_hash))
    }
}
