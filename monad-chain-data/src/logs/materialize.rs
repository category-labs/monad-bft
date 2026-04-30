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

use alloy_primitives::{Address, Bytes, B256};
use bytes::Bytes as RawBytes;

use super::{LogBlockHeader, LogEntry, RawLogEntry};
use crate::{
    blocks::Block,
    engine::{
        clause::{IndexedClause, IndexedFilter},
        family::Family,
        query::family_runner::IndexedFamilyQuery,
        tables::Tables,
    },
    error::{MonadChainDataError, Result},
    family::Hash32,
    primitives::{
        limits::QueryEnvelope,
        page::QueryOrder,
        refs::{BlockRef, BlockSpan},
        state::BlockRecord,
    },
    store::{BlobStore, MetaStore},
    txs::TxEntry,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogFilter {
    pub address: Option<HashSet<Address>>,
    pub topics: [Option<HashSet<B256>>; 4],
}

/// Opt-in relations joined onto a logs query response. Each field is a
/// hint to the service; disabled relations leave the corresponding vec
/// empty in the response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogsRelations {
    /// When true, `QueryLogsResponse::blocks` is populated with deduped
    /// headers (sorted ascending by number) for the blocks that
    /// contributed logs in this page.
    pub blocks: bool,
    /// When true, `QueryLogsResponse::transactions` is populated with
    /// deduped txs (sorted ascending by block number, then tx index) for
    /// the `(block_number, tx_index)` pairs carried on the logs in this
    /// page.
    pub transactions: bool,
}

/// Public log query in queryX spec semantics: `from_block`/`to_block` are
/// the inclusive range start/end, with the lower/upper roles depending on
/// `order`. Omitted bounds default to `"earliest"`/`"latest"` per order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueryLogsRequest {
    pub envelope: QueryEnvelope,
    pub filter: LogFilter,
    pub relations: LogsRelations,
}

/// The span mirrors the resolved request bounds and records the last
/// block scanned. When `logs` is empty there is no next page; callers
/// should treat the response as terminal rather than advance the cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLogsResponse {
    pub logs: Vec<LogEntry>,
    /// Deduped blocks for the `logs` in this page, sorted ascending by
    /// block number. `None` unless `QueryLogsRequest::relations.blocks`.
    pub blocks: Option<Vec<Block>>,
    /// Deduped transactions for the `logs` in this page, sorted ascending
    /// by `(block_number, tx_index)`. `None` unless
    /// `QueryLogsRequest::relations.transactions`.
    pub transactions: Option<Vec<TxEntry>>,
    pub span: BlockSpan,
}

impl LogFilter {
    pub fn has_indexed_clause(&self) -> bool {
        self.address.is_some() || self.topics.iter().any(Option::is_some)
    }

    pub fn matches(&self, log: &LogEntry) -> bool {
        if let Some(addresses) = &self.address {
            if !addresses.contains(&log.address) {
                return false;
            }
        }

        for (i, candidates) in self.topics.iter().enumerate() {
            if let Some(candidates) = candidates {
                match log.topics.get(i) {
                    Some(actual) if candidates.contains(actual) => {}
                    _ => return false,
                }
            }
        }

        true
    }
}

impl IndexedFilter for LogFilter {
    type Record = LogEntry;

    fn indexed_clauses(&self) -> Vec<IndexedClause> {
        const TOPIC_KINDS: [&str; 4] = ["topic0", "topic1", "topic2", "topic3"];

        let mut clauses = Vec::new();

        if let Some(addresses) = &self.address {
            clauses.push(IndexedClause {
                kind: "addr",
                values: addresses
                    .iter()
                    .map(|a| RawBytes::copy_from_slice(a.as_slice()))
                    .collect(),
            });
        }

        for (position, topics) in self.topics.iter().enumerate() {
            if let Some(values) = topics {
                clauses.push(IndexedClause {
                    kind: TOPIC_KINDS[position],
                    values: values
                        .iter()
                        .map(|t| RawBytes::copy_from_slice(t.as_slice()))
                        .collect(),
                });
            }
        }

        clauses
    }

    fn matches(&self, log: &LogEntry) -> bool {
        LogFilter::matches(self, log)
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

impl<'a, M: MetaStore, B: BlobStore> IndexedFamilyQuery<M, B> for LogMaterializer<'a, M, B> {
    type Filter = LogFilter;
    type Record = LogEntry;

    fn family() -> Family {
        Family::Log
    }

    async fn load_block_ref(&self, block_number: u64) -> Result<BlockRef> {
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        Ok(BlockRef::from(&block_record))
    }

    async fn load_record_at(&self, block_number: u64, idx_in_block: usize) -> Result<LogEntry> {
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        let header_bytes = self
            .tables
            .family(Family::Log)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log header"))?;
        let header = LogBlockHeader::decode(&header_bytes)?;

        if idx_in_block + 1 >= header.offsets.len() {
            return Err(MonadChainDataError::Decode("log index out of range"));
        }
        let start = header.offsets[idx_in_block] as usize;
        let end = header.offsets[idx_in_block + 1] as usize;

        let bytes = self
            .tables
            .family(Family::Log)
            .read_block_blob_range(block_number, start, end)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log blob"))?;

        let raw = RawLogEntry::decode(&bytes)?;
        Ok(raw.into_log_entry(block_record.block_number, block_record.block_hash))
    }

    async fn load_filtered_block_records(
        &self,
        block_number: u64,
        order: QueryOrder,
        filter: &LogFilter,
    ) -> Result<(BlockRef, Vec<LogEntry>)> {
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        let block_ref = BlockRef::from(&block_record);

        let header_bytes = self
            .tables
            .family(Family::Log)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log header"))?;
        let header = LogBlockHeader::decode(&header_bytes)?;
        let blob = self
            .tables
            .family(Family::Log)
            .load_block_blob(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log blob"))?;

        let logs = load_filtered_block_logs(&header, &blob, &block_record, order, filter)?;

        Ok((block_ref, logs))
    }
}

fn load_filtered_block_logs(
    header: &LogBlockHeader,
    blob: &Bytes,
    block_record: &BlockRecord,
    order: QueryOrder,
    filter: &LogFilter,
) -> Result<Vec<LogEntry>> {
    let count = header.log_count();
    let indices: Box<dyn Iterator<Item = usize>> = match order {
        QueryOrder::Ascending => Box::new(0..count),
        QueryOrder::Descending => Box::new((0..count).rev()),
    };

    let mut logs = Vec::new();
    for log_idx in indices {
        let log = decode_log_at(
            header,
            blob.as_ref(),
            log_idx,
            block_record.block_number,
            block_record.block_hash,
        )?;
        if filter.matches(&log) {
            logs.push(log);
        }
    }

    Ok(logs)
}

pub(crate) fn decode_log_at(
    header: &LogBlockHeader,
    blob: &[u8],
    log_idx: usize,
    block_number: u64,
    block_hash: Hash32,
) -> Result<LogEntry> {
    if log_idx + 1 >= header.offsets.len() {
        return Err(MonadChainDataError::Decode("log index out of range"));
    }

    let start = header.offsets[log_idx] as usize;
    let end = header.offsets[log_idx + 1] as usize;
    if start > end || end > blob.len() {
        return Err(MonadChainDataError::Decode("invalid log range"));
    }

    let raw = RawLogEntry::decode(&blob[start..end])?;
    Ok(raw.into_log_entry(block_number, block_hash))
}
