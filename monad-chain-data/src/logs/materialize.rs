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

use super::{LogBlockHeader, LogEntry, RawLogEntry};
use crate::{
    error::{MonadChainDataError, Result},
    family::Hash32,
    kernel::{bitmap::sharded_stream_id, tables::Tables},
    primitives::{
        page::{QueryOrder, DEFAULT_QUERY_LIMIT},
        refs::BlockRef,
        state::BlockRecord,
    },
    store::{BlobStore, MetaStore},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogFilter {
    pub address: Option<HashSet<Address>>,
    pub topics: [Option<HashSet<B256>>; 4],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IndexedLogClause {
    Address(Vec<Address>),
    Topic { position: usize, values: Vec<B256> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLogsRequest {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub order: QueryOrder,
    /// Target number of primary log objects to return.
    ///
    /// The server always completes the current block before stopping, so the
    /// actual count may exceed this value. The server may also return fewer if
    /// an internal constraint is reached. Defaults to
    /// [`DEFAULT_QUERY_LIMIT`] (100).
    pub limit: usize,
    pub filter: LogFilter,
}

impl Default for QueryLogsRequest {
    fn default() -> Self {
        Self {
            from_block: None,
            to_block: None,
            order: QueryOrder::default(),
            limit: DEFAULT_QUERY_LIMIT,
            filter: LogFilter::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLogsResponse {
    pub logs: Vec<LogEntry>,
    pub from_block: BlockRef,
    pub to_block: BlockRef,
    pub cursor_block: BlockRef,
}

impl LogFilter {
    pub fn has_indexed_clause(&self) -> bool {
        self.address.is_some() || self.topics.iter().any(Option::is_some)
    }

    pub(crate) fn indexed_clauses(&self) -> Vec<IndexedLogClause> {
        let mut clauses = Vec::new();

        if let Some(addresses) = &self.address {
            clauses.push(IndexedLogClause::Address(
                addresses.iter().copied().collect(),
            ));
        }

        for (position, topics) in self.topics.iter().enumerate() {
            if let Some(values) = topics {
                clauses.push(IndexedLogClause::Topic {
                    position,
                    values: values.iter().copied().collect(),
                });
            }
        }

        clauses
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

impl IndexedLogClause {
    pub(crate) fn stream_ids_for_shard(&self, shard: u64) -> Vec<String> {
        match self {
            Self::Address(addresses) => addresses
                .iter()
                .map(|address| sharded_stream_id("addr", address.as_slice(), shard))
                .collect(),
            Self::Topic { position, values } => {
                let index_kind = match position {
                    0 => "topic0",
                    1 => "topic1",
                    2 => "topic2",
                    3 => "topic3",
                    _ => return Vec::new(),
                };

                values
                    .iter()
                    .map(|topic| sharded_stream_id(index_kind, topic.as_slice(), shard))
                    .collect()
            }
        }
    }
}

pub struct LogMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
}

impl<'a, M: MetaStore, B: BlobStore> LogMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self { tables }
    }

    pub async fn load_block_ref(&self, block_number: u64) -> Result<BlockRef> {
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        Ok(BlockRef::from(&block_record))
    }

    pub async fn load_log_at(&self, block_number: u64, log_idx: usize) -> Result<LogEntry> {
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        let header = self
            .tables
            .logs()
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log header"))?;

        if log_idx + 1 >= header.offsets.len() {
            return Err(MonadChainDataError::Decode("log index out of range"));
        }
        let start = header.offsets[log_idx] as usize;
        let end = header.offsets[log_idx + 1] as usize;

        let bytes = self
            .tables
            .logs()
            .read_block_blob_range(block_number, start, end)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log blob"))?;

        let raw = RawLogEntry::decode(&bytes)?;
        Ok(raw.into_log_entry(block_record.block_number, block_record.block_hash))
    }

    pub async fn load_filtered_block_logs_for_block(
        &self,
        block_number: u64,
        request: &QueryLogsRequest,
    ) -> Result<(BlockRef, Vec<LogEntry>)> {
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        let block_ref = BlockRef::from(&block_record);

        let header = self
            .tables
            .logs()
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log header"))?;
        let blob = self
            .tables
            .logs()
            .load_block_blob(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log blob"))?;

        let logs = load_filtered_block_logs(&header, &blob, &block_record, request)?;

        Ok((block_ref, logs))
    }
}

fn load_filtered_block_logs(
    header: &LogBlockHeader,
    blob: &Bytes,
    block_record: &BlockRecord,
    request: &QueryLogsRequest,
) -> Result<Vec<LogEntry>> {
    let count = header.log_count();
    let indices: Box<dyn Iterator<Item = usize>> = match request.order {
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
        if request.filter.matches(&log) {
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
