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

use std::{collections::HashSet, sync::Arc, time::Instant};

use alloy_primitives::{Address, B256};
use bytes::Bytes as RawBytes;
use zstd::dict::DecoderDictionary;

use super::{BlockBlobHeader, LogEntry, RawLogEntry};
use crate::{
    blocks::Block,
    engine::{
        clause::{IndexedClause, IndexedFilter},
        family::Family,
        query::family_runner::{
            coalesce_frame_ranges, execute_block_scan_family_query, execute_indexed_family_query,
            IndexedFamilyQuery, IndexedQueryStats, MATERIALIZE_WHOLE_REGION_MAX_BYTES,
            MATERIALIZE_WHOLE_REGION_SPAN_THRESHOLD,
        },
        row_codec::RowDecompressor,
        tables::Tables,
    },
    error::{MonadChainDataError, Result},
    family::Hash32,
    primitives::{
        limits::QueryEnvelope,
        page::QueryOrder,
        range::ResolvedBlockWindow,
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

impl<'a, M: MetaStore, B: BlobStore> IndexedFamilyQuery for LogMaterializer<'a, M, B> {
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

    async fn load_record_at(
        &self,
        block_number: u64,
        idx_in_block: usize,
        stats: &IndexedQueryStats,
    ) -> Result<LogEntry> {
        let started = Instant::now();
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        stats.record_materialize_block_record(started);

        let started = Instant::now();
        let header = self
            .tables
            .family(Family::Log)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log header"))?;
        stats.record_materialize_header(started);

        if idx_in_block + 1 >= header.offsets.len() {
            return Err(MonadChainDataError::Decode("log index out of range"));
        }

        let started = Instant::now();
        let frame = self
            .tables
            .read_block_blob_frame(Family::Log, block_number, &header, idx_in_block)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log blob"))?;
        stats.record_materialize_blob_frame(started);

        let started = Instant::now();
        let bytes = self
            .tables
            .decode_block_row(Family::Log, header.dict_version, &frame)
            .await?;
        stats.record_materialize_decode_row(started);
        let started = Instant::now();
        let raw = RawLogEntry::decode(&bytes)?;
        let entry = raw.into_log_entry(block_record.block_number, block_record.block_hash);
        stats.record_materialize_entry_decode(started);
        Ok(entry)
    }

    async fn load_records_in_block(
        &self,
        block_number: u64,
        indices: &[usize],
        stats: &IndexedQueryStats,
    ) -> Result<Vec<LogEntry>> {
        stats.record_materialize_block();
        let started = Instant::now();
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        stats.record_materialize_block_record(started);

        let started = Instant::now();
        let header = self
            .tables
            .family(Family::Log)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log header"))?;
        stats.record_materialize_header(started);

        let spans = coalesce_frame_ranges(indices.iter().copied(), |idx_in_block| {
            if idx_in_block + 1 >= header.offsets.len() {
                return Err(MonadChainDataError::Decode("log index out of range"));
            }
            Ok(header.abs_range(idx_in_block))
        })?;
        let selected_bytes: usize = spans
            .iter()
            .flat_map(|span| &span.frames)
            .map(|frame| frame.abs_end.saturating_sub(frame.abs_start))
            .sum();
        let decoder = self
            .tables
            .block_decoder(Family::Log, header.dict_version)
            .await?;
        let mut decompressor = RowDecompressor::new(decoder.as_ref())?;
        let mut records: Vec<Option<LogEntry>> = (0..indices.len()).map(|_| None).collect();

        let (region_start, region_end) = header.region_range();
        let region_len = region_end.saturating_sub(region_start);
        if spans.len() > MATERIALIZE_WHOLE_REGION_SPAN_THRESHOLD
            && region_len <= MATERIALIZE_WHOLE_REGION_MAX_BYTES
        {
            stats.record_materialize_span(region_len, selected_bytes);
            let started = Instant::now();
            let region = self
                .tables
                .read_block_blob_region(Family::Log, block_number, &header)
                .await?
                .ok_or(MonadChainDataError::MissingData("missing block log blob"))?;
            stats.record_materialize_blob_frame(started);

            for frame in spans.into_iter().flat_map(|span| span.frames) {
                let start = frame.abs_start.saturating_sub(region_start);
                let end = frame.abs_end.saturating_sub(region_start);
                if start > end || end > region.len() {
                    return Err(MonadChainDataError::Decode("invalid log range"));
                }
                let started = Instant::now();
                let bytes = decompressor.decompress(&region[start..end])?;
                stats.record_materialize_decode_row(started);
                let started = Instant::now();
                let raw = RawLogEntry::decode(&bytes)?;
                records[frame.output_pos] =
                    Some(raw.into_log_entry(block_record.block_number, block_record.block_hash));
                stats.record_materialize_entry_decode(started);
            }
        } else {
            for span in spans {
                let span_selected_bytes: usize = span
                    .frames
                    .iter()
                    .map(|frame| frame.abs_end.saturating_sub(frame.abs_start))
                    .sum();
                stats.record_materialize_span(
                    span.abs_end.saturating_sub(span.abs_start),
                    span_selected_bytes,
                );
                let started = Instant::now();
                let span_bytes = self
                    .tables
                    .read_block_blob_header_range(
                        block_number,
                        &header,
                        span.abs_start,
                        span.abs_end,
                    )
                    .await?
                    .ok_or(MonadChainDataError::MissingData("missing block log blob"))?;
                stats.record_materialize_blob_frame(started);

                for frame in span.frames {
                    let start = frame.abs_start.saturating_sub(span.abs_start);
                    let end = frame.abs_end.saturating_sub(span.abs_start);
                    if start > end || end > span_bytes.len() {
                        return Err(MonadChainDataError::Decode("invalid log range"));
                    }
                    let started = Instant::now();
                    let bytes = decompressor.decompress(&span_bytes[start..end])?;
                    stats.record_materialize_decode_row(started);
                    let started = Instant::now();
                    let raw = RawLogEntry::decode(&bytes)?;
                    records[frame.output_pos] = Some(
                        raw.into_log_entry(block_record.block_number, block_record.block_hash),
                    );
                    stats.record_materialize_entry_decode(started);
                }
            }
        }

        records
            .into_iter()
            .map(|record| record.ok_or(MonadChainDataError::Decode("missing materialized log")))
            .collect()
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
        if block_record.logs.count == 0 {
            return Ok((block_ref, Vec::new()));
        }

        let header = self
            .tables
            .family(Family::Log)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log header"))?;
        let blob = self
            .tables
            .read_block_blob_region(Family::Log, block_number, &header)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block log blob"))?;

        let decoder = self
            .tables
            .block_decoder(Family::Log, header.dict_version)
            .await?;
        let logs = load_filtered_block_logs(
            &header,
            &blob,
            &block_record,
            order,
            filter,
            decoder.as_ref(),
        )?;

        Ok((block_ref, logs))
    }
}

#[allow(clippy::too_many_arguments)]
fn load_filtered_block_logs(
    header: &BlockBlobHeader,
    blob: &RawBytes,
    block_record: &BlockRecord,
    order: QueryOrder,
    filter: &LogFilter,
    decoder: Option<&Arc<DecoderDictionary<'static>>>,
) -> Result<Vec<LogEntry>> {
    let count = header.row_count();
    let indices: Box<dyn Iterator<Item = usize>> = match order {
        QueryOrder::Ascending => Box::new(0..count),
        QueryOrder::Descending => Box::new((0..count).rev()),
    };

    let mut decompressor = RowDecompressor::new(decoder)?;
    let mut logs = Vec::new();
    for log_idx in indices {
        let log = decode_log_at(
            header,
            blob.as_ref(),
            log_idx,
            block_record.block_number,
            block_record.block_hash,
            &mut decompressor,
        )?;
        if filter.matches(&log) {
            logs.push(log);
        }
    }

    Ok(logs)
}

pub(crate) fn decode_log_at(
    header: &BlockBlobHeader,
    blob: &[u8],
    log_idx: usize,
    block_number: u64,
    block_hash: Hash32,
    decompressor: &mut RowDecompressor<'_>,
) -> Result<LogEntry> {
    if log_idx + 1 >= header.offsets.len() {
        return Err(MonadChainDataError::Decode("log index out of range"));
    }

    let start = header.offsets[log_idx] as usize;
    let end = header.offsets[log_idx + 1] as usize;
    if start > end || end > blob.len() {
        return Err(MonadChainDataError::Decode("invalid log range"));
    }

    let bytes = decompressor.decompress(&blob[start..end])?;
    let raw = RawLogEntry::decode(&bytes)?;
    Ok(raw.into_log_entry(block_number, block_hash))
}

pub(crate) async fn execute_indexed_log_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryLogsRequest,
    block_window: ResolvedBlockWindow,
    published_head: u64,
) -> Result<QueryLogsResponse> {
    let materializer = LogMaterializer::new(tables);
    let outcome = execute_indexed_family_query(
        tables,
        &materializer,
        &request.filter,
        block_window,
        published_head,
        request.envelope.order,
        request.envelope.limit,
    )
    .await?;
    Ok(QueryLogsResponse {
        logs: outcome.records,
        blocks: None,
        transactions: None,
        span: outcome.span,
    })
}

/// Walks blocks in query order, applying `LogFilter` to each block's logs.
pub(crate) async fn execute_block_scan_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryLogsRequest,
    window: ResolvedBlockWindow,
) -> Result<QueryLogsResponse> {
    let materializer = LogMaterializer::new(tables);
    let outcome = execute_block_scan_family_query(
        &materializer,
        &request.filter,
        window,
        request.envelope.order,
        request.envelope.limit,
    )
    .await?;
    Ok(QueryLogsResponse {
        logs: outcome.records,
        blocks: None,
        transactions: None,
        span: outcome.span,
    })
}
