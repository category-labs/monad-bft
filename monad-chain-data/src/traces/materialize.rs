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

use alloy_primitives::Address;
use bytes::Bytes as RawBytes;
use zstd::dict::DecoderDictionary;

use super::{
    types::{StoredTrace, TraceEntry},
    BlockBlobHeader,
};
use crate::{
    blocks::Block,
    engine::{
        clause::{IndexedClause, IndexedFilter},
        family::Family,
        query::family_runner::{
            execute_block_scan_family_query, execute_indexed_family_query, IndexedFamilyQuery,
            IndexedQueryStats,
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
pub struct TraceFilter {
    pub from: Option<HashSet<Address>>,
    pub to: Option<HashSet<Address>>,
    pub selector: Option<HashSet<[u8; 4]>>,
    /// `Some(true)` keeps only top-level frames; `Some(false)` keeps only
    /// non-top-level frames; `None` is "no top-level constraint".
    pub is_top_level: Option<bool>,
}

impl TraceFilter {
    /// Whether the indexed path can serve this query at least partially.
    /// Returns `false` when no clause-producing fields are set, or when
    /// the only constraint is `is_top_level: Some(false)` (which cannot
    /// be expressed as a positive bitmap clause). Both cases force the
    /// block-scan path, which applies `matches` to drop the unwanted
    /// frames.
    pub fn has_indexed_clause(&self) -> bool {
        if self.from.is_some() || self.to.is_some() || self.selector.is_some() {
            return true;
        }
        // is_top_level: Some(true) emits a positive `top_level` clause;
        // Some(false) does not and must fall back to scan.
        matches!(self.is_top_level, Some(true))
    }
}

/// Opt-in relations joined onto a traces query response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TracesRelations {
    /// When true, `QueryTracesResponse::blocks` is populated with deduped
    /// headers for the blocks that contributed traces in this page.
    pub blocks: bool,
    /// When true, `QueryTracesResponse::transactions` is populated with
    /// deduped txs for the `(block_number, tx_index)` pairs carried on
    /// the traces in this page.
    pub transactions: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryTracesRequest {
    pub envelope: QueryEnvelope,
    pub filter: TraceFilter,
    pub relations: TracesRelations,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTracesResponse {
    pub traces: Vec<TraceEntry>,
    pub blocks: Option<Vec<Block>>,
    pub transactions: Option<Vec<TxEntry>>,
    pub span: BlockSpan,
}

impl IndexedFilter for TraceFilter {
    type Record = TraceEntry;

    fn indexed_clauses(&self) -> Vec<IndexedClause> {
        let mut clauses = Vec::new();

        if let Some(values) = &self.from {
            clauses.push(IndexedClause {
                kind: "from",
                values: values
                    .iter()
                    .map(|a| RawBytes::copy_from_slice(a.as_slice()))
                    .collect(),
            });
        }
        if let Some(values) = &self.to {
            clauses.push(IndexedClause {
                kind: "to",
                values: values
                    .iter()
                    .map(|a| RawBytes::copy_from_slice(a.as_slice()))
                    .collect(),
            });
        }
        if let Some(values) = &self.selector {
            clauses.push(IndexedClause {
                kind: "selector",
                values: values
                    .iter()
                    .map(|s| RawBytes::copy_from_slice(s.as_slice()))
                    .collect(),
            });
        }
        if matches!(self.is_top_level, Some(true)) {
            clauses.push(IndexedClause {
                kind: "top_level",
                values: vec![RawBytes::new()],
            });
        }

        clauses
    }

    fn matches(&self, trace: &TraceEntry) -> bool {
        if let Some(addresses) = &self.from {
            if !addresses.contains(&trace.from) {
                return false;
            }
        }
        if let Some(addresses) = &self.to {
            match trace.to {
                Some(actual) if addresses.contains(&actual) => {}
                _ => return false,
            }
        }
        if let Some(selectors) = &self.selector {
            match trace.selector() {
                Some(actual) if selectors.contains(&actual) => {}
                _ => return false,
            }
        }
        match self.is_top_level {
            Some(true) if !trace.is_top_level() => return false,
            Some(false) if trace.is_top_level() => return false,
            _ => {}
        }

        true
    }
}

pub struct TraceMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
}

impl<'a, M: MetaStore, B: BlobStore> TraceMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self { tables }
    }
}

impl<'a, M: MetaStore, B: BlobStore> IndexedFamilyQuery for TraceMaterializer<'a, M, B> {
    type Filter = TraceFilter;
    type Record = TraceEntry;

    fn family() -> Family {
        Family::Trace
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
    ) -> Result<TraceEntry> {
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
            .family(Family::Trace)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData(
                "missing block trace header",
            ))?;
        stats.record_materialize_header(started);

        if idx_in_block + 1 >= header.offsets.len() {
            return Err(MonadChainDataError::Decode("trace index out of range"));
        }

        let started = Instant::now();
        let frame = self
            .tables
            .read_block_blob_frame(Family::Trace, block_number, &header, idx_in_block)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block trace blob"))?;
        stats.record_materialize_blob_frame(started);

        let started = Instant::now();
        let bytes = self
            .tables
            .decode_block_row(Family::Trace, header.dict_version, &frame)
            .await?;
        stats.record_materialize_decode_row(started);
        let started = Instant::now();
        let stored = StoredTrace::decode(&bytes)?;
        let entry = stored.into_trace_entry(block_record.block_number, block_record.block_hash);
        stats.record_materialize_entry_decode(started);
        Ok(entry)
    }

    async fn load_filtered_block_records(
        &self,
        block_number: u64,
        order: QueryOrder,
        filter: &TraceFilter,
    ) -> Result<(BlockRef, Vec<TraceEntry>)> {
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        let block_ref = BlockRef::from(&block_record);
        if block_record.traces.count == 0 {
            return Ok((block_ref, Vec::new()));
        }

        let header = self
            .tables
            .family(Family::Trace)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData(
                "missing block trace header",
            ))?;
        let blob = self
            .tables
            .read_block_blob_region(Family::Trace, block_number, &header)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block trace blob"))?;

        let decoder = self
            .tables
            .block_decoder(Family::Trace, header.dict_version)
            .await?;
        let traces = load_filtered_block_traces(
            &header,
            &blob,
            &block_record,
            order,
            filter,
            decoder.as_ref(),
        )?;

        Ok((block_ref, traces))
    }
}

#[allow(clippy::too_many_arguments)]
fn load_filtered_block_traces(
    header: &BlockBlobHeader,
    blob: &RawBytes,
    block_record: &BlockRecord,
    order: QueryOrder,
    filter: &TraceFilter,
    decoder: Option<&Arc<DecoderDictionary<'static>>>,
) -> Result<Vec<TraceEntry>> {
    let count = header.row_count();
    let indices: Box<dyn Iterator<Item = usize>> = match order {
        QueryOrder::Ascending => Box::new(0..count),
        QueryOrder::Descending => Box::new((0..count).rev()),
    };

    let mut decompressor = RowDecompressor::new(decoder)?;
    let mut traces = Vec::new();
    for idx in indices {
        let trace = decode_trace_at(
            header,
            blob.as_ref(),
            idx,
            block_record.block_number,
            block_record.block_hash,
            &mut decompressor,
        )?;
        if filter.matches(&trace) {
            traces.push(trace);
        }
    }

    Ok(traces)
}

pub(crate) fn decode_trace_at(
    header: &BlockBlobHeader,
    blob: &[u8],
    idx: usize,
    block_number: u64,
    block_hash: Hash32,
    decompressor: &mut RowDecompressor<'_>,
) -> Result<TraceEntry> {
    if idx + 1 >= header.offsets.len() {
        return Err(MonadChainDataError::Decode("trace index out of range"));
    }

    let start = header.offsets[idx] as usize;
    let end = header.offsets[idx + 1] as usize;
    if start > end || end > blob.len() {
        return Err(MonadChainDataError::Decode("invalid trace range"));
    }

    let bytes = decompressor.decompress(&blob[start..end])?;
    let stored = StoredTrace::decode(&bytes)?;
    Ok(stored.into_trace_entry(block_number, block_hash))
}

pub(crate) async fn execute_indexed_trace_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryTracesRequest,
    block_window: ResolvedBlockWindow,
    published_head: u64,
) -> Result<QueryTracesResponse> {
    let materializer = TraceMaterializer::new(tables);
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
    Ok(QueryTracesResponse {
        traces: outcome.records,
        blocks: None,
        transactions: None,
        span: outcome.span,
    })
}

/// Walks blocks in query order, applying `TraceFilter` to each block's
/// traces. Used when the filter has no indexed clause (e.g.
/// `is_top_level: Some(false)` alone, or no filter at all).
pub(crate) async fn execute_block_scan_trace_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryTracesRequest,
    window: ResolvedBlockWindow,
) -> Result<QueryTracesResponse> {
    let materializer = TraceMaterializer::new(tables);
    let outcome = execute_block_scan_family_query(
        &materializer,
        &request.filter,
        window,
        request.envelope.order,
        request.envelope.limit,
    )
    .await?;
    Ok(QueryTracesResponse {
        traces: outcome.records,
        blocks: None,
        transactions: None,
        span: outcome.span,
    })
}
