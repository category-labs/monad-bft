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

use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
    time::Instant,
};

use alloy_consensus::Transaction;
use alloy_primitives::Address;
use bytes::Bytes as RawBytes;
use futures::{stream, StreamExt, TryStreamExt};
use zstd::dict::DecoderDictionary;

use super::{
    types::{selector_from_envelope, StoredTxEnvelope, TxEntry},
    BlockBlobHeader,
};
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
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TxFilter {
    pub from: Option<HashSet<Address>>,
    pub to: Option<HashSet<Address>>,
    pub selector: Option<HashSet<[u8; 4]>>,
}

impl TxFilter {
    pub fn has_indexed_clause(&self) -> bool {
        self.from.is_some() || self.to.is_some() || self.selector.is_some()
    }
}

/// Opt-in relations joined onto a transactions query response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TxsRelations {
    /// When true, `QueryTransactionsResponse::blocks` is populated with
    /// deduped headers for the blocks that contributed txs in this page.
    pub blocks: bool,
}

/// Public transactions query in queryX spec semantics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryTransactionsRequest {
    pub envelope: QueryEnvelope,
    pub filter: TxFilter,
    pub relations: TxsRelations,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTransactionsResponse {
    pub txs: Vec<TxEntry>,
    /// Deduped blocks for the `txs` in this page, sorted ascending by
    /// block number. `None` unless `QueryTransactionsRequest::relations.blocks`.
    pub blocks: Option<Vec<Block>>,
    /// The span mirrors the resolved request bounds and records the last
    /// block scanned. When `txs` is empty there is no next page; callers
    /// should treat the response as terminal rather than advance the cursor.
    pub span: BlockSpan,
}

impl IndexedFilter for TxFilter {
    type Record = TxEntry;

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

        clauses
    }

    fn matches(&self, tx: &TxEntry) -> bool {
        if let Some(from) = &self.from {
            if !from.contains(&tx.sender) {
                return false;
            }
        }

        // Contract-creation txs and bad envelopes do not match a `to` or
        // `selector` filter; decode once and reuse for both.
        if self.to.is_some() || self.selector.is_some() {
            let Ok(envelope) = tx.envelope() else {
                return false;
            };

            if let Some(to) = &self.to {
                match envelope.to() {
                    Some(actual) if to.contains(&actual) => {}
                    _ => return false,
                }
            }
            if let Some(selector) = &self.selector {
                match selector_from_envelope(&envelope) {
                    Some(actual) if selector.contains(&actual) => {}
                    _ => return false,
                }
            }
        }

        true
    }
}

pub struct TxMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
}

impl<'a, M: MetaStore, B: BlobStore> TxMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self { tables }
    }
}

impl<'a, M: MetaStore, B: BlobStore> IndexedFamilyQuery for TxMaterializer<'a, M, B> {
    type Filter = TxFilter;
    type Record = TxEntry;

    fn family() -> Family {
        Family::Tx
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
    ) -> Result<TxEntry> {
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
            .family(Family::Tx)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block tx header"))?;
        stats.record_materialize_header(started);

        if idx_in_block + 1 >= header.offsets.len() {
            return Err(MonadChainDataError::Decode("tx index out of range"));
        }

        let started = Instant::now();
        let frame = self
            .tables
            .read_block_blob_frame(Family::Tx, block_number, &header, idx_in_block)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block tx blob"))?;
        stats.record_materialize_blob_frame(started);

        let started = Instant::now();
        let bytes = self
            .tables
            .decode_block_row(Family::Tx, header.dict_version, &frame)
            .await?;
        stats.record_materialize_decode_row(started);
        let started = Instant::now();
        let stored = StoredTxEnvelope::decode(&bytes)?;
        let tx_idx_u32 = u32::try_from(idx_in_block)
            .map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;
        let entry = stored.into_tx_entry(
            block_record.block_number,
            block_record.block_hash,
            tx_idx_u32,
        );
        stats.record_materialize_entry_decode(started);
        Ok(entry)
    }

    async fn load_records_in_block(
        &self,
        block_number: u64,
        indices: &[usize],
        stats: &IndexedQueryStats,
    ) -> Result<Vec<TxEntry>> {
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
            .family(Family::Tx)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block tx header"))?;
        stats.record_materialize_header(started);

        let spans = coalesce_frame_ranges(indices.iter().copied(), |idx_in_block| {
            if idx_in_block + 1 >= header.offsets.len() {
                return Err(MonadChainDataError::Decode("tx index out of range"));
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
            .block_decoder(Family::Tx, header.dict_version)
            .await?;
        let mut decompressor = RowDecompressor::new(decoder.as_ref())?;
        let mut records: Vec<Option<TxEntry>> = (0..indices.len()).map(|_| None).collect();

        let (region_start, region_end) = header.region_range();
        let region_len = region_end.saturating_sub(region_start);
        if spans.len() > MATERIALIZE_WHOLE_REGION_SPAN_THRESHOLD
            && region_len <= MATERIALIZE_WHOLE_REGION_MAX_BYTES
        {
            stats.record_materialize_span(region_len, selected_bytes);
            let started = Instant::now();
            let region = self
                .tables
                .read_block_blob_region(Family::Tx, block_number, &header)
                .await?
                .ok_or(MonadChainDataError::MissingData("missing block tx blob"))?;
            stats.record_materialize_blob_frame(started);

            for frame in spans.into_iter().flat_map(|span| span.frames) {
                let start = frame.abs_start.saturating_sub(region_start);
                let end = frame.abs_end.saturating_sub(region_start);
                if start > end || end > region.len() {
                    return Err(MonadChainDataError::Decode("invalid tx range"));
                }
                let started = Instant::now();
                let bytes = decompressor.decompress(&region[start..end])?;
                stats.record_materialize_decode_row(started);
                let started = Instant::now();
                let stored = StoredTxEnvelope::decode(&bytes)?;
                let tx_idx_u32 = u32::try_from(frame.idx_in_block)
                    .map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;
                records[frame.output_pos] = Some(stored.into_tx_entry(
                    block_record.block_number,
                    block_record.block_hash,
                    tx_idx_u32,
                ));
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
                    .ok_or(MonadChainDataError::MissingData("missing block tx blob"))?;
                stats.record_materialize_blob_frame(started);

                for frame in span.frames {
                    let start = frame.abs_start.saturating_sub(span.abs_start);
                    let end = frame.abs_end.saturating_sub(span.abs_start);
                    if start > end || end > span_bytes.len() {
                        return Err(MonadChainDataError::Decode("invalid tx range"));
                    }
                    let started = Instant::now();
                    let bytes = decompressor.decompress(&span_bytes[start..end])?;
                    stats.record_materialize_decode_row(started);
                    let started = Instant::now();
                    let stored = StoredTxEnvelope::decode(&bytes)?;
                    let tx_idx_u32 = u32::try_from(frame.idx_in_block)
                        .map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;
                    records[frame.output_pos] = Some(stored.into_tx_entry(
                        block_record.block_number,
                        block_record.block_hash,
                        tx_idx_u32,
                    ));
                    stats.record_materialize_entry_decode(started);
                }
            }
        }

        records
            .into_iter()
            .map(|record| record.ok_or(MonadChainDataError::Decode("missing materialized tx")))
            .collect()
    }

    async fn load_filtered_block_records(
        &self,
        block_number: u64,
        order: QueryOrder,
        filter: &TxFilter,
    ) -> Result<(BlockRef, Vec<TxEntry>)> {
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        let block_ref = BlockRef::from(&block_record);
        if block_record.txs.count == 0 {
            return Ok((block_ref, Vec::new()));
        }

        let header = self
            .tables
            .family(Family::Tx)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block tx header"))?;
        let blob = self
            .tables
            .read_block_blob_region(Family::Tx, block_number, &header)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block tx blob"))?;

        let decoder = self
            .tables
            .block_decoder(Family::Tx, header.dict_version)
            .await?;
        let txs = load_filtered_block_txs(
            &header,
            &blob,
            &block_record,
            order,
            filter,
            decoder.as_ref(),
        )?;

        Ok((block_ref, txs))
    }
}

#[allow(clippy::too_many_arguments)]
fn load_filtered_block_txs(
    header: &BlockBlobHeader,
    blob: &RawBytes,
    block_record: &BlockRecord,
    order: QueryOrder,
    filter: &TxFilter,
    decoder: Option<&Arc<DecoderDictionary<'static>>>,
) -> Result<Vec<TxEntry>> {
    let count = header.row_count();
    let indices: Box<dyn Iterator<Item = usize>> = match order {
        QueryOrder::Ascending => Box::new(0..count),
        QueryOrder::Descending => Box::new((0..count).rev()),
    };

    let mut decompressor = RowDecompressor::new(decoder)?;
    let mut txs = Vec::new();
    for tx_idx in indices {
        let tx = decode_tx_at(
            header,
            blob.as_ref(),
            tx_idx,
            block_record.block_number,
            block_record.block_hash,
            &mut decompressor,
        )?;
        if filter.matches(&tx) {
            txs.push(tx);
        }
    }

    Ok(txs)
}

/// Related txs point-loaded concurrently when projecting the `transactions`
/// relation. Each is an independent blob frame read, so a modest fan-out hides
/// per-read latency without flooding the backend.
const RELATION_TX_CONCURRENCY: usize = 8;

/// Loads txs for the given `(block_number, tx_index)` pairs in ascending
/// order, deduped. Used to fulfill the `transactions` relation on logs
/// queries.
pub(crate) async fn load_txs_by_positions<M: MetaStore, B: BlobStore, I>(
    tables: &Tables<M, B>,
    positions: I,
) -> Result<Vec<TxEntry>>
where
    I: IntoIterator<Item = (u64, u32)>,
{
    let distinct: BTreeSet<(u64, u32)> = positions.into_iter().collect();
    let materializer = TxMaterializer::new(tables);
    // Point-load each tx concurrently (bounded), preserving the deduped
    // ascending order. The serial loop here dominated `logs + relations`
    // queries (one blob frame read per related tx, awaited one at a time).
    stream::iter(distinct)
        .map(|(block_number, tx_idx)| {
            let materializer = &materializer;
            async move {
                materializer
                    .load_record_at(block_number, tx_idx as usize, &IndexedQueryStats::default())
                    .await
            }
        })
        .buffered(RELATION_TX_CONCURRENCY)
        .try_collect()
        .await
}

pub(crate) fn decode_tx_at(
    header: &BlockBlobHeader,
    blob: &[u8],
    tx_idx: usize,
    block_number: u64,
    block_hash: Hash32,
    decompressor: &mut RowDecompressor<'_>,
) -> Result<TxEntry> {
    if tx_idx + 1 >= header.offsets.len() {
        return Err(MonadChainDataError::Decode("tx index out of range"));
    }

    let start = header.offsets[tx_idx] as usize;
    let end = header.offsets[tx_idx + 1] as usize;
    if start > end || end > blob.len() {
        return Err(MonadChainDataError::Decode("invalid tx range"));
    }

    let bytes = decompressor.decompress(&blob[start..end])?;
    let stored = StoredTxEnvelope::decode(&bytes)?;
    let tx_idx_u32 =
        u32::try_from(tx_idx).map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;
    Ok(stored.into_tx_entry(block_number, block_hash, tx_idx_u32))
}

pub(crate) async fn execute_indexed_tx_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryTransactionsRequest,
    block_window: ResolvedBlockWindow,
    published_head: u64,
) -> Result<QueryTransactionsResponse> {
    let materializer = TxMaterializer::new(tables);
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
    Ok(QueryTransactionsResponse {
        txs: outcome.records,
        blocks: None,
        span: outcome.span,
    })
}

/// Walks blocks in query order, applying `TxFilter` to each block's txs.
pub(crate) async fn execute_block_scan_tx_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryTransactionsRequest,
    window: ResolvedBlockWindow,
) -> Result<QueryTransactionsResponse> {
    let materializer = TxMaterializer::new(tables);
    let outcome = execute_block_scan_family_query(
        &materializer,
        &request.filter,
        window,
        request.envelope.order,
        request.envelope.limit,
    )
    .await?;
    Ok(QueryTransactionsResponse {
        txs: outcome.records,
        blocks: None,
        span: outcome.span,
    })
}
