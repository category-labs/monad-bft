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

use alloy_primitives::{Address, Bytes, U256};
use bytes::Bytes as RawBytes;

use super::types::TransferEntry;
use crate::{
    blocks::Block,
    engine::{
        clause::{IndexedClause, IndexedFilter},
        family::Family,
        query::family_runner::{
            execute_block_scan_family_query, execute_indexed_family_query, IndexedFamilyQuery,
        },
        tables::Tables,
    },
    error::{MonadChainDataError, Result},
    primitives::{
        limits::QueryEnvelope,
        page::QueryOrder,
        range::ResolvedBlockWindow,
        refs::{BlockRef, BlockSpan},
    },
    store::{BlobStore, MetaStore},
    traces::{BlockTraceHeader, StoredTrace, TraceEntry},
    txs::TxEntry,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransferFilter {
    pub from: Option<HashSet<Address>>,
    pub to: Option<HashSet<Address>>,
    pub is_top_level: Option<bool>,
}

impl TransferFilter {
    /// Transfers always carry a positive `has_transfer` clause, so the
    /// indexed path is viable whenever the trace family has any rows in
    /// the window. The same `is_top_level: Some(false)` caveat as
    /// `TraceFilter` does not apply here, because the indexed runner can
    /// drop non-top-level frames as a post-filter against the
    /// `has_transfer` candidate set. Returning `true` unconditionally
    /// here is the right call: scanning every block to find transfers
    /// would defeat the indexed column.
    pub fn has_indexed_clause(&self) -> bool {
        true
    }
}

/// Opt-in relations joined onto a transfers query response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransfersRelations {
    pub blocks: bool,
    pub transactions: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryTransfersRequest {
    pub envelope: QueryEnvelope,
    pub filter: TransferFilter,
    pub relations: TransfersRelations,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTransfersResponse {
    pub transfers: Vec<TransferEntry>,
    pub blocks: Option<Vec<Block>>,
    pub transactions: Option<Vec<TxEntry>>,
    pub span: BlockSpan,
}

impl IndexedFilter for TransferFilter {
    type Record = TransferEntry;

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
        if matches!(self.is_top_level, Some(true)) {
            clauses.push(IndexedClause {
                kind: "top_level",
                values: vec![RawBytes::new()],
            });
        }
        // Always AND in the binary `has_transfer` column — the whole point
        // of the view.
        clauses.push(IndexedClause {
            kind: "has_transfer",
            values: vec![RawBytes::new()],
        });
        clauses
    }

    fn matches(&self, transfer: &TransferEntry) -> bool {
        // Defensive: `value > 0` and `to.is_some()` are guaranteed by the
        // `has_transfer` clause, but re-check them here so a future
        // ingest bug surfaces as a missing row instead of bad output.
        if transfer.value == U256::ZERO {
            return false;
        }
        if let Some(addresses) = &self.from {
            if !addresses.contains(&transfer.from) {
                return false;
            }
        }
        if let Some(addresses) = &self.to {
            if !addresses.contains(&transfer.to) {
                return false;
            }
        }
        match self.is_top_level {
            Some(true) if !transfer.is_top_level() => return false,
            Some(false) if transfer.is_top_level() => return false,
            _ => {}
        }
        true
    }
}

/// Projects a `TraceEntry` from a frame whose `has_transfer` bit was set
/// at ingest into a `TransferEntry`. `to` is unwrapped — for every
/// qualifying call kind the tracer guarantees a resolved `to`
/// (Create/Create2 set the new contract address on success; SelfDestruct
/// sets the beneficiary; Call/CallCode by definition have a `to`).
fn trace_into_transfer(trace: TraceEntry) -> Result<TransferEntry> {
    let TraceEntry {
        block_number,
        block_hash,
        tx_index,
        trace_address,
        typ,
        from,
        to,
        value,
        ..
    } = trace;
    let to = to.ok_or(MonadChainDataError::Decode(
        "transfer frame missing `to` address; tracer invariant violated",
    ))?;
    Ok(TransferEntry {
        block_number,
        block_hash,
        tx_index,
        trace_address,
        typ,
        from,
        to,
        value,
    })
}

pub struct TransferMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
}

impl<'a, M: MetaStore, B: BlobStore> TransferMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self { tables }
    }
}

impl<'a, M: MetaStore, B: BlobStore> IndexedFamilyQuery<M, B> for TransferMaterializer<'a, M, B> {
    type Filter = TransferFilter;
    type Record = TransferEntry;

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
    ) -> Result<TransferEntry> {
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        let header_bytes = self
            .tables
            .family(Family::Trace)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData(
                "missing block trace header",
            ))?;
        let header = BlockTraceHeader::decode(&header_bytes)?;

        if idx_in_block + 1 >= header.offsets.len() {
            return Err(MonadChainDataError::Decode("trace index out of range"));
        }
        let start = header.offsets[idx_in_block] as usize;
        let end = header.offsets[idx_in_block + 1] as usize;

        let bytes = self
            .tables
            .family(Family::Trace)
            .read_block_blob_range(block_number, start, end)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block trace blob"))?;

        let stored = StoredTrace::decode(&bytes)?;
        let trace = stored.into_trace_entry(block_record.block_number, block_record.block_hash);
        trace_into_transfer(trace)
    }

    async fn load_filtered_block_records(
        &self,
        block_number: u64,
        order: QueryOrder,
        filter: &TransferFilter,
    ) -> Result<(BlockRef, Vec<TransferEntry>)> {
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        let block_ref = BlockRef::from(&block_record);

        let header_bytes = self
            .tables
            .family(Family::Trace)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData(
                "missing block trace header",
            ))?;
        let header = BlockTraceHeader::decode(&header_bytes)?;
        let blob: Bytes = self
            .tables
            .family(Family::Trace)
            .load_block_blob(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block trace blob"))?;

        let count = header.trace_count();
        let indices: Box<dyn Iterator<Item = usize>> = match order {
            QueryOrder::Ascending => Box::new(0..count),
            QueryOrder::Descending => Box::new((0..count).rev()),
        };

        let mut transfers = Vec::new();
        for idx in indices {
            if idx + 1 >= header.offsets.len() {
                return Err(MonadChainDataError::Decode("trace index out of range"));
            }
            let start = header.offsets[idx] as usize;
            let end = header.offsets[idx + 1] as usize;
            if start > end || end > blob.len() {
                return Err(MonadChainDataError::Decode("invalid trace range"));
            }
            let stored = StoredTrace::decode(&blob[start..end])?;
            // The block-scan path can run when the indexed clause is not
            // viable; that path materializes every trace and filters
            // here. Re-check the transfer predicate so non-transfer
            // frames don't appear in the response.
            if !is_transfer_trace(&stored) {
                continue;
            }
            let trace = stored.into_trace_entry(block_record.block_number, block_record.block_hash);
            let transfer = match trace_into_transfer(trace) {
                Ok(t) => t,
                Err(_) => continue,
            };
            if filter.matches(&transfer) {
                transfers.push(transfer);
            }
        }

        Ok((block_ref, transfers))
    }
}

/// Mirrors `traces::ingest::is_transfer_frame` over a `StoredTrace`.
/// Used by the block-scan path; the indexed path filters via the
/// `has_transfer` bitmap clause.
fn is_transfer_trace(t: &StoredTrace) -> bool {
    use crate::family::CallKind::*;
    let kind_moves_value = matches!(t.typ, Call | CallCode | Create | Create2 | SelfDestruct);
    t.value > U256::ZERO && kind_moves_value && t.status == 0 && t.tx_status
}

pub(crate) async fn execute_indexed_transfer_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryTransfersRequest,
    block_window: ResolvedBlockWindow,
    published_head: u64,
) -> Result<QueryTransfersResponse> {
    let materializer = TransferMaterializer::new(tables);
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
    Ok(QueryTransfersResponse {
        transfers: outcome.records,
        blocks: None,
        transactions: None,
        span: outcome.span,
    })
}

pub(crate) async fn execute_block_scan_transfer_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryTransfersRequest,
    window: ResolvedBlockWindow,
) -> Result<QueryTransfersResponse> {
    let materializer = TransferMaterializer::new(tables);
    let outcome = execute_block_scan_family_query(
        &materializer,
        &request.filter,
        window,
        request.envelope.order,
        request.envelope.limit,
    )
    .await?;
    Ok(QueryTransfersResponse {
        transfers: outcome.records,
        blocks: None,
        transactions: None,
        span: outcome.span,
    })
}
