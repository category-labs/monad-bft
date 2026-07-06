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

use alloy_primitives::{Address, U256};
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

use super::TransferEntry;
use crate::{
    blocks::Block,
    traces::{StoredTrace, TraceEntry},
    txs::TxEntry,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransferFilter {
    pub from: Option<HashSet<Address>>,
    pub to: Option<HashSet<Address>>,
    pub is_top_level: Option<bool>,
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
        if let Some(v) = &self.from {
            clauses.push(IndexedClause::from_set(IndexKind::From, v));
        }
        if let Some(v) = &self.to {
            clauses.push(IndexedClause::from_set(IndexKind::To, v));
        }
        if matches!(self.is_top_level, Some(true)) {
            clauses.push(IndexedClause::marker(IndexKind::TopLevel));
        }
        // Always AND in `has_transfer` — the defining clause of the view.
        clauses.push(IndexedClause::marker(IndexKind::HasTransfer));
        clauses
    }

    fn matches(&self, transfer: &TransferEntry) -> bool {
        // `value > 0` is guaranteed by `has_transfer`; re-check so an
        // ingest bug surfaces as a missing row instead of bad output.
        if transfer.value == U256::ZERO {
            return false;
        }
        if !set_allows(&self.from, Some(&transfer.from)) {
            return false;
        }
        if !set_allows(&self.to, Some(&transfer.to)) {
            return false;
        }
        if self
            .is_top_level
            .is_some_and(|want| want != transfer.is_top_level())
        {
            return false;
        }
        true
    }
}

/// Projects a `has_transfer` frame into a `TransferEntry`. `to` is
/// unwrapped: for every qualifying call kind the tracer guarantees a
/// resolved `to` (Create* -> new contract, SelfDestruct -> beneficiary).
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
    let to = to.ok_or(QueryError::Decode(
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

/// Indexed-path-only materializer; see its `decode_scan_record` override for
/// why transfers must never reach the block-scan path.
pub struct TransferMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
}

impl<'a, M: MetaStore, B: BlobStore> TransferMaterializer<'a, M, B> {
    pub fn new(tables: &'a Tables<M, B>) -> Self {
        Self { tables }
    }
}

impl<'a, M: MetaStore, B: BlobStore> IndexedFamilyQuery for TransferMaterializer<'a, M, B> {
    type Meta = M;
    type Blob = B;
    type Filter = TransferFilter;
    type Record = TransferEntry;
    type StoredRow = StoredTrace;

    fn family() -> Family {
        Family::Trace
    }

    fn tables(&self) -> &Tables<M, B> {
        self.tables
    }

    fn row_cache(&self) -> &RowCache<StoredTrace> {
        &self.tables.row_caches().traces
    }

    fn decode_stored(bytes: &[u8]) -> Result<StoredTrace> {
        StoredTrace::decode(bytes)
    }

    fn decode_external_container(
        container_idx: usize,
        _row_base: usize,
        tx_status: bool,
        bytes: &[u8],
    ) -> Result<Vec<StoredTrace>> {
        // Shares the trace family's container format (and row cache).
        let tx_index =
            u32::try_from(container_idx).map_err(|_| QueryError::Decode("tx index overflow"))?;
        crate::external::decode_external_trace_container(bytes, tx_index, tx_status)
    }

    fn into_record_owned(
        stored: StoredTrace,
        block_record: &BlockRecord,
        _idx_in_block: usize,
    ) -> Result<TransferEntry> {
        let trace = stored.into_trace_entry(block_record.block_number, block_record.block_hash);
        trace_into_transfer(trace)
    }

    /// Transfers are unconditionally indexed (`TransferFilter::indexed_clauses`
    /// always ANDs in the `has_transfer` clause) and must never reach the
    /// block-scan path: `TransferFilter::matches` omits the `status` /
    /// `tx_status` re-check the bitmap pre-filters, so a scan would emit
    /// reverted-but-value-carrying frames. Hard-fail rather than silently
    /// mis-route. (Both the native and external scan paths funnel through
    /// this hook.)
    fn scan_record_from_stored(
        _stored: StoredTrace,
        _block_record: &BlockRecord,
        _idx_in_block: usize,
    ) -> Result<Option<TransferEntry>> {
        Err(QueryError::InvalidRequest(
            "transfers cannot be served by the block-scan path",
        ))
    }
}
