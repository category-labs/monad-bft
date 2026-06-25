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

use alloy_primitives::Address;

use super::{StoredTrace, TraceEntry};
use crate::{
    blocks::Block,
    engine::{
        bitmap::IndexKind,
        clause::{set_allows, IndexedClause, IndexedFilter},
        family::Family,
        query::{family_runner::IndexedFamilyQuery, row_cache::RowCache},
        tables::Tables,
    },
    error::{MonadChainDataError, Result},
    primitives::{limits::QueryEnvelope, records::BlockRecord, refs::BlockSpan},
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

/// Opt-in relations joined onto a traces query response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TracesRelations {
    /// Populate `QueryTracesResponse::blocks` for the blocks that
    /// contributed traces in this page.
    pub blocks: bool,
    /// Populate `QueryTracesResponse::transactions` for the
    /// `(block_number, tx_index)` pairs carried on this page's traces.
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

    /// `is_top_level: Some(false)` emits no clause — "not top-level" cannot
    /// be expressed as a positive bitmap clause — so a filter constrained
    /// only by it routes to the block-scan path and is enforced by
    /// `matches` alone.
    fn indexed_clauses(&self) -> Vec<IndexedClause> {
        let mut clauses = Vec::new();

        if let Some(v) = &self.from {
            clauses.push(IndexedClause::from_set(IndexKind::From, v));
        }
        if let Some(v) = &self.to {
            clauses.push(IndexedClause::from_set(IndexKind::To, v));
        }
        if let Some(v) = &self.selector {
            clauses.push(IndexedClause::from_set(IndexKind::Selector, v));
        }
        if matches!(self.is_top_level, Some(true)) {
            clauses.push(IndexedClause::marker(IndexKind::TopLevel));
        }

        clauses
    }

    fn matches(&self, trace: &TraceEntry) -> bool {
        if !set_allows(&self.from, Some(&trace.from)) {
            return false;
        }
        if !set_allows(&self.to, trace.to.as_ref()) {
            return false;
        }
        if !set_allows(&self.selector, trace.selector().as_ref()) {
            return false;
        }
        if self
            .is_top_level
            .is_some_and(|want| want != trace.is_top_level())
        {
            return false;
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
    type Meta = M;
    type Blob = B;
    type Filter = TraceFilter;
    type Record = TraceEntry;
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
        // One trace blob item per container (= per tx); rows are its
        // DFS-flattened frames.
        let tx_index = u32::try_from(container_idx)
            .map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;
        crate::external::decode_external_trace_container(bytes, tx_index, tx_status)
    }

    fn into_record_owned(
        stored: StoredTrace,
        block_record: &BlockRecord,
        _idx_in_block: usize,
    ) -> Result<TraceEntry> {
        Ok(stored.into_trace_entry(block_record.block_number, block_record.block_hash))
    }
}
