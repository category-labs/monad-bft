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

use std::collections::{BTreeMap, BTreeSet, HashSet};

use alloy_consensus::Transaction;
use alloy_primitives::Address;
use futures::{stream, StreamExt, TryStreamExt};

use super::types::{selector_from_envelope, StoredTxEnvelope, TxEntry};
use crate::{
    blocks::Block,
    engine::{
        bitmap::IndexKind,
        clause::{set_allows, IndexedClause, IndexedFilter},
        family::Family,
        query::{
            family_runner::{IndexedFamilyQuery, IndexedQueryStats},
            row_cache::RowCache,
        },
        tables::Tables,
    },
    error::{MonadChainDataError, Result},
    primitives::{limits::QueryEnvelope, records::BlockRecord, refs::BlockSpan},
    store::{BlobStore, MetaStore},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TxFilter {
    pub from: Option<HashSet<Address>>,
    pub to: Option<HashSet<Address>>,
    pub selector: Option<HashSet<[u8; 4]>>,
}

/// Opt-in relations joined onto a transactions query response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TxsRelations {
    /// Populate `QueryTransactionsResponse::blocks` for the blocks that
    /// contributed txs in this page.
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
    /// Deduped, ascending by block number; `None` unless `relations.blocks`.
    pub blocks: Option<Vec<Block>>,
    /// Mirrors the resolved bounds and records the last block scanned. An
    /// empty `txs` means no next page; do not advance the cursor.
    pub span: BlockSpan,
}

impl IndexedFilter for TxFilter {
    type Record = TxEntry;

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

        clauses
    }

    fn matches(&self, tx: &TxEntry) -> bool {
        if !set_allows(&self.from, Some(&tx.sender)) {
            return false;
        }

        // Contract-creation txs and bad envelopes do not match a `to` or
        // `selector` filter; decode once and reuse for both.
        if self.to.is_some() || self.selector.is_some() {
            let Ok(envelope) = tx.envelope() else {
                return false;
            };

            if !set_allows(&self.to, envelope.to().as_ref()) {
                return false;
            }
            if !set_allows(&self.selector, selector_from_envelope(&envelope).as_ref()) {
                return false;
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
    type Meta = M;
    type Blob = B;
    type Filter = TxFilter;
    type Record = TxEntry;
    type StoredRow = StoredTxEnvelope;

    fn family() -> Family {
        Family::Tx
    }

    fn tables(&self) -> &Tables<M, B> {
        self.tables
    }

    fn row_cache(&self) -> &RowCache<StoredTxEnvelope> {
        &self.tables.row_caches().txs
    }

    fn decode_stored(bytes: &[u8]) -> Result<StoredTxEnvelope> {
        StoredTxEnvelope::decode(bytes)
    }

    fn into_record_owned(
        stored: StoredTxEnvelope,
        block_record: &BlockRecord,
        idx_in_block: usize,
    ) -> Result<TxEntry> {
        let tx_index = u32::try_from(idx_in_block)
            .map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;
        Ok(stored.into_tx_entry(block_record.block_number, block_record.block_hash, tx_index))
    }
}

/// Per-block fan-out when materializing the `transactions` relation;
/// each block is one coalesced batched read.
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
    // One batched load per block (frame reads coalesce, rows hit the row
    // cache); BTree ordering keeps the flattened output deduped ascending.
    let mut by_block: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    for (block_number, tx_index) in distinct {
        by_block
            .entry(block_number)
            .or_default()
            .push(tx_index as usize);
    }
    let materializer = TxMaterializer::new(tables);
    let stats = IndexedQueryStats::default();
    let per_block: Vec<Vec<TxEntry>> = stream::iter(by_block)
        .map(|(block_number, indices)| {
            let materializer = &materializer;
            let stats = &stats;
            async move {
                materializer
                    .load_records_in_block(block_number, &indices, stats)
                    .await
            }
        })
        .buffered(RELATION_TX_CONCURRENCY)
        .try_collect()
        .await?;
    Ok(per_block.into_iter().flatten().collect())
}
