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

use alloy_consensus::Transaction;
use alloy_primitives::{Address, Bytes};
use bytes::Bytes as RawBytes;

use super::types::{selector_from_envelope, BlockTxHeader, StoredTxEnvelope, TxEntry};
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

impl<'a, M: MetaStore, B: BlobStore> IndexedFamilyQuery<M, B> for TxMaterializer<'a, M, B> {
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

    async fn load_record_at(&self, block_number: u64, idx_in_block: usize) -> Result<TxEntry> {
        let block_record = self
            .tables
            .blocks()
            .load_record(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block record"))?;
        let header_bytes = self
            .tables
            .family(Family::Tx)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block tx header"))?;
        let header = BlockTxHeader::decode(&header_bytes)?;

        if idx_in_block + 1 >= header.offsets.len() {
            return Err(MonadChainDataError::Decode("tx index out of range"));
        }
        let start = header.offsets[idx_in_block] as usize;
        let end = header.offsets[idx_in_block + 1] as usize;

        let bytes = self
            .tables
            .family(Family::Tx)
            .read_block_blob_range(block_number, start, end)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block tx blob"))?;

        let stored = StoredTxEnvelope::decode(&bytes)?;
        let tx_idx_u32 = u32::try_from(idx_in_block)
            .map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;
        Ok(stored.into_tx_entry(
            block_record.block_number,
            block_record.block_hash,
            tx_idx_u32,
        ))
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

        let header_bytes = self
            .tables
            .family(Family::Tx)
            .load_block_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block tx header"))?;
        let header = BlockTxHeader::decode(&header_bytes)?;
        let blob = self
            .tables
            .family(Family::Tx)
            .load_block_blob(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block tx blob"))?;

        let txs = load_filtered_block_txs(&header, &blob, &block_record, order, filter)?;

        Ok((block_ref, txs))
    }
}

fn load_filtered_block_txs(
    header: &BlockTxHeader,
    blob: &Bytes,
    block_record: &BlockRecord,
    order: QueryOrder,
    filter: &TxFilter,
) -> Result<Vec<TxEntry>> {
    let count = header.tx_count();
    let indices: Box<dyn Iterator<Item = usize>> = match order {
        QueryOrder::Ascending => Box::new(0..count),
        QueryOrder::Descending => Box::new((0..count).rev()),
    };

    let mut txs = Vec::new();
    for tx_idx in indices {
        let tx = decode_tx_at(
            header,
            blob.as_ref(),
            tx_idx,
            block_record.block_number,
            block_record.block_hash,
        )?;
        if filter.matches(&tx) {
            txs.push(tx);
        }
    }

    Ok(txs)
}

pub(crate) fn decode_tx_at(
    header: &BlockTxHeader,
    blob: &[u8],
    tx_idx: usize,
    block_number: u64,
    block_hash: Hash32,
) -> Result<TxEntry> {
    if tx_idx + 1 >= header.offsets.len() {
        return Err(MonadChainDataError::Decode("tx index out of range"));
    }

    let start = header.offsets[tx_idx] as usize;
    let end = header.offsets[tx_idx + 1] as usize;
    if start > end || end > blob.len() {
        return Err(MonadChainDataError::Decode("invalid tx range"));
    }

    let stored = StoredTxEnvelope::decode(&blob[start..end])?;
    let tx_idx_u32 =
        u32::try_from(tx_idx).map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;
    Ok(stored.into_tx_entry(block_number, block_hash, tx_idx_u32))
}
