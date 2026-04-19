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
use alloy_primitives::Address;
use bytes::Bytes as RawBytes;

use super::types::{
    decode_envelope, selector_from_envelope, BlockTxHeader, StoredTxEnvelope, TxEntry,
};
use crate::{
    blocks::Block,
    engine::{
        clause::{IndexedClause, IndexedFilter},
        family::Family,
        tables::Tables,
    },
    error::{MonadChainDataError, Result},
    primitives::{
        page::{QueryOrder, DEFAULT_QUERY_LIMIT},
        refs::BlockRef,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTransactionsRequest {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub order: QueryOrder,
    /// Target tx count. Defaults to [`DEFAULT_QUERY_LIMIT`].
    pub limit: usize,
    pub filter: TxFilter,
    pub relations: TxsRelations,
}

impl Default for QueryTransactionsRequest {
    fn default() -> Self {
        Self {
            from_block: None,
            to_block: None,
            order: QueryOrder::default(),
            limit: DEFAULT_QUERY_LIMIT,
            filter: TxFilter::default(),
            relations: TxsRelations::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTransactionsResponse {
    pub txs: Vec<TxEntry>,
    pub blocks: Vec<Block>,
    pub from_block: BlockRef,
    pub to_block: BlockRef,
    pub cursor_block: BlockRef,
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
        // `selector` filter; parse once and reuse for both.
        if self.to.is_some() || self.selector.is_some() {
            let Ok(envelope) = decode_envelope(&tx.signed_tx_bytes) else {
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

#[allow(dead_code)]
pub struct TxMaterializer<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
}

#[allow(dead_code)]
impl<'a, M: MetaStore, B: BlobStore> TxMaterializer<'a, M, B> {
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

    /// Resolves and materializes one tx by block number and block-local index.
    pub async fn load_tx_at(&self, block_number: u64, tx_idx: usize) -> Result<TxEntry> {
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

        if tx_idx + 1 >= header.offsets.len() {
            return Err(MonadChainDataError::Decode("tx index out of range"));
        }
        let start = header.offsets[tx_idx] as usize;
        let end = header.offsets[tx_idx + 1] as usize;

        let bytes = self
            .tables
            .family(Family::Tx)
            .read_block_blob_range(block_number, start, end)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing block tx blob"))?;

        let stored = StoredTxEnvelope::decode(&bytes)?;
        let tx_idx_u32 =
            u32::try_from(tx_idx).map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;
        Ok(stored.into_tx_entry(
            block_record.block_number,
            block_record.block_hash,
            tx_idx_u32,
        ))
    }
}
