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

use alloy_consensus::Transaction;

use super::types::{decode_envelope, selector_from_envelope, BlockTxHeader, StoredTxEnvelope};
use crate::{
    engine::bitmap::{encode_grouped_bitmap_fragments, sharded_stream_id, BitmapFragmentWrite},
    error::{MonadChainDataError, Result},
    family::{FinalizedBlock, IngestTx},
    primitives::state::{FamilyWindowRecord, TxId},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxIngestPlan {
    pub tx_window: FamilyWindowRecord,
    pub block_tx_header: BlockTxHeader,
    pub block_tx_blob: Vec<u8>,
    pub bitmap_fragments: Vec<BitmapFragmentWrite>,
    pub written_txs: usize,
}

impl TxIngestPlan {
    /// Derives the per-block tx artifacts and index fragments for one finalized block.
    pub fn build(block: &FinalizedBlock, first_tx_id: TxId) -> Result<Self> {
        let tx_count = u32::try_from(block.txs.len())
            .map_err(|_| MonadChainDataError::Decode("tx count overflow"))?;

        let (block_tx_header, block_tx_blob) = Self::encode_block_txs(&block.txs)?;
        let bitmap_fragments = Self::collect_bitmap_fragments(&block.txs, first_tx_id)?;
        let tx_window = FamilyWindowRecord {
            first_primary_id: first_tx_id.into(),
            count: tx_count,
        };

        Ok(Self {
            tx_window,
            block_tx_header,
            block_tx_blob,
            bitmap_fragments,
            written_txs: block.txs.len(),
        })
    }

    fn encode_block_txs(txs: &[IngestTx]) -> Result<(BlockTxHeader, Vec<u8>)> {
        let mut offsets = Vec::with_capacity(txs.len() + 1);
        let mut blob = Vec::new();

        for tx in txs {
            offsets.push(
                u32::try_from(blob.len())
                    .map_err(|_| MonadChainDataError::Decode("block tx blob too large"))?,
            );
            let stored = StoredTxEnvelope {
                tx_hash: tx.tx_hash,
                sender: tx.sender,
                signed_tx_bytes: tx.signed_tx_bytes.clone(),
            };
            blob.extend_from_slice(&stored.encode());
        }

        offsets.push(
            u32::try_from(blob.len())
                .map_err(|_| MonadChainDataError::Decode("block tx blob too large"))?,
        );

        Ok((BlockTxHeader { offsets }, blob))
    }

    fn collect_bitmap_fragments(
        txs: &[IngestTx],
        first_tx_id: TxId,
    ) -> Result<Vec<BitmapFragmentWrite>> {
        let mut stream_values = Vec::new();

        for (ordinal, tx) in txs.iter().enumerate() {
            let ordinal = u64::try_from(ordinal)
                .map_err(|_| MonadChainDataError::Decode("tx ordinal overflow"))?;
            let tx_id = first_tx_id.checked_add(ordinal)?;
            stream_values.extend(stream_entries_for_tx(tx, tx_id)?);
        }

        encode_grouped_bitmap_fragments(stream_values)
    }
}

/// Expands one tx into the indexed stream entries written at ingest time.
/// The "to" and "selector" streams are skipped for contract-creation txs
/// and for calldata shorter than 4 bytes.
fn stream_entries_for_tx(tx: &IngestTx, global_tx_id: TxId) -> Result<Vec<(String, u32)>> {
    let shard = global_tx_id.shard();
    let local = global_tx_id.local();
    let envelope = decode_envelope(&tx.signed_tx_bytes)?;

    let mut entries = Vec::with_capacity(3);
    entries.push((
        sharded_stream_id("from", tx.sender.as_slice(), shard),
        local,
    ));

    if let Some(to) = envelope.to() {
        entries.push((sharded_stream_id("to", to.as_slice(), shard), local));
    }
    if let Some(selector) = selector_from_envelope(&envelope) {
        entries.push((
            sharded_stream_id("selector", selector.as_slice(), shard),
            local,
        ));
    }

    Ok(entries)
}
