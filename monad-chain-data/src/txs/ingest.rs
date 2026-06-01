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

use std::collections::{BTreeMap, BTreeSet};

use alloy_consensus::Transaction;

use super::types::{decode_envelope, selector_from_envelope, StoredTxEnvelope};
use super::BlockBlobHeader;
use crate::{
    engine::bitmap::{
        encode_grouped_bitmap_fragments, sharded_stream_id, touched_streams_by_page,
        BitmapFragmentWrite,
    },
    engine::digest::{ArtifactChecksum, RowDigest},
    engine::row_codec::RowCodec,
    error::{MonadChainDataError, Result},
    family::{FinalizedBlock, Hash32, IngestTx},
    primitives::state::{FamilyWindowRecord, TxId},
    txs::types::TxLocation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxIngestPlan {
    pub tx_window: FamilyWindowRecord,
    pub block_tx_header: BlockBlobHeader,
    pub block_tx_blob: Vec<u8>,
    pub bitmap_fragments: Vec<BitmapFragmentWrite>,
    pub touched_bitmap_streams_by_page: BTreeMap<u64, BTreeSet<String>>,
    /// (tx_hash, location) pairs to write into `tx_hash_index` for this
    /// block. Caller-authoritative `tx_hash`; collisions last-write-win.
    pub(crate) hash_locations: Vec<(Hash32, TxLocation)>,
    /// Checksum over this block's uncompressed tx row payloads, in order. Fed
    /// into the per-block artifact digest for standby ingest verification.
    pub rows_digest: ArtifactChecksum,
    pub written_txs: usize,
}

impl TxIngestPlan {
    /// Derives the per-block tx artifacts and index fragments for one finalized block.
    pub fn build(block: &FinalizedBlock, first_tx_id: TxId, codec: &RowCodec) -> Result<Self> {
        let tx_count = u32::try_from(block.txs.len())
            .map_err(|_| MonadChainDataError::Decode("tx count overflow"))?;

        let (block_tx_header, block_tx_blob, rows_digest) =
            Self::encode_block_txs(&block.txs, codec)?;
        let bitmap_fragments = Self::collect_bitmap_fragments(&block.txs, first_tx_id)?;
        let touched_bitmap_streams_by_page = touched_streams_by_page(&bitmap_fragments)?;
        let hash_locations = Self::collect_hash_locations(block)?;
        let tx_window = FamilyWindowRecord {
            first_primary_id: first_tx_id.into(),
            count: tx_count,
        };

        Ok(Self {
            tx_window,
            block_tx_header,
            block_tx_blob,
            bitmap_fragments,
            touched_bitmap_streams_by_page,
            hash_locations,
            rows_digest,
            written_txs: block.txs.len(),
        })
    }

    fn collect_hash_locations(block: &FinalizedBlock) -> Result<Vec<(Hash32, TxLocation)>> {
        block
            .txs
            .iter()
            .enumerate()
            .map(|(idx, tx)| {
                let tx_idx = u32::try_from(idx)
                    .map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;
                Ok((
                    tx.tx_hash,
                    TxLocation {
                        block_number: block.block_number(),
                        tx_idx,
                    },
                ))
            })
            .collect()
    }

    fn encode_block_txs(
        txs: &[IngestTx],
        codec: &RowCodec,
    ) -> Result<(BlockBlobHeader, Vec<u8>, ArtifactChecksum)> {
        let mut offsets = Vec::with_capacity(txs.len() + 1);
        let mut blob = Vec::new();
        let mut rows_digest = RowDigest::new();
        let mut compressor = codec.block_compressor()?;

        for tx in txs.iter() {
            offsets.push(
                u32::try_from(blob.len())
                    .map_err(|_| MonadChainDataError::Decode("block tx blob too large"))?,
            );
            let stored = StoredTxEnvelope {
                tx_hash: tx.tx_hash,
                sender: tx.sender,
                signed_tx_bytes: tx.signed_tx_bytes.clone(),
            };
            let raw = stored.encode();
            // Fold the uncompressed payload before framing so the artifact
            // checksum is independent of the zstd codec/version.
            rows_digest.row(&raw);
            let frame = compressor.compress_row(&raw)?;
            blob.extend_from_slice(&frame);
        }

        offsets.push(
            u32::try_from(blob.len())
                .map_err(|_| MonadChainDataError::Decode("block tx blob too large"))?,
        );

        Ok((
            BlockBlobHeader {
                offsets,
                dict_version: codec.version(),
            },
            blob,
            rows_digest.finish(),
        ))
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
