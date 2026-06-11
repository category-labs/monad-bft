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

use super::types::{decode_envelope, selector_from_envelope, StoredTxEnvelope};
use crate::{
    engine::{
        bitmap::{IndexKind, StreamKey},
        digest::ChainDigest,
        row_codec::{encode_block_rows, RowCodec},
    },
    error::{MonadChainDataError, Result},
    ingest_types::{FinalizedBlock, Hash32, IngestTx},
    primitives::records::BlockBlobHeader,
    txs::types::TxLocation,
};

/// Derives the `(tx_hash, location)` pairs to write into `tx_hash_index` for
/// one block. Caller-authoritative `tx_hash`; collisions last-write-win.
pub(crate) fn collect_hash_locations(block: &FinalizedBlock) -> Result<Vec<(Hash32, TxLocation)>> {
    block
        .txs
        .iter()
        .enumerate()
        .map(|(idx, tx)| {
            let tx_index =
                u32::try_from(idx).map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;
            Ok((
                tx.tx_hash,
                TxLocation {
                    block_number: block.block_number(),
                    tx_index,
                },
            ))
        })
        .collect()
}

/// Compresses a block's tx rows into the framed per-family blob.
pub(crate) fn encode_block_txs(
    txs: &[IngestTx],
    codec: &RowCodec,
) -> Result<(BlockBlobHeader, Vec<u8>, ChainDigest)> {
    encode_block_rows(txs, codec, "block tx blob too large", |tx| {
        StoredTxEnvelope {
            tx_hash: tx.tx_hash,
            sender: tx.sender,
            signed_tx_bytes: tx.signed_tx_bytes.clone(),
        }
        .encode()
    })
}

/// Expands one tx into the indexed streams written at ingest;
/// `to`/`selector` skipped for contract creations and calldata < 4 bytes.
pub(crate) fn stream_entries_for_tx(tx: &IngestTx) -> Result<Vec<StreamKey>> {
    let envelope = decode_envelope(&tx.signed_tx_bytes)?;

    let mut entries = Vec::with_capacity(3);
    entries.push(StreamKey::new(IndexKind::From, tx.sender.as_slice()));

    if let Some(to) = envelope.to() {
        entries.push(StreamKey::new(IndexKind::To, to.as_slice()));
    }
    if let Some(selector) = selector_from_envelope(&envelope) {
        entries.push(StreamKey::new(IndexKind::Selector, selector.as_slice()));
    }

    Ok(entries)
}
