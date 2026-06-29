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
pub use monad_query_types::txs::TxLocation;
use monad_query_types::txs::{decode_envelope, selector_from_envelope};

use crate::{
    engine::{
        bitmap::{IndexKind, StreamKey},
        digest::ChainDigest,
        row_codec::{digest_block_rows, encode_block_rows, RowCodec},
    },
    error::Result,
    ingest_types::IngestTx,
    primitives::records::BlockBlobHeader,
};

/// Compresses a block's tx rows into the framed per-family blob.
pub(crate) fn encode_block_txs(
    txs: &[IngestTx],
    codec: &RowCodec,
) -> Result<(BlockBlobHeader, Vec<u8>, ChainDigest)> {
    encode_block_rows(txs, codec, "block tx blob too large", IngestTx::encode_row)
}

/// The [`encode_block_txs`] row digest alone (external-payload ingest).
pub(crate) fn digest_block_txs(txs: &[IngestTx]) -> ChainDigest {
    digest_block_rows(txs, IngestTx::encode_row)
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
