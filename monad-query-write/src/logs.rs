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

use alloy_primitives::B256;
use monad_query_types::logs::StoredLog;

use crate::{
    engine::{
        bitmap::{IndexKind, StreamKey},
        digest::ChainDigest,
        row_codec::{digest_block_rows, encode_block_rows, RowCodec},
    },
    error::Result,
    primitives::records::BlockBlobHeader,
};

/// Compresses a block's log rows into the framed per-family blob.
pub(crate) fn encode_block_logs(
    logs: &[StoredLog],
    codec: &RowCodec,
) -> Result<(BlockBlobHeader, Vec<u8>, ChainDigest)> {
    encode_block_rows(logs, codec, "block log blob too large", |log| log.encode())
}

/// The [`encode_block_logs`] row digest alone (external-payload ingest).
pub(crate) fn digest_block_logs(logs: &[StoredLog]) -> ChainDigest {
    digest_block_rows(logs, |log| log.encode())
}

/// Expands one log into the indexed streams written at ingest time
/// (`ingest::index::accumulate_family` pairs each with the record's id).
pub(crate) fn stream_entries_for_log(address: &[u8], topics: &[B256]) -> Vec<StreamKey> {
    let mut entries = Vec::with_capacity(5);
    entries.push(StreamKey::new(IndexKind::Addr, address));

    for (topic, kind) in topics.iter().zip(IndexKind::TOPICS) {
        entries.push(StreamKey::new(kind, topic.as_slice()));
    }

    entries
}
