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

use alloy_primitives::B256;

use super::{BlockBlobHeader, RawLogEntry};
use crate::{
    engine::bitmap::{
        encode_grouped_bitmap_fragments, sharded_stream_id, touched_streams_by_page,
        BitmapFragmentWrite,
    },
    engine::digest::{ArtifactChecksum, RowDigest},
    engine::row_codec::RowCodec,
    error::{MonadChainDataError, Result},
    family::FinalizedBlock,
    primitives::state::{FamilyWindowRecord, LogId},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogIngestPlan {
    pub log_window: FamilyWindowRecord,
    pub block_log_header: BlockBlobHeader,
    pub block_log_blob: Vec<u8>,
    pub bitmap_fragments: Vec<BitmapFragmentWrite>,
    pub touched_bitmap_streams_by_page: BTreeMap<u64, BTreeSet<String>>,
    /// Checksum over this block's uncompressed log row payloads, in order. Fed
    /// into the per-block artifact digest for standby ingest verification.
    pub rows_digest: ArtifactChecksum,
    pub written_logs: usize,
}

impl LogIngestPlan {
    /// Derives the per-block log artifacts and index fragments for one finalized block.
    pub fn build(block: &FinalizedBlock, first_log_id: LogId, codec: &RowCodec) -> Result<Self> {
        Self::validate_logs(block)?;
        let logs = Self::flatten_logs(block)?;
        let log_count = u32::try_from(logs.len())
            .map_err(|_| MonadChainDataError::Decode("log count overflow"))?;

        let (block_log_header, block_log_blob, rows_digest) =
            Self::encode_block_logs(&logs, codec)?;
        let bitmap_fragments = Self::collect_bitmap_fragments(&logs, first_log_id)?;
        let touched_bitmap_streams_by_page = touched_streams_by_page(&bitmap_fragments)?;
        let log_window = FamilyWindowRecord {
            first_primary_id: first_log_id.into(),
            count: log_count,
        };

        Ok(Self {
            log_window,
            block_log_header,
            block_log_blob,
            bitmap_fragments,
            touched_bitmap_streams_by_page,
            rows_digest,
            written_logs: logs.len(),
        })
    }

    fn validate_logs(block: &FinalizedBlock) -> Result<()> {
        for tx_logs in &block.logs_by_tx {
            for log in tx_logs {
                if log.data.topics().len() > 4 {
                    return Err(MonadChainDataError::InvalidRequest("log topics exceed 4"));
                }
            }
        }

        Ok(())
    }

    fn flatten_logs(block: &FinalizedBlock) -> Result<Vec<RawLogEntry>> {
        let total_logs: usize = block.logs_by_tx.iter().map(|tx| tx.len()).sum();
        let mut logs = Vec::with_capacity(total_logs);

        for (tx_index, tx_logs) in block.logs_by_tx.iter().enumerate() {
            let tx_index = u32::try_from(tx_index)
                .map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;

            for log in tx_logs {
                let log_index = u32::try_from(logs.len())
                    .map_err(|_| MonadChainDataError::Decode("log index overflow"))?;
                logs.push(RawLogEntry {
                    tx_index,
                    log_index,
                    address: log.address,
                    topics: log.data.topics().to_vec(),
                    data: log.data.data.clone(),
                });
            }
        }

        Ok(logs)
    }

    fn encode_block_logs(
        logs: &[RawLogEntry],
        codec: &RowCodec,
    ) -> Result<(BlockBlobHeader, Vec<u8>, ArtifactChecksum)> {
        let mut offsets = Vec::with_capacity(logs.len() + 1);
        let mut blob = Vec::new();
        let mut rows_digest = RowDigest::new();
        let mut compressor = codec.block_compressor()?;

        for log in logs.iter() {
            offsets.push(
                u32::try_from(blob.len())
                    .map_err(|_| MonadChainDataError::Decode("block log blob too large"))?,
            );
            let raw = log.encode();
            // Fold the uncompressed payload before framing so the artifact
            // checksum is independent of the zstd codec/version.
            rows_digest.row(&raw);
            let frame = compressor.compress_row(&raw)?;
            blob.extend_from_slice(&frame);
        }

        offsets.push(
            u32::try_from(blob.len())
                .map_err(|_| MonadChainDataError::Decode("block log blob too large"))?,
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
        logs: &[RawLogEntry],
        first_log_id: LogId,
    ) -> Result<Vec<BitmapFragmentWrite>> {
        let mut stream_values = Vec::new();

        for (ordinal, log) in logs.iter().enumerate() {
            let ordinal = u64::try_from(ordinal)
                .map_err(|_| MonadChainDataError::Decode("log ordinal overflow"))?;
            let log_id = first_log_id.checked_add(ordinal)?;
            stream_values.extend(stream_entries_for_log(
                log.address.as_slice(),
                &log.topics,
                log_id,
            ));
        }

        encode_grouped_bitmap_fragments(stream_values)
    }
}

/// Expands one log into the indexed stream entries written at ingest time.
fn stream_entries_for_log(
    address: &[u8],
    topics: &[B256],
    global_log_id: LogId,
) -> Vec<(String, u32)> {
    let shard = global_log_id.shard();
    let local = global_log_id.local();

    let mut entries = Vec::with_capacity(5);
    entries.push((sharded_stream_id("addr", address, shard), local));

    let topic_kinds = ["topic0", "topic1", "topic2", "topic3"];
    for (topic, kind) in topics.iter().zip(topic_kinds) {
        entries.push((sharded_stream_id(kind, topic.as_slice(), shard), local));
    }

    entries
}
