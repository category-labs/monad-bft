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

use super::{LogBlockHeader, RawLogEntry};
use crate::{
    error::{MonadChainDataError, Result},
    family::FinalizedBlock,
    kernel::bitmap::{
        encode_grouped_bitmap_fragments, stream_entries_for_log, BitmapFragmentWrite,
    },
    primitives::state::{BlockRecord, FamilyWindowRecord, LogId},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogIngestPlan {
    pub block_record: BlockRecord,
    pub block_log_header: LogBlockHeader,
    pub block_log_blob: Vec<u8>,
    pub bitmap_fragments: Vec<BitmapFragmentWrite>,
    pub written_logs: usize,
}

impl LogIngestPlan {
    /// Derives the per-block log artifacts and index fragments for one finalized block.
    pub fn build(block: &FinalizedBlock, first_log_id: LogId) -> Result<Self> {
        // First pass writes only per-block payload/header artifacts plus the shared block record.
        // Later commits add global log IDs, directory fragments, and bitmap index artifacts.
        let logs = Self::flatten_logs(block)?;
        let log_count = u32::try_from(logs.len())
            .map_err(|_| MonadChainDataError::Decode("log count overflow"))?;

        let (block_log_header, block_log_blob) = Self::encode_block_logs(&logs)?;
        let bitmap_fragments = Self::collect_bitmap_fragments(&logs, first_log_id)?;
        let block_record = BlockRecord {
            block_number: block.block_number,
            block_hash: block.block_hash,
            parent_hash: block.parent_hash,
            logs: FamilyWindowRecord {
                first_log_id,
                count: log_count,
            },
        };

        Ok(Self {
            block_record,
            block_log_header,
            block_log_blob,
            bitmap_fragments,
            written_logs: logs.len(),
        })
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

    fn encode_block_logs(logs: &[RawLogEntry]) -> Result<(LogBlockHeader, Vec<u8>)> {
        let mut offsets = Vec::with_capacity(logs.len() + 1);
        let mut blob = Vec::new();

        for log in logs {
            offsets.push(
                u32::try_from(blob.len())
                    .map_err(|_| MonadChainDataError::Decode("block log blob too large"))?,
            );
            blob.extend_from_slice(&log.encode());
        }

        offsets.push(
            u32::try_from(blob.len())
                .map_err(|_| MonadChainDataError::Decode("block log blob too large"))?,
        );

        Ok((LogBlockHeader { offsets }, blob))
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
