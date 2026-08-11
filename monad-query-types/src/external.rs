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

//! External archive payload locators: per-family container byte ranges inside
//! an EXISTING monad-archive object ("BDR": per-block uncompressed RLP
//! objects), validated against the decoded block they locate.
//!
//! The byte-range reader trait ([`monad_query_primitives::ExternalBlobReader`])
//! lives in `monad-query-primitives`; the decode mirrors that turn the located
//! bytes back into rows live in `monad-query-read`.

use monad_query_errors::{QueryError, Result};

use crate::ingest_types::FinalizedBlock;

/// One family's byte layout inside its archive object, as supplied by the
/// ingest source. All offsets are object-relative through `base_offset`;
/// containers are contiguous (`container_offsets` is a partition), which the
/// source asserts when collapsing the scanner's exact item ranges into it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalFamilyRegion {
    /// Raw archive object key (e.g. `block/000000000123`); never empty.
    pub key: Vec<u8>,
    /// Start byte of the first container within the object.
    pub base_offset: u32,
    /// Container byte offsets relative to `base_offset`.
    /// Length is `containers + 1`, with `[0] = 0` for empty blocks.
    pub container_offsets: Vec<u32>,
    /// Cumulative row counts per container.
    /// Empty indicates identity mode (one row per container).
    pub container_rows: Vec<u32>,
    /// Per-container tx-status bitvec, LSB-first (traces only; empty otherwise).
    /// Trailing pad bits must be zero.
    pub container_status: Vec<u8>,
}

/// Per-block external payload locators for all three families.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalPayloadSpec {
    /// The block the locators were scanned from. Binds the spec to its block
    /// so a source that mis-pairs a spec with another block's `FinalizedBlock`
    /// fails validation even when per-family counts coincide (any equal tx
    /// count satisfies the identity tx manifest).
    pub block_number: u64,
    pub logs: ExternalFamilyRegion,
    pub txs: ExternalFamilyRegion,
    pub traces: ExternalFamilyRegion,
}

impl ExternalFamilyRegion {
    /// Validates structural invariants: non-empty key, well-formed partition
    /// with non-empty containers, and consistent row counts.
    fn validate(&self, rows_per_container: &[u32], identity: bool) -> Result<()> {
        let containers = rows_per_container.len();
        if self.key.is_empty() {
            return Err(QueryError::InvalidBlock(
                "external payload region missing archive key",
            ));
        }
        if self.container_offsets.len() != containers + 1 || self.container_offsets[0] != 0 {
            return Err(QueryError::InvalidBlock(
                "external payload container partition has the wrong shape",
            ));
        }
        if self
            .container_offsets
            .windows(2)
            .any(|window| window[0] >= window[1])
        {
            return Err(QueryError::InvalidBlock(
                "external payload containers must be non-empty and ascending",
            ));
        }
        if self.container_offsets.last().copied().unwrap_or(0) as u64 + u64::from(self.base_offset)
            > u32::MAX as u64
        {
            return Err(QueryError::InvalidBlock(
                "external payload region exceeds the u32 offset space",
            ));
        }
        if identity {
            if !self.container_rows.is_empty() {
                return Err(QueryError::InvalidBlock(
                    "identity external region must omit container_rows",
                ));
            }
            if rows_per_container.iter().any(|&rows| rows != 1) {
                return Err(QueryError::InvalidBlock(
                    "identity external region requires one row per container",
                ));
            }
            return Ok(());
        }
        if self.container_rows.len() != containers {
            return Err(QueryError::InvalidBlock(
                "external payload container_rows length mismatch",
            ));
        }
        let mut cumulative = 0u32;
        for (i, &rows) in rows_per_container.iter().enumerate() {
            cumulative = cumulative
                .checked_add(rows)
                .ok_or(QueryError::InvalidBlock(
                    "external payload row count overflow",
                ))?;
            if self.container_rows[i] != cumulative {
                return Err(QueryError::InvalidBlock(
                    "external payload container_rows disagrees with the block's rows",
                ));
            }
        }
        Ok(())
    }

    /// Validates the status bitvec: exactly `statuses.len().div_ceil(8)` bytes,
    /// matching `statuses` bit for bit, with zero trailing pad bits.
    fn validate_status(&self, statuses: &[bool]) -> Result<()> {
        let byte_count = statuses.len().div_ceil(8);

        if self.container_status.len() != byte_count {
            return Err(QueryError::InvalidBlock(
                "external payload container_status length mismatch",
            ));
        }

        for (i, &status) in statuses.iter().enumerate() {
            let bit_index = i % 8;
            let byte_index = i / 8;
            let bit_set = (self.container_status[byte_index] >> bit_index) & 1 == 1;
            if status != bit_set {
                return Err(QueryError::InvalidBlock(
                    "external payload container_status disagrees with the block's receipts",
                ));
            }
        }

        // Trailing pad bits in the final byte must be zero.
        let pad_bits = statuses.len() % 8;
        if pad_bits != 0 && self.container_status[byte_count - 1] >> pad_bits != 0 {
            return Err(QueryError::InvalidBlock(
                "external payload container_status disagrees with the block's receipts",
            ));
        }
        Ok(())
    }
}

impl ExternalPayloadSpec {
    /// Validates the spec against a decoded block: matches block number,
    /// container counts, row counts, and trace status bits.
    pub fn validate(&self, block_number: u64, block: &FinalizedBlock) -> Result<()> {
        if self.block_number != block_number {
            return Err(QueryError::InvalidBlock(
                "external payload spec belongs to a different block",
            ));
        }
        let tx_count = block.txs.len();
        if block.logs_by_tx.len() != tx_count {
            return Err(QueryError::InvalidBlock(
                "external payload requires one receipt container per tx",
            ));
        }

        self.txs.validate(&vec![1u32; tx_count], true)?;
        self.txs.validate_status(&[])?;

        let logs_per_tx: Vec<u32> = block
            .logs_by_tx
            .iter()
            .map(|tx_logs| {
                u32::try_from(tx_logs.len()).map_err(|_| QueryError::Decode("log count overflow"))
            })
            .collect::<Result<Vec<_>>>()?;
        self.logs.validate(&logs_per_tx, false)?;
        self.logs.validate_status(&[])?;

        // Count frames per tx and collect trace statuses
        let mut frames_per_tx = vec![0u32; tx_count];
        let mut trace_statuses: Vec<Option<bool>> = vec![None; tx_count];

        for trace in &block.traces {
            let tx_idx = trace.tx_index as usize;
            if tx_idx >= tx_count {
                return Err(QueryError::InvalidBlock(
                    "external payload trace tx_index out of range",
                ));
            }
            frames_per_tx[tx_idx] += 1;

            match trace_statuses[tx_idx] {
                None => trace_statuses[tx_idx] = Some(trace.tx_status),
                Some(existing_status) if existing_status == trace.tx_status => {}
                Some(_) => {
                    return Err(QueryError::InvalidBlock(
                        "external payload trace tx_status inconsistent within a tx",
                    ))
                }
            }
        }
        self.traces.validate(&frames_per_tx, false)?;

        // Build final status array, falling back to stored bits for containers without frames
        let get_status_bit = |container_idx: usize| {
            let byte_idx = container_idx / 8;
            let bit_idx = container_idx % 8;
            byte_idx < self.traces.container_status.len()
                && (self.traces.container_status[byte_idx] >> bit_idx) & 1 == 1
        };

        let final_statuses: Vec<bool> = trace_statuses
            .iter()
            .enumerate()
            .map(|(idx, status)| status.unwrap_or_else(|| get_status_bit(idx)))
            .collect();

        self.traces.validate_status(&final_statuses)?;
        Ok(())
    }
}
