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
//! bytes back into rows live in `monad-chain-data`.

use monad_query_errors::{MonadChainDataError, Result};

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
    /// Container partition relative to `base_offset`: container `j` spans
    /// `[container_offsets[j], container_offsets[j + 1])`; length is
    /// `containers + 1` (`[0]` for an empty block).
    pub container_offsets: Vec<u32>,
    /// Cumulative row counts per container; empty = identity (one row per
    /// container, the tx family) — also empty when there are no containers.
    pub container_rows: Vec<u32>,
    /// Per-container tx-status bitvec, LSB-first (traces only; empty
    /// otherwise). Trailing pad bits must be zero.
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
    /// Structural invariants shared by every family: a non-empty key, a
    /// well-formed container partition (each container non-empty, so item
    /// ranges are exact), and a consistent cumulative row manifest whose
    /// per-container counts match `rows_per_container`.
    fn validate(&self, rows_per_container: &[u32], identity: bool) -> Result<()> {
        let containers = rows_per_container.len();
        if self.key.is_empty() {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload region missing archive key",
            ));
        }
        if self.container_offsets.len() != containers + 1 || self.container_offsets[0] != 0 {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload container partition has the wrong shape",
            ));
        }
        if self.container_offsets.windows(2).any(|w| w[0] >= w[1]) {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload containers must be non-empty and ascending",
            ));
        }
        if self.container_offsets.last().copied().unwrap_or(0) as u64 + u64::from(self.base_offset)
            > u32::MAX as u64
        {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload region exceeds the u32 offset space",
            ));
        }
        if identity {
            if !self.container_rows.is_empty() {
                return Err(MonadChainDataError::InvalidBlock(
                    "identity external region must omit container_rows",
                ));
            }
            if rows_per_container.iter().any(|&rows| rows != 1) {
                return Err(MonadChainDataError::InvalidBlock(
                    "identity external region requires one row per container",
                ));
            }
            return Ok(());
        }
        if self.container_rows.len() != containers {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload container_rows length mismatch",
            ));
        }
        let mut cumulative = 0u32;
        for (&rows, &cum) in rows_per_container.iter().zip(&self.container_rows) {
            cumulative = cumulative
                .checked_add(rows)
                .ok_or(MonadChainDataError::InvalidBlock(
                    "external payload row count overflow",
                ))?;
            if cum != cumulative {
                return Err(MonadChainDataError::InvalidBlock(
                    "external payload container_rows disagrees with the block's rows",
                ));
            }
        }
        Ok(())
    }

    /// Status-bitvec invariants: exactly `containers.div_ceil(8)` bytes with
    /// zero trailing pad bits, matching `statuses` bit for bit.
    fn validate_status(&self, statuses: impl ExactSizeIterator<Item = bool>) -> Result<()> {
        let containers = statuses.len();
        if self.container_status.len() != containers.div_ceil(8) {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload container_status length mismatch",
            ));
        }
        let mut expected = vec![0u8; containers.div_ceil(8)];
        for (j, status) in statuses.enumerate() {
            if status {
                expected[j / 8] |= 1 << (j % 8);
            }
        }
        if self.container_status != expected {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload container_status disagrees with the block's receipts",
            ));
        }
        Ok(())
    }
}

impl ExternalPayloadSpec {
    /// Validates the spec against the decoded block it locates: the spec was
    /// scanned from this block number, container counts equal the tx count
    /// for every family, row manifests match the block's logs/frames exactly,
    /// and the trace status bits match every frame's `tx_status`. Hard error
    /// on any mismatch — a bad locator would otherwise serve another tx's
    /// bytes.
    pub fn validate(&self, block_number: u64, block: &FinalizedBlock) -> Result<()> {
        if self.block_number != block_number {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload spec belongs to a different block",
            ));
        }
        let tx_count = block.txs.len();
        if block.logs_by_tx.len() != tx_count {
            return Err(MonadChainDataError::InvalidBlock(
                "external payload requires one receipt container per tx",
            ));
        }

        self.txs.validate(&vec![1u32; tx_count], true)?;
        self.txs.validate_status(std::iter::empty())?;

        let logs_per_tx: Vec<u32> = block
            .logs_by_tx
            .iter()
            .map(|logs| u32::try_from(logs.len()))
            .collect::<std::result::Result<_, _>>()
            .map_err(|_| MonadChainDataError::Decode("log count overflow"))?;
        self.logs.validate(&logs_per_tx, false)?;
        self.logs.validate_status(std::iter::empty())?;

        let mut frames_per_tx = vec![0u32; tx_count];
        let mut statuses: Vec<Option<bool>> = vec![None; tx_count];
        for trace in &block.traces {
            let slot = frames_per_tx.get_mut(trace.tx_index as usize).ok_or(
                MonadChainDataError::InvalidBlock("external payload trace tx_index out of range"),
            )?;
            *slot += 1;
            let status = &mut statuses[trace.tx_index as usize];
            if status.is_some_and(|s| s != trace.tx_status) {
                return Err(MonadChainDataError::InvalidBlock(
                    "external payload trace tx_status inconsistent within a tx",
                ));
            }
            *status = Some(trace.tx_status);
        }
        self.traces.validate(&frames_per_tx, false)?;
        // Containers without frames carry the receipt status the source read;
        // rows only ever consult the bits of their own container, so bits of
        // frameless containers are accepted as supplied.
        let supplied_bit = |j: usize| {
            self.traces
                .container_status
                .get(j / 8)
                .is_some_and(|byte| byte >> (j % 8) & 1 == 1)
        };
        self.traces.validate_status(
            statuses
                .iter()
                .enumerate()
                .map(|(j, status)| status.unwrap_or_else(|| supplied_bit(j)))
                .collect::<Vec<_>>()
                .into_iter(),
        )?;
        Ok(())
    }
}
