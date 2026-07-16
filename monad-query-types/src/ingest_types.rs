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

use alloy_primitives::{Address, Bytes, Log, U256};
use monad_query_errors::{QueryError, Result};
use monad_query_primitives::{CallKind, EvmBlockHeader, Hash32};

use crate::{
    external::ExternalPayloadSpec,
    logs::StoredLog,
    traces::StoredTrace,
    txs::{StoredTxEnvelope, TxLocation},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedBlock {
    pub header: EvmBlockHeader,
    pub logs_by_tx: Vec<Vec<Log>>,
    pub txs: Vec<IngestTx>,
    /// DFS-flattened call frames; indexed by tx.
    pub traces: Vec<IngestTrace>,
    /// Archive payload locators for external-payload ingest.
    pub external: Option<ExternalPayloadSpec>,
}

/// Per-tx envelope in a finalized block.
/// Sender is caller-authoritative (never recovered from signed bytes).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestTx {
    pub tx_hash: Hash32,
    pub sender: Address,
    pub signed_tx_bytes: Bytes,
}

impl IngestTx {
    /// Encodes this tx as its [`StoredTxEnvelope`] storage row.
    pub fn encode_row(&self) -> Vec<u8> {
        StoredTxEnvelope {
            tx_hash: self.tx_hash,
            sender: self.sender,
            signed_tx_bytes: self.signed_tx_bytes.clone(),
        }
        .encode()
    }
}

/// A single DFS-flattened call frame from a tx's execution trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestTrace {
    pub typ: CallKind,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas: u64,
    pub gas_used: u64,
    pub input: Bytes,
    pub output: Bytes,
    /// Frame-level status: 0 = success, non-zero = revert/VM error.
    pub status: u8,
    pub depth: u32,
    pub tx_index: u32,
    /// OpenEthereum-style path from root within tx's call tree; empty for top-level.
    pub trace_address: Vec<u32>,
    /// Top-level tx receipt status from ingest (`true` = succeeded).
    pub tx_status: bool,
}

impl IngestTrace {
    /// Returns true if this frame represents a value transfer: the call kind
    /// moves value, value > 0, the frame succeeded, and the containing tx committed.
    pub fn is_transfer_frame(&self) -> bool {
        let kind_moves_value = matches!(
            self.typ,
            CallKind::Call
                | CallKind::CallCode
                | CallKind::Create
                | CallKind::Create2
                | CallKind::SelfDestruct
        );
        self.value > U256::ZERO && kind_moves_value && self.status == 0 && self.tx_status
    }

    /// Encodes this frame as its [`StoredTrace`] storage row.
    pub fn encode_row(&self) -> Vec<u8> {
        StoredTrace::from(self).encode()
    }
}

impl FinalizedBlock {
    pub fn block_number(&self) -> u64 {
        self.header.number
    }

    pub fn block_hash(&self) -> Hash32 {
        self.header.hash_slow()
    }

    pub fn parent_hash(&self) -> Hash32 {
        self.header.parent_hash
    }

    /// Derives the `(tx_hash, location)` pairs to write into `tx_hash_index` for
    /// this block. Caller-authoritative `tx_hash`; collisions last-write-win.
    pub fn tx_hash_locations(&self) -> Result<Vec<(Hash32, TxLocation)>> {
        self.txs
            .iter()
            .enumerate()
            .map(|(idx, tx)| {
                let tx_index =
                    u32::try_from(idx).map_err(|_| QueryError::Decode("tx index overflow"))?;
                Ok((
                    tx.tx_hash,
                    TxLocation {
                        block_number: self.block_number(),
                        tx_index,
                    },
                ))
            })
            .collect()
    }

    /// Flattens per-tx log lists into block-ordered row entries.
    pub fn flatten_logs(&self) -> Result<Vec<StoredLog>> {
        self.logs_by_tx
            .iter()
            .enumerate()
            .try_fold(
                (Vec::new(), 0u32),
                |(mut logs, mut log_index), (tx_index, tx_logs)| {
                    let tx_index_u32 = u32::try_from(tx_index)
                        .map_err(|_| QueryError::Decode("tx index overflow"))?;

                    for raw_log in tx_logs {
                        logs.push(StoredLog {
                            tx_index: tx_index_u32,
                            log_index,
                            address: raw_log.address,
                            topics: raw_log.data.topics().to_vec(),
                            data: raw_log.data.data.clone(),
                        });

                        log_index = log_index
                            .checked_add(1)
                            .ok_or(QueryError::Decode("log index overflow"))?;
                    }

                    Ok((logs, log_index))
                },
            )
            .map(|(logs, _)| logs)
    }
}
