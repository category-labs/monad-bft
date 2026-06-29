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
use monad_query_errors::{MonadChainDataError, Result};
use monad_query_primitives::{CallKind, EvmBlockHeader, Hash32};

use crate::{
    external::ExternalPayloadSpec,
    logs::StoredLog,
    txs::{StoredTxEnvelope, TxLocation},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedBlock {
    pub header: EvmBlockHeader,
    pub logs_by_tx: Vec<Vec<Log>>,
    pub txs: Vec<IngestTx>,
    /// DFS-flattened call frames across all txs in the block; the producer
    /// assigns each frame its `tx_index` and `trace_address` before ingest.
    pub traces: Vec<IngestTrace>,
    /// Archive payload locators for external-payload ingest; `None` for
    /// native ingest. Required (hard error) on every block when the engine
    /// runs in external payload mode.
    pub external: Option<ExternalPayloadSpec>,
}

/// Per-tx envelope in a finalized block. `sender` is caller-authoritative
/// (never recovered from `signed_tx_bytes`); ingest validates
/// `txs.len() == logs_by_tx.len()` when `txs` is non-empty.
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
    /// Frame-level status. `0 == EVMC_SUCCESS` per the tracer's encoding;
    /// any non-zero value is some flavour of revert or VM error.
    pub status: u8,
    pub depth: u32,
    pub tx_index: u32,
    /// OpenEthereum-style path-from-root within the tx's call tree;
    /// empty for the top-level frame.
    pub trace_address: Vec<u32>,
    /// Status of the containing top-level tx, from receipts at ingest
    /// (`true == receipt succeeded`). `has_transfer` ANDs this with
    /// `status == 0` to exclude successful sub-calls of reverted txs.
    pub tx_status: bool,
}

impl IngestTrace {
    /// THE definition of the `has_transfer` index bit: every trace frame this
    /// predicate accepts gets the bit at ingest, and the transfers view is
    /// exactly the frames carrying it (no read-side mirror exists). A frame
    /// transfers value iff the call kind moves value (DelegateCall/StaticCall
    /// never do), value > 0, the frame succeeded (`status == 0`), and the
    /// containing tx committed. Both status checks are required: the tracer
    /// does not rewrite descendant statuses when a parent reverts.
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
                let tx_index = u32::try_from(idx)
                    .map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;
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

    /// Flattens this block's per-tx log lists into block-ordered raw row entries.
    pub fn flatten_logs(&self) -> Result<Vec<StoredLog>> {
        let total_logs: usize = self.logs_by_tx.iter().map(|tx| tx.len()).sum();
        let mut logs = Vec::with_capacity(total_logs);

        for (tx_index, tx_logs) in self.logs_by_tx.iter().enumerate() {
            let tx_index = u32::try_from(tx_index)
                .map_err(|_| MonadChainDataError::Decode("tx index overflow"))?;

            for log in tx_logs {
                let log_index = u32::try_from(logs.len())
                    .map_err(|_| MonadChainDataError::Decode("log index overflow"))?;
                logs.push(StoredLog {
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
}
