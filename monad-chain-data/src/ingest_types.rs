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

use alloy_primitives::{Address, Bytes, Log, B256, U256};

use crate::primitives::EvmBlockHeader;

pub type Hash32 = B256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedBlock {
    pub header: EvmBlockHeader,
    pub logs_by_tx: Vec<Vec<Log>>,
    pub txs: Vec<IngestTx>,
    /// DFS-flattened call frames across all txs in the block; the producer
    /// assigns each frame its `tx_index` and `trace_address` before ingest.
    pub traces: Vec<IngestTrace>,
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

/// Local mirror of the archive's `CallKind` so this crate avoids a
/// `monad-archive` dependency; the ingest binary converts at the boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CallKind {
    Call,
    DelegateCall,
    CallCode,
    Create,
    Create2,
    SelfDestruct,
    StaticCall,
}

impl CallKind {
    pub const fn as_u8(self) -> u8 {
        match self {
            CallKind::Call => 0,
            CallKind::StaticCall => 1,
            CallKind::DelegateCall => 2,
            CallKind::CallCode => 3,
            CallKind::Create => 4,
            CallKind::Create2 => 5,
            CallKind::SelfDestruct => 6,
        }
    }

    pub fn from_u8(byte: u8) -> Option<Self> {
        Some(match byte {
            0 => CallKind::Call,
            1 => CallKind::StaticCall,
            2 => CallKind::DelegateCall,
            3 => CallKind::CallCode,
            4 => CallKind::Create,
            5 => CallKind::Create2,
            6 => CallKind::SelfDestruct,
            _ => return None,
        })
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
}
