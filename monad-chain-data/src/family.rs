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
    /// DFS-flattened call frames across all txs in the block. The producer
    /// is responsible for flattening the per-tx `Vec<Vec<CallFrame>>` and
    /// assigning each frame a `tx_index` and `trace_address` before ingest.
    pub traces: Vec<IngestTrace>,
}

/// Per-transaction envelope carried by a finalized block. `sender` is
/// caller-authoritative and is not recovered from `signed_tx_bytes`;
/// indexed `from` queries read `sender` directly. Ingest validates
/// `txs.len() == logs_by_tx.len()` when `txs` is non-empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestTx {
    pub tx_hash: Hash32,
    pub sender: Address,
    pub signed_tx_bytes: Bytes,
}

/// Local mirror of the archive's `CallKind` so the library crate does not
/// take a dependency on `monad-archive`. `monad-chain-data-ingest` converts
/// archive `CallKind` values into this type at the boundary.
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

/// A single DFS-flattened call frame from a tx's execution trace. The
/// producer is responsible for flattening `Vec<Vec<CallFrame>>` per tx and
/// computing `trace_address` (the OpenEthereum-style path-from-root index
/// into the tx's call tree).
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
    /// Path-from-root for this frame within its tx's call tree. The
    /// top-level frame has an empty `trace_address`.
    pub trace_address: Vec<u32>,
    /// Status of the top-level tx that contains this frame, threaded in
    /// from receipts at ingest. `true == receipt succeeded`. The
    /// `has_transfer` predicate AND-s this with `frame.status == 0` so a
    /// successful sub-call inside a reverted parent tx is excluded.
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
