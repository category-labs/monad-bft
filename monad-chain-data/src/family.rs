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

use alloy_primitives::{Address, Bytes, Log, B256};

use crate::primitives::EvmBlockHeader;

pub type Hash32 = B256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedBlock {
    pub header: EvmBlockHeader,
    pub logs_by_tx: Vec<Vec<Log>>,
    pub txs: Vec<IngestTx>,
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
