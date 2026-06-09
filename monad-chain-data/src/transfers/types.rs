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

use alloy_primitives::{Address, U256};

use crate::family::{CallKind, Hash32};

/// Public, owned per-transfer view returned by queries. Projected from a
/// `TraceEntry` whose `has_transfer` bit was set at ingest; in
/// particular, `to` is always present (Create/Create2 resolve to the
/// new contract address; SelfDestruct resolves to the beneficiary).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferEntry {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub tx_index: u32,
    pub trace_address: Vec<u32>,
    pub typ: CallKind,
    pub from: Address,
    pub to: Address,
    pub value: U256,
}

impl TransferEntry {
    pub fn is_top_level(&self) -> bool {
        self.trace_address.is_empty()
    }
}
