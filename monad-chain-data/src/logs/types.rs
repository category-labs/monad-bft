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

use alloy_primitives::{Address, Bytes, B256};

use crate::family::Hash32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub tx_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogBlockHeader {
    pub offsets: Vec<u32>,
}
