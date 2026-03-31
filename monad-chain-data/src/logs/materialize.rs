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

use alloy_primitives::{Address, B256};

use super::LogEntry;
use crate::primitives::{page::QueryOrder, refs::BlockRef};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogFilter {
    pub address: Option<Vec<Address>>,
    pub topics: [Option<Vec<B256>>; 4],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLogsRequest {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub order: QueryOrder,
    pub limit: usize,
    pub filter: LogFilter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLogsResponse {
    pub logs: Vec<LogEntry>,
    pub from_block: BlockRef,
    pub to_block: BlockRef,
    pub cursor_block: BlockRef,
}
