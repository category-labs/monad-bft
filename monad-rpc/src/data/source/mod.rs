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

use monad_types::BlockId;

pub use self::{
    archive::ArchiveDataSource,
    historical::{HistoricalDataSource, HistoricalDataSourceStack},
    stack::DataSourceStack,
};

mod archive;
mod historical;
mod stack;

pub type DataSourceResult<T> = Result<T, DataSourceError>;

#[derive(Debug, thiserror::Error)]
pub enum DataSourceError {
    #[error("data source error: {0}")]
    Internal(String),
}

#[derive(Clone, Copy, Debug)]
pub enum BlockCommitState {
    Proposed,
    Voted,
    Finalized,
}

/// A resolved block reference that has been successfully located by a data source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockPointer {
    Finalized(u64, Option<BlockId>),
    NonFinalized(u64, BlockId),
}
