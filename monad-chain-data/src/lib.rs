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

pub mod api;
pub mod error;
pub mod family;
pub mod ingest;
pub mod kernel;
pub mod logs;
pub mod primitives;
pub mod query;
pub mod store;

pub use alloy_primitives::{Address, Bytes, Log, LogData, B256};
pub use api::{IngestOutcome, MonadChainDataService};
pub use family::{FinalizedBlock, Hash32};
pub use kernel::tables::Tables;
pub use logs::{LogEntry, LogFilter, QueryLogsRequest, QueryLogsResponse};
pub use primitives::{
    page::{QueryOrder, DEFAULT_QUERY_LIMIT},
    refs::BlockRef,
    state::{BlockRecord, FamilyWindowRecord, LogId},
};
pub use store::{InMemoryBlobStore, InMemoryMetaStore};

pub type Topic = B256;
