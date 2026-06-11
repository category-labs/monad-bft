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

mod hash_index;
mod ingest;
mod materialize;
mod types;

pub use hash_index::TxHashIndexTable;
pub(crate) use ingest::{collect_hash_locations, encode_block_txs, stream_entries_for_tx};
pub(crate) use materialize::{load_txs_by_positions, TxMaterializer};
pub use materialize::{
    QueryTransactionsRequest, QueryTransactionsResponse, TxFilter, TxsRelations,
};
pub(crate) use types::TxLocation;
pub use types::{StoredTxEnvelope, TxEntry};
