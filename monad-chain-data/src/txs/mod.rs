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

mod ingest;
mod materialize;

pub use monad_query_engine::txs::TxHashIndexTable;
pub(crate) use ingest::{
    collect_hash_locations, digest_block_txs, encode_block_txs, stream_entries_for_tx,
};
pub(crate) use materialize::{load_txs_by_positions, TxMaterializer};
pub use materialize::{
    QueryTransactionsRequest, QueryTransactionsResponse, TxFilter, TxsRelations,
};
pub(crate) use monad_query_types::txs::TxLocation;
pub use monad_query_types::txs::{StoredTxEnvelope, TxEntry};
