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

use bytes::Bytes;

use crate::engine::bitmap::sharded_stream_id;

/// One AND-term of an indexed query. `kind` names the indexed field
/// (e.g. `"addr"`, `"topic0"`, `"from"`); `values` are the accepted
/// byte values for that field, combined with OR semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedClause {
    pub kind: &'static str,
    pub values: Vec<Bytes>,
}

impl IndexedClause {
    pub fn stream_ids_for_shard(&self, shard: u64) -> Vec<String> {
        self.values
            .iter()
            .map(|value| sharded_stream_id(self.kind, value, shard))
            .collect()
    }
}

/// Per-domain indexed query contract. Implementers translate filters
/// that are fully represented by indexed clauses into engine clauses.
/// `matches` is an invariant check for materialized records from those
/// clauses, not a release-mode post-filter.
pub trait IndexedFilter {
    type Record;

    fn indexed_clauses(&self) -> Vec<IndexedClause>;
    fn matches(&self, record: &Self::Record) -> bool;
}
