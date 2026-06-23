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

use std::{collections::HashSet, hash::Hash};

use bytes::Bytes;

use crate::engine::bitmap::{render_stream_id, IndexKind};

/// One AND-term of an indexed query. `kind` names the indexed field through
/// the same [`IndexKind`] the ingest side writes streams under; `values` are
/// the accepted byte values for that field, combined with OR semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedClause {
    pub kind: IndexKind,
    pub values: Vec<Bytes>,
}

impl IndexedClause {
    /// Clause accepting any of the byte values in `values`.
    pub fn from_set<T: AsRef<[u8]>>(kind: IndexKind, values: &HashSet<T>) -> Self {
        Self {
            kind,
            values: values
                .iter()
                .map(|value| Bytes::copy_from_slice(value.as_ref()))
                .collect(),
        }
    }

    /// Presence-only clause (e.g. `top_level`, `has_transfer`): the indexed
    /// field carries no value bytes.
    pub fn marker(kind: IndexKind) -> Self {
        Self {
            kind,
            values: vec![Bytes::new()],
        }
    }

    /// The bitmap stream ids this clause's OR set expands to.
    pub fn stream_ids(&self) -> Vec<String> {
        self.values
            .iter()
            .map(|value| render_stream_id(self.kind.as_str(), value))
            .collect()
    }
}

/// "If the filter constrains this field, the (possibly absent) actual value
/// must be in the allowed set." Absent filter ⇒ allow; absent value ⇒ reject.
pub fn set_allows<T: Eq + Hash>(allowed: &Option<HashSet<T>>, actual: Option<&T>) -> bool {
    allowed
        .as_ref()
        .is_none_or(|set| actual.is_some_and(|value| set.contains(value)))
}

/// Per-domain indexed query contract: translates a fully-indexed filter into
/// engine clauses. `matches` is an invariant check on materialized records,
/// not a release-mode post-filter.
pub trait IndexedFilter {
    type Record;

    fn indexed_clauses(&self) -> Vec<IndexedClause>;
    fn matches(&self, record: &Self::Record) -> bool;

    /// Whether the indexed path can serve this query: true iff the filter
    /// emits at least one indexed clause. `false` routes the query to the
    /// block-scan path, where `matches` does all the filtering.
    fn has_indexed_clause(&self) -> bool {
        !self.indexed_clauses().is_empty()
    }
}
