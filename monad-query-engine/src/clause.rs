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

use crate::bitmap::{render_stream_id, IndexKind};

/// AND-term of an indexed query combining values with OR semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedClause {
    pub kind: IndexKind,
    pub values: Vec<Bytes>,
}

impl IndexedClause {
    pub fn from_set<T: AsRef<[u8]>>(kind: IndexKind, values: &HashSet<T>) -> Self {
        Self {
            kind,
            values: values
                .iter()
                .map(|value| Bytes::copy_from_slice(value.as_ref()))
                .collect(),
        }
    }

    pub fn marker(kind: IndexKind) -> Self {
        Self {
            kind,
            values: vec![Bytes::new()],
        }
    }

    pub(crate) fn stream_ids(&self) -> Vec<String> {
        self.values
            .iter()
            .map(|value| render_stream_id(self.kind.as_str(), value))
            .collect()
    }
}

pub fn set_allows<T: Eq + Hash>(allowed: &Option<HashSet<T>>, actual: Option<&T>) -> bool {
    allowed
        .as_ref()
        .is_none_or(|set| actual.is_some_and(|value| set.contains(value)))
}

pub trait IndexedFilter {
    type Record;

    fn indexed_clauses(&self) -> Vec<IndexedClause>;
    fn matches(&self, record: &Self::Record) -> bool;

    fn has_indexed_clause(&self) -> bool {
        !self.indexed_clauses().is_empty()
    }
}
