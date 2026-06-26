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

//! queryX storage + query engine: row codec, page-relative bitmaps, primary-id
//! directories, the `Tables` aggregate, and the indexed query pipeline. Layered
//! on `monad-query-store` (backends), `monad-query-types`, and
//! `monad-query-primitives`.

// The lower crates are re-exported under the historical module paths
// (`crate::{error, store, primitives, ingest_types, external, txs, ...}`) so the
// engine's modules keep their existing `use crate::...` imports. The `store`
// facade also folds in the engine-owned `WriteSession`/`SessionFuture`, which
// live here because they borrow `Tables`. Transition shim: these can be inlined
// to direct `monad_query_*::` paths later.
use monad_query_errors as error;

pub mod store {
    pub use monad_query_store::*;

    pub use crate::session::{SessionFuture, WriteSession};
}

pub mod primitives {
    pub use monad_query_primitives::*;

    pub mod range;
}

pub mod ingest_types {
    pub use monad_query_primitives::{CallKind, Hash32};
    pub use monad_query_types::ingest_types::*;
}

pub mod external {
    pub use monad_query_primitives::{ExternalBlobReader, InMemoryExternalBlobReader, RawBytes};
    pub use monad_query_types::external::*;
}

pub mod logs {
    pub use monad_query_types::logs::*;
}

pub mod traces {
    pub use monad_query_types::traces::*;
}

pub mod transfers {
    pub use monad_query_types::transfers::*;
}

pub mod txs {
    pub use monad_query_types::txs::*;

    mod hash_index;
    pub use hash_index::TxHashIndexTable;
}

pub mod bitmap;
pub mod clause;
pub mod digest;
pub mod family;
pub mod primary_dir;
pub mod query;
pub mod row_codec;
pub mod seal;
pub mod session;
pub mod tables;

// Self-facade: engine modules were previously reached as `crate::engine::*`
// (when this was a submodule of monad-chain-data). Keep that path resolving
// without rewriting every `use`. Transition shim.
pub(crate) mod engine {
    pub(crate) use crate::{bitmap, clause, digest, family, primary_dir, query, row_codec, tables};
}

pub use session::{SessionFuture, WriteSession};
