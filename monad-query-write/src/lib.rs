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

//! queryX ingest (write path): the branchless single-writer ingest engine that
//! indexes finalized blocks into the engine's tables, plus the
//! `ChainDataIngestSource` seam that block sources implement.
//!
//! Lower crates are re-exported under their historical module paths so the
//! ingest modules keep their `crate::{engine, store, ...}` imports; these are
//! transition shims to be flattened to direct `monad_query_*::` paths.

use monad_query_errors as error;

pub mod store {
    pub use monad_query_engine::{SessionFuture, WriteSession};
    pub use monad_query_store::*;
}

pub mod primitives {
    pub use monad_query_primitives::*;
}

pub mod ingest_types {
    pub use monad_query_primitives::{CallKind, Hash32};
    pub use monad_query_types::ingest_types::*;
}

pub mod engine {
    pub use monad_query_engine::{
        bitmap, clause, digest, family, primary_dir, query, row_codec, seal, tables,
    };
}

pub mod external {
    pub use monad_query_types::external::*;
}

pub mod ingest;
mod logs;
mod traces;
mod txs;
