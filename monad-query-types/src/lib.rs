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

//! Shared record + ingest types for the queryX (chain-data) crate family: the
//! ingest block representation, external-payload locators, and the per-family
//! stored-row / query-entry types. Depends only on `monad-query-primitives`
//! and `monad-query-errors`.

pub mod external;
pub mod ingest_types;
pub mod logs;
pub mod traces;
pub mod transfers;
pub mod txs;

pub use external::{ExternalFamilyRegion, ExternalPayloadSpec};
pub use ingest_types::{FinalizedBlock, IngestTrace, IngestTx};
pub use logs::{LogEntry, StoredLog};
pub use traces::{StoredTrace, TraceEntry};
pub use transfers::TransferEntry;
pub use txs::{StoredTxEnvelope, TxEntry, TxLocation};
