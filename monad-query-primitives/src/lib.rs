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

//! Foundational value types for the queryX (chain-data) crate family: scalar
//! aliases, the query envelope/limits/order, the on-disk record headers and
//! refs, and the external-payload reader trait.

mod call_kind;
mod external_reader;
pub mod limits;
pub mod order;
pub mod records;
pub mod refs;

pub use alloy_consensus::Header as EvmBlockHeader;
pub use call_kind::CallKind;
pub use external_reader::{ExternalBlobReader, InMemoryExternalBlobReader, RawBytes};

/// 32-byte hash: block hash, tx hash, log topic, content digest.
pub type Hash32 = alloy_primitives::B256;
