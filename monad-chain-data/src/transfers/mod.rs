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

//! View over the trace family exposing value-bearing transfers. No
//! separate persistence: the materializer ANDs a `has_transfer` clause
//! into trace queries and projects matches into `TransferEntry`s.

mod materialize;
mod types;

pub(crate) use materialize::TransferMaterializer;
pub use materialize::{
    QueryTransfersRequest, QueryTransfersResponse, TransferFilter, TransfersRelations,
};
pub use types::TransferEntry;
