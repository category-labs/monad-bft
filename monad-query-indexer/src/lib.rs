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

//! Bridges monad-archive and the queryX chain-data engine: an ingest source
//! over archive block data, external-payload readers for archive-format
//! storage, and the standalone ingest binary.

pub mod archive_range_read;
pub mod chain_data_external;
pub mod chain_data_ingest;
pub mod chain_data_source;
pub(crate) mod chunked_range;
pub mod metrics;
