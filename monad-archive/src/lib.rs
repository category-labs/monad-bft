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

#![allow(async_fn_in_trait, clippy::too_many_arguments)]

pub mod archive_reader;
pub mod cli;
pub mod error;
pub mod failover_circuit_breaker;
pub mod kvstore;
pub mod metrics;
pub mod model;
pub mod prelude;
pub mod rlp_offset_scanner;

// not excluded via cfg(test) to enable import by binaries
pub mod test_utils;
