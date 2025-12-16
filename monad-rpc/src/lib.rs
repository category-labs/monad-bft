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

pub mod chainstate;
pub mod comparator;
pub mod config;
pub mod docs;
pub mod eth_json_types;
pub mod event;
pub mod handlers;
pub mod hex;
pub mod jsonrpc;
pub mod metrics;
pub mod serialize;
pub mod timing;
pub mod txpool;
pub mod vpool;
pub mod websocket;

pub const MONAD_RPC_VERSION: Option<&str> = option_env!("MONAD_VERSION");
