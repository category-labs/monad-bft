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

use monad_rpc_docs::rpc;

use crate::{jsonrpc::JsonRpcResult, WEB3_RPC_CLIENT_VERSION};

#[rpc(method = "net_version", ignore = "chain_id")]
pub fn monad_net_version(chain_id: u64) -> JsonRpcResult<String> {
    Ok(chain_id.to_string())
}

#[rpc(method = "web3_clientVersion")]
pub fn monad_web3_client_version() -> JsonRpcResult<String> {
    Ok(WEB3_RPC_CLIENT_VERSION.to_string())
}
