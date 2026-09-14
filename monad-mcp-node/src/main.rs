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

use monad_mcp_node::{RunError, config::NodeConfig, init_logging, run_node};

#[tokio::main]
async fn main() -> Result<(), RunError> {
    init_logging();

    let path = std::env::args()
        .nth(1)
        .ok_or("usage: monad-mcp-node <config.toml>")?;
    let text = std::fs::read_to_string(&path)?;
    let config: NodeConfig = toml::from_str(&text)?;

    tokio::select! {
        result = run_node(config) => result,
        _ = tokio::signal::ctrl_c() => Ok(()),
    }
}
