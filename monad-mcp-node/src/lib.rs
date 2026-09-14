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

mod cadence_task;
pub mod config;
mod da_task;
mod epoch;
mod finalization;
mod logging;
mod network;
mod node;
mod proposing_task;

use std::error::Error;

pub use monad_mcp_chorus::stub as chorus;
pub use monad_mcp_da::stub as da;
use tracing::Instrument as _;

pub use self::logging::init_logging;
use self::{
    chorus::slot::chorus::FinalizationPath,
    config::NodeConfig,
    finalization::FinalizedSlot,
    network::{NetworkHandle, stub::UdpNetwork},
    node::Node,
};

pub type RunError = Box<dyn Error + Send + Sync>;

// a node over the UDP stub, until its loop ends. Every log line
// carries the node's id.
pub async fn run_node(config: NodeConfig) -> Result<(), RunError> {
    let span = tracing::info_span!("node", id = u64::from(config.node_id));
    run(config).instrument(span).await
}

async fn run(config: NodeConfig) -> Result<(), RunError> {
    let mut node = Node::new(&config)?;
    let (handle, transport) = NetworkHandle::pair();
    node.set_network(handle);
    node.on_finalization(log_finalized);

    let udp = UdpNetwork::bind(config.node_id, config.network.port, config.peers()).await?;
    let _transport_task = udp.spawn(transport);

    node.run().await;
    Ok(())
}

// todo: the ledger / execution boundary. The block reads one char per
// proposal index, + committed or - not, green on the fast path and
// yellow on the fallback path.
fn log_finalized(finalized: FinalizedSlot) {
    let FinalizedSlot {
        slot,
        at,
        finalization,
        proposals,
    } = finalized;
    let mut shape = String::new();
    for ((j, root), proposal) in finalization.roots().into_indexed_iter().zip(proposals) {
        shape.push(if root.is_some() { '+' } else { '-' });
        let Some(message) = proposal else {
            continue;
        };
        tracing::debug!(slot = slot.0, j, ?root, len = message.len(), "committed");
    }
    let color = match finalization.path() {
        FinalizationPath::Fast => logging::GREEN,
        FinalizationPath::Fallback => logging::YELLOW,
    };
    let block = logging::paint(color, &shape);
    tracing::info!(slot = slot.0, block = %block, at = at.as_nanos(), "finalized");
}
