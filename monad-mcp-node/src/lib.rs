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

mod async_node;
mod clock;
pub mod component;
pub mod config;
mod epoch;
mod finalization;
pub mod ledger;
mod logging;
pub mod network;
mod node;
mod runtime;

use std::error::Error;

pub use monad_mcp_chorus::stub as chorus;
pub use monad_mcp_da::stub as da;
use tokio::task::JoinSet;
use tracing::Instrument as _;

use self::{
    async_node::AsyncNode,
    clock::Clock,
    config::NodeConfig,
    ledger::{LedgerSink, Recorder},
    network::{NetworkHandle, stub::UdpNetwork},
};
pub use self::{
    component::{Component, Dispatch},
    epoch::{EpochHandle, NodeProposerSchedule},
    finalization::FinalizedSlot,
    logging::init_logging,
    node::{Node, NodeOutput},
    runtime::{Effect, NodeRuntime, Runtime},
};

pub type RunError = Box<dyn Error + Send + Sync>;

// a node over the UDP stub, until its loop ends. Every log line
// carries the node's id. Dropping the future stops the node and its
// transport.
pub async fn run_node(config: NodeConfig) -> Result<(), RunError> {
    let span = tracing::info_span!("node", id = u64::from(config.node_id));
    run(config).instrument(span).await
}

async fn run(config: NodeConfig) -> Result<(), RunError> {
    let node = Node::new(&config)?;
    let proposers = node.runtime().epoch_handle().proposers.clone();
    let sink = LedgerSink::spawn(&config.ledger.dir)?;

    let mut node = AsyncNode::spawn(node, Clock::start());
    let (handle, transport) = NetworkHandle::pair();
    node.set_network(handle);
    let mut recorder = Recorder::new(proposers, sink);
    node.on_finalization(move |finalized| recorder.record(finalized));

    let mut tasks = JoinSet::new();
    let udp = UdpNetwork::bind(config.node_id, config.network.port, config.peers()).await?;
    tasks.spawn(udp.run(transport).in_current_span());

    node.run().await;
    Ok(())
}
