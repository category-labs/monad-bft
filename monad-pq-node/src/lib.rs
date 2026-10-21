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

//! The Quantum Cadence validator: Quantum Chorus slots in the unchanged
//! cadence runtime, the pq DA, and monad-mcp-node's network, proposing,
//! repeater, mempool and ledger sink.

mod async_node;
mod clock;
pub mod component;
pub mod config;
pub mod evidence;
pub mod finalization;
pub mod ledger;
mod node;
pub mod runtime;
pub mod wire;

pub use monad_mcp_chorus::stub as chorus;
pub use monad_mcp_node::{RunError, init_logging, network};
pub use monad_pq_da::stub as da;
use tokio::task::JoinSet;
use tracing::Instrument as _;

use self::{
    async_node::PqAsyncNode,
    clock::Clock,
    config::PqNodeConfig,
    evidence::EvidenceSink,
    ledger::PqRecorder,
    network::{NetworkHandle, stub::UdpNetwork},
};
pub use self::{
    finalization::PqFinalizedSlot,
    node::{PqNode, PqNodeOutput},
    runtime::{PqEffect, PqNodeRuntime, PqRuntime},
};

// a pq node over the UDP stub, until its loop ends. Every log line
// carries the node's id. Dropping the future stops the node and its
// transport.
pub async fn run_node(config: PqNodeConfig) -> Result<(), RunError> {
    let span = tracing::info_span!("node", id = u64::from(config.base.node_id));
    run(config).instrument(span).await
}

async fn run(config: PqNodeConfig) -> Result<(), RunError> {
    let node = PqNode::new(&config)?;
    let proposers = node.runtime().epoch_handle().proposers.clone();
    let sink = monad_mcp_node::ledger::LedgerSink::spawn(&config.base.ledger.dir)?;
    let retention_slots = config.quantum.evidence_retention_slots;
    let evidence = config
        .evidence_dir()
        .map(|dir| EvidenceSink::spawn(dir, config.base.node_id, retention_slots))
        .transpose()?;

    let mut node = PqAsyncNode::spawn(node, Clock::start());
    let (handle, transport) = NetworkHandle::pair();
    node.set_network(handle);
    let mut recorder = PqRecorder::new(proposers, sink, config.quantum.ledger_proof);
    node.on_finalization(move |finalized| recorder.record(finalized));
    if let Some(mut evidence) = evidence {
        node.on_evidence(move |slot, found| evidence.record(slot, found));
    }

    let mut tasks = JoinSet::new();
    let base = &config.base;
    let udp = UdpNetwork::bind(base.node_id, base.network.port, base.peers()).await?;
    tasks.spawn(udp.run(transport).in_current_span());

    node.run().await;
    Ok(())
}
