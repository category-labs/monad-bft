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

//! The node with every component in its own task. Same router as
//! `Node`; only where the components run differs.

use std::future::pending;

use tokio::task::JoinSet;

use crate::{
    clock::Clock,
    component::{
        CadenceInput, CadenceOutput, DAInput, FinalizationInput, Link, ProposingInput,
        ProposingOutput, spawn,
    },
    da::DAOutput,
    finalization::FinalizedSlot,
    network::{NetworkInput, NetworkOutput, NodeAppMessage, Wire, WireAppMessage},
    node::{Node, Parts},
    router::{Effect, NodeRouter, Routed, Router, Sink},
    transport::TransportHandle,
};

type FinalizationLogger = Box<dyn FnMut(FinalizedSlot) + Send>;

type NetworkLink<W> = Link<NetworkInput<NodeAppMessage, W>, NetworkOutput<NodeAppMessage, W>>;

pub struct AsyncNode<R = NodeRouter, W = WireAppMessage> {
    router: R,
    clock: Clock,
    tasks: Tasks<W>,
}

// the far ends of the component tasks, the transport and the ledger
struct Tasks<W> {
    cadence: Link<CadenceInput, CadenceOutput>,
    da: Link<DAInput, DAOutput>,
    collector: Link<FinalizationInput, FinalizedSlot>,
    proposing: Link<ProposingInput, ProposingOutput>,
    network: NetworkLink<W>,

    transport: Option<TransportHandle<W>>,
    finalization: Option<FinalizationLogger>,

    #[expect(unused)] // abort on Drop only
    join_set: JoinSet<()>,
}

impl<R, W> AsyncNode<R, W>
where
    R: Router<W>,
    W: Wire<NodeAppMessage> + Send + 'static,
{
    pub fn spawn(node: Node<R, W>, clock: Clock) -> Self {
        let Node { router, parts, .. } = node;
        let tasks = Tasks::spawn(parts, clock);
        Self {
            router,
            clock,
            tasks,
        }
    }

    pub fn set_transport(&mut self, transport: TransportHandle<W>) {
        self.tasks.transport = Some(transport);
    }

    pub fn on_finalization(&mut self, logger: impl FnMut(FinalizedSlot) + Send + 'static) {
        self.tasks.finalization = Some(Box::new(logger));
    }

    pub async fn run(mut self) {
        let tasks = &mut self.tasks;
        let router = &mut self.router;
        loop {
            let effect = tokio::select! {
                Some(inbound) = recv(&mut tasks.transport) => Effect::Transport(inbound),
                Some(output) = tasks.network.recv() => Effect::Network(output),
                Some(output) = tasks.cadence.recv() => Effect::Cadence(output),
                Some(output) = tasks.da.recv() => Effect::DA(output),
                Some(finalized) = tasks.collector.recv() => Effect::Finalization(finalized),
                Some(output) = tasks.proposing.recv() => Effect::Proposing(output),
                else => return,
            };
            router.route(self.clock.now(), effect, tasks);
        }
    }
}

impl<W> Tasks<W>
where
    W: Wire<NodeAppMessage> + Send + 'static,
{
    // lifts each component of a node into its own task
    fn spawn(parts: Parts<W>, clock: Clock) -> Self {
        let Parts {
            cadence,
            da,
            collector,
            proposing,
            network,
        } = parts;

        let mut join_set = JoinSet::new();
        Self {
            cadence: spawn(cadence, clock, &mut join_set),
            da: spawn(da, clock, &mut join_set),
            collector: spawn(collector, clock, &mut join_set),
            proposing: spawn(proposing, clock, &mut join_set),
            network: spawn(network, clock, &mut join_set),
            transport: None,
            finalization: None,
            join_set,
        }
    }
}

impl<W> Sink<Routed<W>> for Tasks<W> {
    fn send(&mut self, routed: Routed<W>) {
        match routed {
            Routed::Cadence(input) => self.cadence.send(input),
            Routed::DA(input) => self.da.send(input),
            Routed::Finalization(input) => self.collector.send(input),
            Routed::Proposing(input) => self.proposing.send(input),
            Routed::Network(input) => self.network.send(input),

            Routed::Transport(outbound) if let Some(transport) = &mut self.transport => {
                transport.send(outbound);
            }
            Routed::Ledger(finalized) if let Some(logger) = &mut self.finalization => {
                logger(finalized);
            }
            // no transport or no logger, dropped
            Routed::Transport(_) => {}
            Routed::Ledger(_) => {}
        }
    }
}

// pending forever while the link is absent
async fn recv<Out, In>(link: &mut Option<Link<Out, In>>) -> Option<In> {
    match link {
        Some(link) => link.recv().await,
        None => pending().await,
    }
}
