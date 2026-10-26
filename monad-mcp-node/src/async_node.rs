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
    chorus::slot::chorus::ChorusMessage,
    clock::Clock,
    component::{
        CadenceInput, CadenceOutput, DAInput, FinalizationInput, Link, ProposingInput,
        ProposingOutput, RepeaterInput, RepeaterOutput, spawn,
    },
    da::DAOutput,
    finalization::FinalizedSlot,
    network::NetworkHandle,
    node::{Node, Parts},
    router::{Effect, NodeRouter, Routed, Router, Sink},
};

type FinalizationLogger = Box<dyn FnMut(FinalizedSlot) + Send>;

type RepeaterLink = Link<RepeaterInput<ChorusMessage>, RepeaterOutput<ChorusMessage>>;

pub struct AsyncNode<R = NodeRouter> {
    router: R,
    clock: Clock,
    tasks: Tasks,
}

// the far ends of the component tasks, the transport and the ledger
struct Tasks {
    cadence: Link<CadenceInput, CadenceOutput>,
    da: Link<DAInput, DAOutput>,
    collector: Link<FinalizationInput, FinalizedSlot>,
    proposing: Link<ProposingInput, ProposingOutput>,

    repeater: Option<RepeaterLink>,
    network: Option<NetworkHandle>,
    finalization: Option<FinalizationLogger>,

    #[expect(unused)] // abort on Drop only
    join_set: JoinSet<()>,
}

impl<R: Router> AsyncNode<R> {
    pub fn spawn(node: Node<R>, clock: Clock) -> Self {
        let Node { router, parts, .. } = node;
        let tasks = Tasks::spawn(parts, clock);
        Self {
            router,
            clock,
            tasks,
        }
    }

    pub fn set_network(&mut self, network: NetworkHandle) {
        self.tasks.network = Some(network);
    }

    pub fn on_finalization(&mut self, logger: impl FnMut(FinalizedSlot) + Send + 'static) {
        self.tasks.finalization = Some(Box::new(logger));
    }

    pub async fn run(mut self) {
        let tasks = &mut self.tasks;
        let router = &mut self.router;
        loop {
            let effect = tokio::select! {
                Some(inbound) = recv(&mut tasks.network) => Effect::Network(inbound),
                Some(output) = tasks.cadence.recv() => Effect::Cadence(output),
                Some(output) = tasks.da.recv() => Effect::DA(output),
                Some(finalized) = tasks.collector.recv() => Effect::Finalization(finalized),
                Some(output) = tasks.proposing.recv() => Effect::Proposing(output),
                Some(output) = recv(&mut tasks.repeater) => Effect::Repeater(output),
                else => return,
            };
            router.route(self.clock.now(), effect, tasks);
        }
    }
}

impl Tasks {
    // lifts each component of a node into its own task
    fn spawn(parts: Parts, clock: Clock) -> Self {
        let Parts {
            cadence,
            da,
            collector,
            proposing,
            repeater,
        } = parts;

        let mut join_set = JoinSet::new();
        Self {
            cadence: spawn(cadence, clock, &mut join_set),
            da: spawn(da, clock, &mut join_set),
            collector: spawn(collector, clock, &mut join_set),
            proposing: spawn(proposing, clock, &mut join_set),
            repeater: repeater.map(|repeater| spawn(repeater, clock, &mut join_set)),
            network: None,
            finalization: None,
            join_set,
        }
    }
}

impl Sink<Routed> for Tasks {
    fn send(&mut self, routed: Routed) {
        match routed {
            // required components
            Routed::Cadence(input) => self.cadence.send(input),
            Routed::DA(input) => self.da.send(input),
            Routed::Finalization(input) => self.collector.send(input),
            Routed::Proposing(input) => self.proposing.send(input),

            // optional components
            Routed::Repeater(input) if let Some(repeater) = &mut self.repeater => {
                repeater.send(input);
            }
            Routed::Network(outbound) if let Some(network) = &mut self.network => {
                network.send(outbound)
            }
            Routed::Ledger(finalized) if let Some(logger) = &mut self.finalization => {
                logger(finalized);
            }
            // component not present, dropped
            Routed::Repeater(_) => {}
            Routed::Network(_) => {}
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
