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

//! The node with every component in its own task. Same runtime as
//! `Node`; only where the components run differs.

use std::future::pending;

use tokio::task::JoinSet;

use crate::{
    chorus::slot::chorus::ChorusMessage,
    clock::Clock,
    component::{
        CadenceInput, CadenceOutput, DAInput, Dispatch, Link, ProposingInput, ProposingOutput,
        RepeaterInput, RepeaterOutput, spawn,
    },
    da::DAOutput,
    finalization::FinalizedSlot,
    network::NetworkHandle,
    node::Node,
    runtime::{Effect, NodeRuntime, Runtime},
};

// todo: the ledger / execution boundary
type FinalizationLogger = Box<dyn FnMut(FinalizedSlot) + Send>;

type RepeaterLink = Link<RepeaterInput<ChorusMessage>, RepeaterOutput<ChorusMessage>>;

pub struct AsyncNode<R = NodeRuntime> {
    runtime: R,
    clock: Clock,
    links: Links,

    #[expect(unused)] // abort on Drop only
    tasks: JoinSet<()>,
}

// the far ends of the component tasks, the transport and the ledger
struct Links {
    cadence: Link<CadenceInput, CadenceOutput>,
    da: Link<DAInput, DAOutput>,
    proposing: Link<ProposingInput, ProposingOutput>,

    repeater: Option<RepeaterLink>,
    network: Option<NetworkHandle>,
    finalization: Option<FinalizationLogger>,
}

impl<R: Runtime> AsyncNode<R> {
    // lifts each component of a node into its own task
    pub fn spawn(node: Node<R>, clock: Clock) -> Self {
        let Node {
            runtime,
            cadence,
            da,
            proposing,
            repeater,
            ..
        } = node;

        let mut tasks = JoinSet::new();
        let links = Links {
            cadence: spawn(cadence, clock, &mut tasks),
            da: spawn(da, clock, &mut tasks),
            proposing: spawn(proposing, clock, &mut tasks),
            repeater: repeater.map(|repeater| spawn(repeater, clock, &mut tasks)),
            network: None,
            finalization: None,
        };
        Self {
            runtime,
            clock,
            links,
            tasks,
        }
    }

    pub fn set_network(&mut self, network: NetworkHandle) {
        self.links.network = Some(network);
    }

    pub fn on_finalization(&mut self, logger: impl FnMut(FinalizedSlot) + Send + 'static) {
        self.links.finalization = Some(Box::new(logger));
    }

    pub async fn run(mut self) {
        let links = &mut self.links;
        let runtime = &mut self.runtime;
        loop {
            tokio::select! {
                Some(inbound) = recv(&mut links.network) => {
                    runtime.handle_inbound(inbound, links);
                }
                Some(output) = links.cadence.recv() => {
                    runtime.handle_cadence(output, links);
                }
                Some(output) = links.da.recv() => {
                    runtime.handle_da(output, links);
                }
                Some(output) = links.proposing.recv() => {
                    runtime.handle_proposal(self.clock.now(), output, links);
                }
                Some(output) = recv(&mut links.repeater) => {
                    runtime.handle_repeat(output, links);
                }
                else => return,
            }
        }
    }
}

impl Dispatch<Effect> for Links {
    fn dispatch(&mut self, effect: Effect) {
        match effect {
            // required components
            Effect::Cadence(input) => self.cadence.send(input),
            Effect::DA(input) => self.da.send(input),
            Effect::Proposing(input) => self.proposing.send(input),

            // optional components
            Effect::Repeater(input) if let Some(repeater) = &mut self.repeater => {
                repeater.send(input);
            }
            Effect::Network(outbound) if let Some(network) = &mut self.network => {
                network.send(outbound)
            }
            Effect::Ledger(finalized) if let Some(logger) = &mut self.finalization => {
                logger(finalized);
            }
            // component not present, effect dropped
            Effect::Repeater(_) => {}
            Effect::Network(_) => {}
            Effect::Ledger(_) => {}
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
