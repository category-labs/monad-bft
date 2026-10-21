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

//! The pq node with every component in its own task. Same runtime as
//! `PqNode`; only where the components run differs.

use std::future::pending;

use monad_pq_chorus::{Evidence, PqMessage};
use tokio::task::JoinSet;

use crate::{
    chorus::types::Slot,
    clock::Clock,
    component::{
        Dispatch, Link, PqCadenceInput, PqCadenceOutput, PqDAInput, ProposingInput,
        ProposingOutput, RepeaterInput, RepeaterOutput, spawn,
    },
    da::DAOutput,
    finalization::PqFinalizedSlot,
    network::NetworkHandle,
    node::PqNode,
    runtime::{PqEffect, PqNodeRuntime, PqRuntime},
};

type FinalizationLogger = Box<dyn FnMut(PqFinalizedSlot) + Send>;
type EvidenceLogger = Box<dyn FnMut(Slot, Evidence) + Send>;

pub struct PqAsyncNode<R = PqNodeRuntime> {
    runtime: R,
    clock: Clock,
    links: Links,

    #[expect(unused)] // abort on Drop only
    tasks: JoinSet<()>,
}

// the far ends of the component tasks, the transport and the sinks
struct Links {
    cadence: Link<PqCadenceInput, PqCadenceOutput>,
    da: Link<PqDAInput, DAOutput>,
    proposing: Link<ProposingInput, ProposingOutput>,
    repeater: Link<RepeaterInput<PqMessage>, RepeaterOutput<PqMessage>>,

    network: Option<NetworkHandle>,
    finalization: Option<FinalizationLogger>,
    evidence: Option<EvidenceLogger>,
}

impl<R: PqRuntime> PqAsyncNode<R> {
    // lifts each component of a node into its own task
    pub fn spawn(node: PqNode<R>, clock: Clock) -> Self {
        let PqNode {
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
            repeater: spawn(repeater, clock, &mut tasks),
            network: None,
            finalization: None,
            evidence: None,
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

    pub fn on_finalization(&mut self, logger: impl FnMut(PqFinalizedSlot) + Send + 'static) {
        self.links.finalization = Some(Box::new(logger));
    }

    pub fn on_evidence(&mut self, logger: impl FnMut(Slot, Evidence) + Send + 'static) {
        self.links.evidence = Some(Box::new(logger));
    }

    pub async fn run(mut self) {
        let links = &mut self.links;
        let runtime = &mut self.runtime;
        loop {
            tokio::select! {
                Some(inbound) = recv(&mut links.network) => {
                    runtime.handle_inbound(self.clock.now(), inbound, links);
                }
                Some(output) = links.cadence.recv() => {
                    runtime.handle_cadence(output, links);
                }
                Some(output) = links.da.recv() => {
                    runtime.handle_da(self.clock.now(), output, links);
                }
                Some(output) = links.proposing.recv() => {
                    runtime.handle_proposal(self.clock.now(), output, links);
                }
                Some(output) = links.repeater.recv() => {
                    runtime.handle_repeat(output, links);
                }
                else => return,
            }
        }
    }
}

impl Dispatch<PqEffect> for Links {
    fn dispatch(&mut self, effect: PqEffect) {
        match effect {
            PqEffect::Cadence(input) => self.cadence.send(input),
            PqEffect::DA(input) => self.da.send(input),
            PqEffect::Proposing(input) => self.proposing.send(input),
            PqEffect::Repeater(input) => self.repeater.send(input),

            // optional sinks; an absent one drops the effect
            PqEffect::Network(outbound) => {
                if let Some(network) = &mut self.network {
                    network.send(outbound);
                }
            }
            PqEffect::Ledger(finalized) => {
                if let Some(logger) = &mut self.finalization {
                    logger(finalized);
                }
            }
            PqEffect::Evidence(slot, evidence) => {
                if let Some(logger) = &mut self.evidence {
                    logger(slot, evidence);
                }
            }
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
