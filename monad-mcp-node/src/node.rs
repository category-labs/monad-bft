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

//! The node with every component stepped in place: deterministic,
//! itself a component, for simulation and tests. `AsyncNode` runs the
//! same router with the components in tasks.

use std::collections::VecDeque;

use monad_mcp_common::Timestamp;

use crate::{
    RunError,
    cadence::{SlotManager, conductor::CadenceConductor},
    component::{
        Cadence, Component, DA, Mempool, MempoolProposing, Proposing, ProposingComponent as _,
        RandomProposing,
    },
    config::{NodeConfig, SourceKind},
    da::DARuntime,
    finalization::{FinalizationCollector, FinalizedSlot},
    network::{Network, NodeAppMessage, Wire, WireInbound, WireOutbound},
    router::{Effect, NodeRouter, Routed, Router, Sink},
};

pub enum NodeOutput<W = NodeAppMessage> {
    Transport(WireOutbound<W>),
    Ledger(FinalizedSlot),
}

// W is the form of a message on the wire. With the default, the node's
// own message type, the node does no ser/de.
pub struct Node<R = NodeRouter, W = NodeAppMessage> {
    pub(crate) router: R,
    pub(crate) parts: Parts<W>,
    outbox: VecDeque<NodeOutput<W>>,
}

pub(crate) struct Parts<W> {
    // components
    pub(crate) cadence: Cadence,
    pub(crate) da: DA,
    pub(crate) collector: FinalizationCollector,
    pub(crate) proposing: Proposing,
    pub(crate) network: Network<NodeAppMessage, W>,
}

impl<W> Node<NodeRouter, W> {
    pub fn new(config: &NodeConfig) -> Result<Self, RunError> {
        config.validate()?;
        let epoch_handle = config.epoch_handle()?;

        let slot_config = config.cadence.chorus();
        let slot_manager = SlotManager::new(slot_config, epoch_handle.chorus());
        let conductor_config = config.cadence.conductor(config.genesis_deadline)?;
        let conductor = CadenceConductor::genesis(conductor_config, ())?;
        let cadence = Cadence::new(slot_manager, conductor);

        let da = DARuntime::new(
            config.da.runtime(config.cadence.delta),
            epoch_handle.da(),
            epoch_handle.proposers.clone(),
        );

        let proposing: Proposing = match config.proposal.source {
            SourceKind::Random | SourceKind::Fixed => {
                let proposing = RandomProposing::new(&epoch_handle, &config.proposal, config.rng());
                Box::new(proposing)
            }
            SourceKind::Mempool => {
                let mempool = Mempool::new(config.mempool);
                let proposing = MempoolProposing::new(&epoch_handle, &config.proposal, mempool);
                Box::new(proposing)
            }
        };
        let observe_votes = proposing.wants_vote_facts();
        let network = Network::new(&epoch_handle, config.repeater());
        let collector = FinalizationCollector::default();

        let parts = Parts {
            cadence,
            da,
            collector,
            proposing,
            network,
        };
        Ok(Self {
            router: NodeRouter::new(epoch_handle, config).observe_votes(observe_votes),
            parts,
            outbox: VecDeque::new(),
        })
    }

    // the same node with a wrapped or replaced proposer, for scenarios; the
    // router copies batch votes to it if it wants them
    pub fn map_proposing(mut self, wrap: impl FnOnce(Proposing) -> Proposing) -> Self {
        self.parts.proposing = wrap(self.parts.proposing);
        let observe_votes = self.parts.proposing.wants_vote_facts();
        self.router = self.router.observe_votes(observe_votes);
        self
    }

    // the same components under a wrapped router, for scenarios
    pub fn map_router<R>(self, wrap: impl FnOnce(NodeRouter) -> R) -> Node<R, W> {
        Node {
            router: wrap(self.router),
            parts: self.parts,
            outbox: self.outbox,
        }
    }
}

impl<R, W> Node<R, W> {
    pub fn router(&self) -> &R {
        &self.router
    }
}

impl<R: Router<W>, W: Wire<NodeAppMessage>> Node<R, W> {
    fn route(&mut self, now: Timestamp, effect: Effect<W>) {
        let mut sink = NodeSink {
            parts: &mut self.parts,
            outbox: &mut self.outbox,
            now,
        };
        self.router.route(now, effect, &mut sink);
    }

    // wire every component's outputs onward until nothing moves
    fn step(&mut self, now: Timestamp) {
        while let Some(effect) = self.parts.poll() {
            self.route(now, effect);
        }
    }
}

impl<R: Router<W>, W: Wire<NodeAppMessage>> Component for Node<R, W> {
    type Input = WireInbound<W>;
    type Output = NodeOutput<W>;

    fn handle(&mut self, now: Timestamp, inbound: WireInbound<W>) {
        self.route(now, Effect::Transport(inbound));
        self.step(now);
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.parts.next_due()
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.parts.handle_due(now);
        self.step(now);
    }

    fn poll(&mut self) -> Option<NodeOutput<W>> {
        self.outbox.pop_front()
    }
}

// Parts is the parallel-composition of its components into a larger
// Component. However, it does not implement Component because it can
// produce/handle external effects (transport and ledger).
impl<W: Wire<NodeAppMessage>> Parts<W> {
    fn poll(&mut self) -> Option<Effect<W>> {
        if let Some(output) = self.network.poll() {
            return Some(Effect::Network(output));
        }
        if let Some(output) = self.cadence.poll() {
            return Some(Effect::Cadence(output));
        }
        if let Some(output) = self.da.poll() {
            return Some(Effect::DA(output));
        }
        if let Some(output) = self.collector.poll() {
            return Some(Effect::Finalization(output));
        }
        if let Some(output) = self.proposing.poll() {
            return Some(Effect::Proposing(output));
        }
        None
    }

    fn next_due(&self) -> Option<Timestamp> {
        [
            self.cadence.next_due(),
            self.da.next_due(),
            self.proposing.next_due(),
            self.network.next_due(),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn handle_due(&mut self, now: Timestamp) {
        handle_if_due(&mut self.cadence, now);
        handle_if_due(&mut self.da, now);
        handle_if_due(&mut self.proposing, now);
        handle_if_due(&mut self.network, now);
    }
}

struct NodeSink<'a, W> {
    parts: &'a mut Parts<W>,
    outbox: &'a mut VecDeque<NodeOutput<W>>,
    now: Timestamp,
}

impl<W: Wire<NodeAppMessage>> Sink<Routed<W>> for NodeSink<'_, W> {
    fn send(&mut self, routed: Routed<W>) {
        let now = self.now;
        let parts = &mut *self.parts;
        match routed {
            Routed::Cadence(input) => parts.cadence.handle(now, input),
            Routed::DA(input) => parts.da.handle(now, input),
            Routed::Finalization(input) => parts.collector.handle(now, input),
            Routed::Proposing(input) => parts.proposing.handle(now, input),
            Routed::Network(input) => parts.network.handle(now, input),
            Routed::Transport(outbound) => {
                self.outbox.push_back(NodeOutput::Transport(outbound));
            }
            Routed::Ledger(finalized) => self.outbox.push_back(NodeOutput::Ledger(finalized)),
        }
    }
}

fn handle_if_due<C: Component>(component: &mut C, now: Timestamp) {
    if component.next_due().is_some_and(|due| due <= now) {
        component.handle_due(now);
    }
}
