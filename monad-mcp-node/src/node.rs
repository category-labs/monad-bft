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

use std::{collections::VecDeque, sync::Arc};

use monad_mcp_common::{Slot, Timestamp, spec::ValidatorData as _};

use crate::{
    RunError,
    cadence::{SlotManager, conductor::CadenceConductor, types::HeaderAuth},
    component::{Cadence, Component, DA, MempoolProposing, Proposing, RandomProposing},
    config::{NodeConfig, SourceKind},
    da::{DARuntime, validate_header},
    epoch::{EpochHandle, EpochManager},
    finalization::{FinalizationCollector, FinalizedSlot},
    network::{Network, NodeAppMessage, Wire, WireInbound, WireOutbound},
    proposing::Mempool,
    router::{Effect, NodeRouter, Routed, Router, Sink},
};

pub enum NodeOutput<W = NodeAppMessage> {
    Transport(WireOutbound<W>),
    Ledger(FinalizedSlot),
}

// W is the form of a message on the wire. With the default, the node's
// own message type, the node does no ser/de.
pub struct Node<R = NodeRouter, W = NodeAppMessage> {
    epoch_handle: EpochHandle,
    pub(crate) router: R,
    pub(crate) parts: Parts<W>,
    outbox: VecDeque<NodeOutput<W>>,
}

pub(crate) struct Parts<W> {
    // components
    pub(crate) epoch_manager: EpochManager,
    pub(crate) cadence: Cadence,
    pub(crate) da: DA,
    pub(crate) collector: FinalizationCollector,
    pub(crate) proposing: Proposing,
    pub(crate) network: Network<NodeAppMessage, W>,
}

impl<W> Node<NodeRouter, W> {
    pub fn new(config: &NodeConfig) -> Result<Self, RunError> {
        config.validate()?;
        let credential = config.credential();
        let epoch_manager = config.epoch_manager()?;
        let epoch_handle = epoch_manager.genesis();

        // todo: a full node, which has no slot instance
        let validators = epoch_handle
            .validators_at(Slot::FIRST)
            .map_err(|_| "no validators at the first slot")?;
        if validators.index_of(&credential.node_id).is_none() {
            return Err("this node is not a validator".into());
        }

        let header_auth: Arc<HeaderAuth> = Arc::new(validate_header);
        let slot_config = config.cadence.chorus(&credential, header_auth);
        let slot_manager = SlotManager::new(slot_config, epoch_handle.clone());
        let conductor_config = config.cadence.conductor(config.genesis_deadline)?;
        let conductor = CadenceConductor::genesis(conductor_config, epoch_handle.clone(), ())?;
        let cadence = Cadence::new(slot_manager, conductor, epoch_handle.clone());

        let da_config = config.da.runtime(config.cadence.delta);
        let da = DARuntime::new(da_config, &credential, epoch_handle.clone());

        let planner = config.planner(epoch_handle.clone());
        let proposing: Proposing = match config.proposal.source {
            SourceKind::Random | SourceKind::Fixed => {
                let proposing = RandomProposing::new(planner, &config.proposal, config.rng());
                Box::new(proposing)
            }
            SourceKind::Mempool => {
                let mempool = Mempool::new(config.mempool.into());
                let proposing = MempoolProposing::new(planner, &config.proposal, mempool);
                Box::new(proposing)
            }
        };
        let mut network = Network::new(credential.node_id, epoch_handle.clone());
        if let Some(repeater) = config.repeater() {
            network = network.with_repeater(repeater);
        }
        let collector = FinalizationCollector::default();

        let parts = Parts {
            epoch_manager,
            cadence,
            da,
            collector,
            proposing,
            network,
        };
        Ok(Self {
            epoch_handle,
            router: NodeRouter::new(config),
            parts,
            outbox: VecDeque::new(),
        })
    }

    // the same node with a wrapped or replaced proposer, for scenarios
    pub fn map_proposing(mut self, wrap: impl FnOnce(Proposing) -> Proposing) -> Self {
        self.parts.proposing = wrap(self.parts.proposing);
        self
    }

    // the same components under a wrapped router, for scenarios
    pub fn map_router<R>(self, wrap: impl FnOnce(NodeRouter) -> R) -> Node<R, W> {
        Node {
            epoch_handle: self.epoch_handle,
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

    pub fn epoch_handle(&self) -> &EpochHandle {
        &self.epoch_handle
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
        if let Some(epoch_handle) = self.epoch_manager.poll() {
            return Some(Effect::Epoch(epoch_handle));
        }
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
            Routed::Epoch(event) => parts.epoch_manager.handle(now, event),
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
