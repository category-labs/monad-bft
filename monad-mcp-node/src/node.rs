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
//! same runtime with the components in tasks.

use std::collections::VecDeque;

use crate::{
    RunError,
    chorus::{
        SlotManager, conductor::MonadConductor, slot::chorus::ChorusMessage, types::Timestamp,
    },
    component::{Cadence, Component, DA, Proposing, Repeater},
    config::NodeConfig,
    da::DARuntime,
    finalization::FinalizedSlot,
    network::{Inbound, Outbound},
    runtime::{Effect, NodeRuntime, Runtime},
};

pub enum NodeOutput {
    Send(Outbound),
    Finalized(FinalizedSlot),
}

pub struct Node<R = NodeRuntime> {
    // wiring of components
    pub(crate) runtime: R,

    // components
    pub(crate) cadence: Cadence,
    pub(crate) da: DA,
    pub(crate) proposing: Proposing,
    pub(crate) repeater: Option<Repeater<ChorusMessage>>,

    // node effects
    outbox: VecDeque<NodeOutput>,
}

impl Node {
    pub fn new(config: &NodeConfig) -> Result<Self, RunError> {
        let epoch_handle = config.epoch_handle()?;

        let slot_config = config.cadence.chorus();
        let slot_manager = SlotManager::new(slot_config, epoch_handle.chorus());
        let conductor_config = config.cadence.conductor(config.genesis_deadline)?;
        let conductor = MonadConductor::genesis(conductor_config, ())?;
        let cadence = Cadence::new(slot_manager, conductor);

        let da = DARuntime::new(
            config.da.runtime(),
            epoch_handle.da(),
            epoch_handle.proposers.clone(),
        );

        let proposing = Proposing::new(&epoch_handle, &config.proposal);
        let repeater = config.repeater().map(Repeater::new);

        Ok(Self {
            runtime: NodeRuntime::new(epoch_handle),
            cadence,
            da,
            proposing,
            repeater,
            outbox: VecDeque::new(),
        })
    }

    // the same components under a wrapped runtime, for scenarios
    pub fn map_runtime<R>(self, wrap: impl FnOnce(NodeRuntime) -> R) -> Node<R> {
        Node {
            runtime: wrap(self.runtime),
            cadence: self.cadence,
            da: self.da,
            proposing: self.proposing,
            repeater: self.repeater,
            outbox: self.outbox,
        }
    }
}

impl<R: Runtime> Node<R> {
    fn dispatch(&mut self, now: Timestamp, effect: Effect) {
        match effect {
            Effect::Cadence(input) => self.cadence.handle(now, input),
            Effect::DA(input) => self.da.handle(now, input),
            Effect::Proposing(input) => self.proposing.handle(now, input),
            Effect::Repeater(input) => {
                if let Some(repeater) = &mut self.repeater {
                    repeater.handle(now, input);
                }
            }
            Effect::Network(outbound) => self.outbox.push_back(NodeOutput::Send(outbound)),
            Effect::Ledger(finalized) => self.outbox.push_back(NodeOutput::Finalized(finalized)),
        }
    }

    // wire every component's outputs onward until nothing moves
    fn step(&mut self, now: Timestamp) {
        let mut effects = Vec::new();
        loop {
            while let Some(output) = self.cadence.poll() {
                self.runtime.handle_cadence(output, &mut effects);
            }
            while let Some(output) = self.da.poll() {
                self.runtime.handle_da(output, &mut effects);
            }
            while let Some(output) = self.proposing.poll() {
                self.runtime.handle_proposal(now, output, &mut effects);
            }
            while let Some(output) = self.repeater.as_mut().and_then(Repeater::poll) {
                self.runtime.handle_repeat(output, &mut effects);
            }
            if effects.is_empty() {
                return;
            }
            for effect in effects.drain(..) {
                self.dispatch(now, effect);
            }
        }
    }
}

impl<R: Runtime> Component for Node<R> {
    type Input = Inbound;
    type Output = NodeOutput;

    fn handle(&mut self, now: Timestamp, inbound: Inbound) {
        let mut effects = Vec::new();
        self.runtime.handle_inbound(inbound, &mut effects);
        for effect in effects {
            self.dispatch(now, effect);
        }
        self.step(now);
    }

    fn next_due(&self) -> Option<Timestamp> {
        let repeater = self.repeater.as_ref().and_then(Repeater::next_due);
        [
            self.cadence.next_due(),
            self.da.next_due(),
            self.proposing.next_due(),
            repeater,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn handle_due(&mut self, now: Timestamp) {
        handle_if_due(&mut self.cadence, now);
        handle_if_due(&mut self.da, now);
        handle_if_due(&mut self.proposing, now);
        if let Some(repeater) = &mut self.repeater {
            handle_if_due(repeater, now);
        }
        self.step(now);
    }

    fn poll(&mut self) -> Option<NodeOutput> {
        self.outbox.pop_front()
    }
}

fn handle_if_due<C: Component>(component: &mut C, now: Timestamp) {
    if component.next_due().is_some_and(|due| due <= now) {
        component.handle_due(now);
    }
}
