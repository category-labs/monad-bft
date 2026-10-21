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

//! The pq node with every component stepped in place: deterministic,
//! itself a component, for simulation and tests. `PqAsyncNode` runs the
//! same runtime with the components in tasks.

use std::collections::VecDeque;

use monad_mcp_node::{RunError, config::SourceKind};
use monad_pq_chorus::{Evidence, PqMessage};

use crate::{
    chorus::{
        SlotManager,
        conductor::MonadConductor,
        types::{Slot, Timestamp},
    },
    component::{
        Component, Mempool, MempoolSource, PqCadence, PqDA, ProposalSource, Proposing,
        RandomSource, Repeater, SharedMempool,
    },
    config::{PqConfigError, PqEpoch, PqNodeConfig},
    da::{DAConfig, DARuntime},
    finalization::PqFinalizedSlot,
    network::{Inbound, Outbound},
    runtime::{PqEffect, PqNodeRuntime, PqRuntime},
};

pub enum PqNodeOutput {
    Send(Outbound),
    Finalized(PqFinalizedSlot),
    Evidence(Slot, Evidence),
}

pub struct PqNode<R = PqNodeRuntime> {
    // wiring of components
    pub(crate) runtime: R,

    // components
    pub(crate) cadence: PqCadence,
    pub(crate) da: PqDA,
    pub(crate) proposing: Proposing,
    pub(crate) repeater: Repeater<PqMessage>,

    // node effects
    outbox: VecDeque<PqNodeOutput>,
}

impl PqNode {
    pub fn new(config: &PqNodeConfig) -> Result<Self, RunError> {
        config.validate()?;
        let PqEpoch {
            handle,
            da: da_handle,
            context,
        } = config.epoch()?;
        let base = &config.base;

        let slot_manager = SlotManager::new(config.quantum_config(), context);
        let conductor_config = base.cadence.conductor(base.genesis_deadline)?;
        let conductor = MonadConductor::genesis(conductor_config, ())?;
        let cadence = PqCadence::new(slot_manager, conductor);

        let da_config = DAConfig {
            completed_slot_retention: base.da.completed_slot_retention,
            delta: base.cadence.delta,
        };
        let da = PqDA(DARuntime::new(
            da_config,
            da_handle.clone(),
            handle.proposers.clone(),
        ));

        let mempool = match base.proposal.source {
            SourceKind::Random => None,
            SourceKind::Mempool => Some(SharedMempool::new(Mempool::new(base.mempool))),
        };
        let source: Box<dyn ProposalSource + Send> = match &mempool {
            None => Box::new(RandomSource),
            Some(mempool) => Box::new(MempoolSource(mempool.clone())),
        };
        let proposing = Proposing::new(&handle, &base.proposal, source);
        let repeater_config = base
            .repeater()
            .ok_or_else(|| PqConfigError("[repeater] is required".into()))?;

        Ok(Self {
            runtime: PqNodeRuntime::new(handle, da_handle, mempool),
            cadence,
            da,
            proposing,
            repeater: Repeater::new(repeater_config),
            outbox: VecDeque::new(),
        })
    }

    // the same components under a wrapped runtime, for scenarios
    pub fn map_runtime<R>(self, wrap: impl FnOnce(PqNodeRuntime) -> R) -> PqNode<R> {
        PqNode {
            runtime: wrap(self.runtime),
            cadence: self.cadence,
            da: self.da,
            proposing: self.proposing,
            repeater: self.repeater,
            outbox: self.outbox,
        }
    }
}

impl<R> PqNode<R> {
    pub fn runtime(&self) -> &R {
        &self.runtime
    }
}

impl<R: PqRuntime> PqNode<R> {
    fn dispatch(&mut self, now: Timestamp, effect: PqEffect) {
        match effect {
            PqEffect::Cadence(input) => self.cadence.handle(now, input),
            PqEffect::DA(input) => self.da.handle(now, input),
            PqEffect::Proposing(input) => self.proposing.handle(now, input),
            PqEffect::Repeater(input) => self.repeater.handle(now, input),
            PqEffect::Network(outbound) => self.outbox.push_back(PqNodeOutput::Send(outbound)),
            PqEffect::Ledger(finalized) => {
                self.outbox.push_back(PqNodeOutput::Finalized(finalized))
            }
            PqEffect::Evidence(slot, evidence) => self
                .outbox
                .push_back(PqNodeOutput::Evidence(slot, evidence)),
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
                self.runtime.handle_da(now, output, &mut effects);
            }
            while let Some(output) = self.proposing.poll() {
                self.runtime.handle_proposal(now, output, &mut effects);
            }
            while let Some(output) = self.repeater.poll() {
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

impl<R: PqRuntime> Component for PqNode<R> {
    type Input = Inbound;
    type Output = PqNodeOutput;

    fn handle(&mut self, now: Timestamp, inbound: Inbound) {
        let mut effects = Vec::new();
        self.runtime.handle_inbound(now, inbound, &mut effects);
        for effect in effects {
            self.dispatch(now, effect);
        }
        self.step(now);
    }

    fn next_due(&self) -> Option<Timestamp> {
        [
            self.cadence.next_due(),
            self.da.next_due(),
            self.proposing.next_due(),
            self.repeater.next_due(),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn handle_due(&mut self, now: Timestamp) {
        handle_if_due(&mut self.cadence, now);
        handle_if_due(&mut self.da, now);
        handle_if_due(&mut self.proposing, now);
        handle_if_due(&mut self.repeater, now);
        self.step(now);
    }

    fn poll(&mut self) -> Option<PqNodeOutput> {
        self.outbox.pop_front()
    }
}

fn handle_if_due<C: Component>(component: &mut C, now: Timestamp) {
    if component.next_due().is_some_and(|due| due <= now) {
        component.handle_due(now);
    }
}
