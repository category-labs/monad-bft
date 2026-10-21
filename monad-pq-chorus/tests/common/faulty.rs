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

//! A Byzantine `SlotConsensus`: a correct `QuantumChorus` whose outputs a
//! rewrite turns into arbitrary sends.

use std::{collections::VecDeque, sync::Arc};

use monad_mcp_chorus::stub::{
    SlotConsensus, SlotOutput,
    types::{NodeId, Slot},
};
use monad_pq_chorus::{
    PqEvent, PqHalt, PqMessage, PqTimer, QuantumChorus, QuantumConfig, QuantumContext,
    da::{ChorusDAEvent, PqDACommand},
};

pub type Rewrite = Arc<
    dyn Fn(NodeId, Slot, SlotOutput<QuantumChorus>) -> Vec<SlotOutput<QuantumChorus>> + Send + Sync,
>;

#[derive(Clone)]
pub struct FaultyConfig {
    pub inner: QuantumConfig,
    pub rewrite: Rewrite,
}

pub struct Faulty {
    inner: QuantumChorus,
    me: NodeId,
    slot: Slot,
    rewrite: Rewrite,
    out: VecDeque<SlotOutput<Faulty>>,
}

fn convert(output: SlotOutput<QuantumChorus>) -> SlotOutput<Faulty> {
    match output {
        SlotOutput::ScheduleTimer(delta, timer) => SlotOutput::ScheduleTimer(delta, timer),
        SlotOutput::Broadcast(message) => SlotOutput::Broadcast(message),
        SlotOutput::Unicast { to, message } => SlotOutput::Unicast { to, message },
        SlotOutput::DA(command) => SlotOutput::DA(command),
        SlotOutput::CommitOptimistic(event) => SlotOutput::CommitOptimistic(event),
        SlotOutput::Finalize(halt) => SlotOutput::Finalize(halt),
        SlotOutput::Fault { reason } => SlotOutput::Fault { reason },
    }
}

impl SlotConsensus for Faulty {
    type Config = FaultyConfig;
    type Context = QuantumContext;
    type Message = PqMessage;
    type Timer = PqTimer;
    type OptimisticCommitData = PqEvent;
    type FinalizationData = PqHalt;
    type DAEvent = ChorusDAEvent;
    type DACommand = PqDACommand;

    fn new(slot: Slot, config: &FaultyConfig, context: &QuantumContext) -> Self {
        Self {
            inner: QuantumChorus::new(slot, &config.inner, context),
            me: context.node_id,
            slot,
            rewrite: config.rewrite.clone(),
            out: VecDeque::new(),
        }
    }

    fn handle_deadline(&mut self) {
        self.inner.handle_deadline();
    }

    fn handle_message(&mut self, sender: NodeId, message: PqMessage) {
        self.inner.handle_message(sender, message);
    }

    fn handle_timer(&mut self, timer: PqTimer) {
        self.inner.handle_timer(timer);
    }

    fn handle_da_event(&mut self, event: ChorusDAEvent) {
        self.inner.handle_da_event(event);
    }

    fn poll(&mut self) -> Option<SlotOutput<Self>> {
        while let Some(output) = self.inner.poll() {
            let rewritten = (self.rewrite)(self.me, self.slot, output);
            self.out.extend(rewritten.into_iter().map(convert));
        }
        self.out.pop_front()
    }
}
