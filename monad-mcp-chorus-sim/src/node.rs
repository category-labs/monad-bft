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

use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use chorus::{
    NodeEvent, Runtime, WakeId,
    da::DataAvailability as _,
    proposing::{PlannerOutput, ProposalPlanner},
    types::{NodeId, Slot, Timestamp, TimestampDelta, Validated},
};
// choose stub chorus for implementation
use monad_mcp_chorus::stub as chorus;
use monad_sim::{Ctx, Handle, StepLabel, Time};
use monad_sim_swarm::{Net, SimClient};

use crate::da::{DaAnnouncement, MockDa, mock_payload};

// The wire format of the simulated network: consensus messages plus the
// mock DA layer's dissemination, sharing one transport (and latency model).
#[derive(Clone)]
pub enum SimMessage<M> {
    Cadence(M),
    Da(DaAnnouncement),
}

// using NodeId as Addr
type SimNet<M> = Net<NodeId, SimMessage<M>>;

// Both chorus and monad-sim store time in nanoseconds.
pub(crate) fn time_of(at: Timestamp) -> Time {
    Time(i128::try_from(at.as_nanos()).expect("chorus timestamp exceeds simulation time range"))
}

fn duration_of(delta: TimestampDelta) -> Duration {
    delta.as_duration()
}

pub(crate) fn to_timestamp(time: Time) -> Timestamp {
    Timestamp::from_nanos(
        u128::try_from(time.0).expect("simulation time cannot be converted to a timestamp"),
    )
}

// The node-level proposal machinery: the planner decides when to seal, the
// mock DA layer stores and announces the sealed proposals. The planner is
// shared with the facts observer planted in the runtime (see swarm.rs).
pub(crate) struct ProposerHarness {
    pub(crate) planner: Rc<RefCell<ProposalPlanner>>,
    pub(crate) da: Arc<MockDa>,
}

// A monad-sim process that contains a cadence runtime and
// participates in a simulated network. Translates between node events
// and monad-sim steps.
pub struct SimNode<M> {
    id: NodeId,
    runtime: Box<dyn Runtime<M>>,
    proposer: Option<ProposerHarness>,
    me: Option<Handle<Self>>,
    net: Option<Handle<SimNet<M>>>,
}

impl<M> SimNode<M>
where
    M: Clone + 'static,
{
    pub fn new(id: NodeId, runtime: impl Runtime<M> + 'static) -> Self {
        Self {
            id,
            runtime: Box::new(runtime),
            proposer: None,
            me: None,
            net: None,
        }
    }

    pub(crate) fn with_proposer(
        id: NodeId,
        runtime: impl Runtime<M> + 'static,
        harness: ProposerHarness,
    ) -> Self {
        Self {
            proposer: Some(harness),
            ..Self::new(id, runtime)
        }
    }

    pub fn init(&mut self, ctx: &mut Ctx) {
        self.runtime.init();
        self.process(ctx);
    }

    fn wake(&mut self, id: WakeId, ctx: &mut Ctx) {
        let now = to_timestamp(ctx.now());
        self.runtime.wake(now, id);
        self.process(ctx);
    }

    fn proposal_wake(&mut self, slot: Slot, ctx: &mut Ctx) {
        if let Some(harness) = &self.proposer {
            let now = to_timestamp(ctx.now());
            harness.planner.borrow_mut().handle_wake(now, slot);
        }
        self.drain_planner(ctx);
    }

    // interpret all pending runtime events into monad-sim steps.
    fn process(&mut self, ctx: &mut Ctx) {
        while let Some(event) = self.runtime.poll() {
            self.interpret(event, ctx);
        }
        // runtime steps may have fed facts to the planner (through the
        // observer planted in the runtime); execute its requests.
        self.drain_planner(ctx);
    }

    fn drain_planner(&mut self, ctx: &mut Ctx) {
        let Some(harness) = &self.proposer else {
            return;
        };

        loop {
            let output = harness.planner.borrow_mut().poll();
            let Some(output) = output else {
                break;
            };
            match output {
                PlannerOutput::ScheduleWake { at, slot } => {
                    let me = self.me.expect("node not wired");
                    // an alarm for a passed timestamp fires immediately
                    let at = time_of(at).max(ctx.now());
                    ctx.schedule(
                        me,
                        at,
                        StepLabel::source("proposal-wake"),
                        move |node, ctx| node.proposal_wake(slot, ctx),
                    );
                }
                PlannerOutput::Seal { slot, index } => {
                    harness
                        .da
                        .submit_proposal(slot, index, mock_payload(self.id, slot, index));
                    let from = self.id;
                    let net = self.net.expect("node not wired");
                    let now = ctx.now();
                    for announcement in harness.da.drain_announcements() {
                        let message = SimMessage::Da(announcement);
                        ctx.schedule(
                            net,
                            now,
                            StepLabel::source("da-broadcast"),
                            move |net, ctx| net.broadcast(ctx, from, message),
                        );
                    }
                }
            }
        }
    }

    fn interpret(&mut self, event: NodeEvent<M>, ctx: &mut Ctx) {
        let me = self.me.expect("node not wired");
        let now = ctx.now();
        match event {
            NodeEvent::Wake(at, id) => {
                // an alarm for a passed timestamp fires immediately
                let at = time_of(at).max(now);
                ctx.schedule(me, at, StepLabel::source("wake"), move |node, ctx| {
                    node.wake(id, ctx)
                });
            }
            NodeEvent::WakeAfter(delta, id) => {
                let delta = duration_of(delta);
                ctx.schedule_after(me, delta, StepLabel::source("wake"), move |node, ctx| {
                    node.wake(id, ctx)
                });
            }
            NodeEvent::Broadcast(message) => {
                let from = self.id;
                let net = self.net.expect("node not wired");
                let message = SimMessage::Cadence(message);
                ctx.schedule(net, now, StepLabel::source("broadcast"), move |net, ctx| {
                    net.broadcast(ctx, from, message)
                });
            }
        }
    }
}

impl<M> SimClient for SimNode<M>
where
    M: Clone + 'static,
{
    type Addr = NodeId;
    type Message = SimMessage<M>;

    fn wire(&mut self, me: Handle<Self>, net: Handle<SimNet<M>>) {
        self.me = Some(me);
        self.net = Some(net);
    }

    fn receive(&mut self, from: NodeId, message: SimMessage<M>, ctx: &mut Ctx) {
        match message {
            SimMessage::Cadence(message) => {
                // this is where the message validation would happen in a real
                // network.
                let message = Validated::new_unchecked(message, from);
                let now = to_timestamp(ctx.now());
                self.runtime.receive(now, message);
                self.process(ctx);
            }
            SimMessage::Da(announcement) => {
                // nodes without a mock DA instance drop announcements (and
                // vote negative on the affected proposals). Proposer identity
                // is not validated here; see the module docs of crate::da.
                if let Some(harness) = &self.proposer {
                    harness.da.receive_announcement(announcement);
                }
            }
        }
    }
}
