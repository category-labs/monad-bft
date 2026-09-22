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

use std::sync::{Arc, Mutex};

use chorus::{
    Outbound, Runtime,
    da::DataAvailability as _,
    proposing::ProposalPlanner,
    types::{NodeId, Timestamp, Validated},
};
// choose stub chorus for implementation
use monad_mcp_chorus::stub as chorus;
use monad_sim::{CancelToken, Ctx, Handle, StepLabel, Time};
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

pub(crate) fn to_timestamp(time: Time) -> Timestamp {
    Timestamp::from_nanos(
        u128::try_from(time.0).expect("simulation time cannot be converted to a timestamp"),
    )
}

// How an available proposal is reported to the slot consensus: the DA
// events, in order, that make it votable. Supplied by the wiring, which
// knows the concrete consensus (see swarm.rs).
pub(crate) type DaEventsFn<E> = dyn Fn(&DaAnnouncement) -> Vec<E>;

// The node-level proposal machinery: the planner decides when to seal, the
// mock DA layer stores and announces the sealed proposals and reports what
// became available. The planner is shared with the facts observer planted
// in the runtime (see swarm.rs).
pub(crate) struct ProposerHarness<E> {
    pub(crate) planner: Arc<Mutex<ProposalPlanner>>,
    pub(crate) da: Arc<MockDa>,
    pub(crate) da_events: Box<DaEventsFn<E>>,
    /// The one live seal alarm, at the planner's last polled `next_due`.
    pub(crate) seal_alarm: Option<(Time, CancelToken)>,
}

// A monad-sim process that contains a cadence runtime and
// participates in a simulated network. Translates between node events
// and monad-sim steps.
pub struct SimNode<M, E> {
    id: NodeId,
    runtime: Box<dyn Runtime<M, DAEvent = E>>,
    proposer: Option<ProposerHarness<E>>,
    /// The one live timer step, at the runtime's last polled `next_due`.
    due_alarm: Option<(Time, CancelToken)>,
    me: Option<Handle<Self>>,
    net: Option<Handle<SimNet<M>>>,
}

impl<M, E> SimNode<M, E>
where
    M: Clone + 'static,
    E: 'static,
{
    pub fn new(id: NodeId, runtime: impl Runtime<M, DAEvent = E> + 'static) -> Self {
        Self {
            id,
            runtime: Box::new(runtime),
            proposer: None,
            due_alarm: None,
            me: None,
            net: None,
        }
    }

    pub(crate) fn with_proposer(
        id: NodeId,
        runtime: impl Runtime<M, DAEvent = E> + 'static,
        harness: ProposerHarness<E>,
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

    fn handle_due(&mut self, ctx: &mut Ctx) {
        self.due_alarm = None;
        let now = to_timestamp(ctx.now());
        self.runtime.handle_due(now);
        self.process(ctx);
    }

    fn proposal_wake(&mut self, ctx: &mut Ctx) {
        if let Some(harness) = &mut self.proposer {
            harness.seal_alarm = None;
        }
        self.process(ctx);
    }

    // interpret all pending runtime events into monad-sim steps.
    fn process(&mut self, ctx: &mut Ctx) {
        loop {
            while let Some(event) = self.runtime.poll() {
                self.interpret(event, ctx);
            }
            // runtime steps may have fed facts to the planner (through the
            // observer planted in the runtime); seal what is due and re-arm
            // its alarm.
            self.drain_planner(ctx);
            // a sealed proposal is available to its own proposer at once;
            // reporting it can produce further runtime events.
            if !self.report_availability(ctx) {
                break;
            }
        }
        self.arm_due_alarm(ctx);
    }

    // one step at next_due; a due already passed fires now. Every runtime
    // input can move the due time, so re-arm after every process.
    fn arm_due_alarm(&mut self, ctx: &mut Ctx) {
        let me = self.me.expect("node not wired");
        let at = self
            .runtime
            .next_due()
            .map(|due| time_of(due).max(ctx.now()));
        if self.due_alarm.as_ref().map(|(at, _)| *at) == at {
            return;
        }
        if let Some((_, token)) = self.due_alarm.take() {
            token.cancel();
        }
        if let Some(at) = at {
            let token = ctx.schedule(me, at, StepLabel::source("due"), |node, ctx| {
                node.handle_due(ctx)
            });
            self.due_alarm = Some((at, token));
        }
    }

    // Report the proposals that became available locally to the slot
    // consensus, the way the real DA layer does. Returns whether anything
    // was reported.
    fn report_availability(&mut self, ctx: &mut Ctx) -> bool {
        let events: Vec<_> = match &self.proposer {
            None => return false,
            Some(harness) => harness
                .da
                .drain_available()
                .iter()
                .flat_map(|announcement| {
                    let slot = announcement.slot();
                    (harness.da_events)(announcement)
                        .into_iter()
                        .map(move |event| (slot, event))
                })
                .collect(),
        };

        if events.is_empty() {
            return false;
        }

        let now = to_timestamp(ctx.now());
        for (slot, event) in events {
            self.runtime.handle_da_event(now, slot, event);
        }
        true
    }

    fn drain_planner(&mut self, ctx: &mut Ctx) {
        let Some(harness) = &self.proposer else {
            return;
        };
        let now = to_timestamp(ctx.now());

        loop {
            let sealed = harness.planner.lock().expect("planner poisoned").poll(now);
            let Some((slot, index)) = sealed else {
                break;
            };
            harness
                .da
                .submit_proposal(slot, index, mock_payload(self.id, slot, index));
            let from = self.id;
            let net = self.net.expect("node not wired");
            for announcement in harness.da.drain_announcements() {
                let message = SimMessage::Da(announcement);
                ctx.schedule(
                    net,
                    ctx.now(),
                    StepLabel::source("da-broadcast"),
                    move |net, ctx| net.broadcast(ctx, from, message),
                );
            }
        }
        self.arm_seal_alarm(ctx);
    }

    // one alarm at next_due; a due already passed fires now. Facts can move
    // the due time (a released gated seal), so re-arm after every drain.
    fn arm_seal_alarm(&mut self, ctx: &mut Ctx) {
        let me = self.me.expect("node not wired");
        let Some(harness) = &mut self.proposer else {
            return;
        };
        let due = harness.planner.lock().expect("planner poisoned").next_due();
        let at = due.map(|due| time_of(due).max(ctx.now()));
        if harness.seal_alarm.as_ref().map(|(at, _)| *at) == at {
            return;
        }
        if let Some((_, token)) = harness.seal_alarm.take() {
            token.cancel();
        }
        if let Some(at) = at {
            let token = ctx.schedule(me, at, StepLabel::source("proposal-wake"), |node, ctx| {
                node.proposal_wake(ctx)
            });
            harness.seal_alarm = Some((at, token));
        }
    }

    fn interpret(&mut self, event: Outbound<M>, ctx: &mut Ctx) {
        let now = ctx.now();
        match event {
            Outbound::Broadcast(message) => {
                let from = self.id;
                let net = self.net.expect("node not wired");
                let message = SimMessage::Cadence(message);
                ctx.schedule(net, now, StepLabel::source("broadcast"), move |net, ctx| {
                    net.broadcast(ctx, from, message)
                });
            }
            Outbound::Unicast(to, message) => {
                let from = self.id;
                let net = self.net.expect("node not wired");
                let message = SimMessage::Cadence(message);
                ctx.schedule(net, now, StepLabel::source("unicast"), move |net, ctx| {
                    net.send(ctx, from, to, message)
                });
            }
        }
    }
}

impl<M, E> SimClient for SimNode<M, E>
where
    M: Clone + 'static,
    E: 'static,
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
                // vote negative on the affected proposals). The mock layer
                // checks the announced index against the proposer schedule;
                // see the module docs of crate::da.
                if let Some(harness) = &self.proposer {
                    harness.da.receive_announcement(announcement);
                }
                self.process(ctx);
            }
        }
    }
}
