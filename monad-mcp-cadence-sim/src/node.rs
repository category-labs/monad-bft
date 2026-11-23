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

use std::sync::Arc;

use cadence::{
    Inbound, Outbound, Recipients, Runtime, slot::chorus::ChorusDAEvent, types::ValidatorIndex,
};
// choose stub cadence for implementation
use monad_mcp_cadence::stub as cadence;
use monad_mcp_common::{
    Slot, SlotLifecycle, Timestamp,
    spec::ValidatorData as _,
    stub::{NodeId, ValidatorData},
};
use monad_mcp_proposing::stub::ProposingInput;
use monad_sim::{CancelToken, Ctx, Handle, StepLabel, Time};
use monad_sim_swarm::{Net, SimClient};

use crate::{
    da::{DaAnnouncement, MockDa, MockDaOutput},
    proposing::MockProposing,
};

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

// What a node does with the local outputs of its runtime, supplied by the
// wiring, which knows the concrete runtime (see swarm.rs and mvba.rs).
pub(crate) type OutputFn<R> = dyn FnMut(Timestamp, <R as Runtime>::LocalOut) + Send;

// The node-level proposal machinery: the proposing runtime decides when to
// seal and what, the mock DA layer stores and announces the sealed proposals
// and reports what became available. The two functions come from the wiring,
// which knows the concrete runtime (see swarm.rs).
pub(crate) struct ProposerHarness<R: Runtime> {
    pub(crate) proposing: MockProposing,
    pub(crate) da: MockDa,
    // the slot lifecycle event in a local output of the runtime, if it is one
    pub(crate) lifecycle: fn(&R::LocalOut) -> Option<SlotLifecycle>,
    // a DA event as a local input of the runtime
    pub(crate) da_input: fn(Slot, ChorusDAEvent) -> R::LocalIn,
    /// The one live seal alarm, at the proposing runtime's last `next_due`.
    pub(crate) seal_alarm: Option<(Time, CancelToken)>,
}

// A monad-sim process that contains a runtime and participates in a
// simulated network. Translates between node events and monad-sim steps.
pub struct SimNode<R: Runtime> {
    id: NodeId,
    runtime: R,
    // None until the swarm builder sets it
    validators: Option<Arc<ValidatorData>>,
    on_output: Box<OutputFn<R>>,
    proposer: Option<ProposerHarness<R>>,
    /// The one live timer step, at the runtime's last polled `next_due`.
    due_alarm: Option<(Time, CancelToken)>,
    me: Option<Handle<Self>>,
    net: Option<Handle<SimNet<R::Message>>>,
}

impl<R> SimNode<R>
where
    R: Runtime + Send + 'static,
    R::Message: Clone + Send + 'static,
{
    pub(crate) fn new(
        id: NodeId,
        runtime: R,
        on_output: impl FnMut(Timestamp, R::LocalOut) + Send + 'static,
    ) -> Self {
        Self {
            id,
            runtime,
            validators: None,
            on_output: Box::new(on_output),
            proposer: None,
            due_alarm: None,
            me: None,
            net: None,
        }
    }

    pub(crate) fn with_proposer(
        id: NodeId,
        runtime: R,
        on_output: impl FnMut(Timestamp, R::LocalOut) + Send + 'static,
        harness: ProposerHarness<R>,
    ) -> Self {
        Self {
            proposer: Some(harness),
            ..Self::new(id, runtime, on_output)
        }
    }

    pub(crate) fn set_validators(&mut self, validators: Arc<ValidatorData>) {
        self.validators = Some(validators);
    }

    fn node_id(&self, index: ValidatorIndex) -> NodeId {
        let validators = self.validators.as_ref();
        *validators.expect("the validator table").node_id(index)
    }

    pub fn init(&mut self, ctx: &mut Ctx) {
        self.process(ctx);
    }

    pub(crate) fn handle_input(&mut self, input: R::LocalIn, ctx: &mut Ctx) {
        let now = to_timestamp(ctx.now());
        self.runtime.handle_input(now, input);
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
        let now = to_timestamp(ctx.now());
        loop {
            while let Some(event) = self.runtime.poll_msg() {
                self.interpret(event, ctx);
            }
            // local outputs may feed the slot lifecycle to the proposing
            // runtime; seal what is due and re-arm its alarm.
            while let Some(output) = self.runtime.poll_output() {
                self.tell_proposing(&output, now);
                (self.on_output)(now, output);
            }
            self.drain_proposing(ctx);
            // a sealed proposal is available to its own proposer at once;
            // reporting it can produce further runtime events.
            if !self.drain_da(ctx) {
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

    // Broadcast the announcements of the mock DA layer, and report the
    // proposals that became available locally to the slot consensus, the way
    // the real DA layer does. Returns whether anything was reported.
    fn drain_da(&mut self, ctx: &mut Ctx) -> bool {
        let Some(harness) = &mut self.proposer else {
            return false;
        };
        let net = self.net.expect("node not wired");

        let mut inputs = Vec::new();
        while let Some(output) = harness.da.poll() {
            match output {
                MockDaOutput::Announce(announcement) => {
                    let message = SimMessage::Da(announcement);
                    ctx.schedule(
                        net,
                        ctx.now(),
                        StepLabel::source("da-broadcast"),
                        move |net, ctx| net.broadcast(ctx, message),
                    );
                }
                MockDaOutput::Event(slot, event) => {
                    inputs.push((harness.da_input)(slot, event));
                }
            }
        }
        if inputs.is_empty() {
            return false;
        }

        let now = to_timestamp(ctx.now());
        for input in inputs {
            self.runtime.handle_input(now, input);
        }
        true
    }

    fn tell_proposing(&mut self, output: &R::LocalOut, now: Timestamp) {
        let Some(harness) = &mut self.proposer else {
            return;
        };
        let Some(event) = (harness.lifecycle)(output) else {
            return;
        };
        let input = ProposingInput::Lifecycle(event);
        harness.proposing.handle_input(input, now);
    }

    fn drain_proposing(&mut self, ctx: &mut Ctx) {
        let Some(harness) = &mut self.proposer else {
            return;
        };
        let now = to_timestamp(ctx.now());

        harness.proposing.handle_due(now);
        while let Some(proposal) = harness.proposing.poll() {
            harness.da.handle_proposal(proposal);
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
        let due = harness.proposing.next_due();
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

    fn interpret(&mut self, event: Outbound<R::Message>, ctx: &mut Ctx) {
        let now = ctx.now();
        let net = self.net.expect("node not wired");
        let message = SimMessage::Cadence(event.message);
        match event.to {
            Recipients::Validators(_, validators) => {
                let nodes = validators.iter().map(|to| self.node_id(to));
                let nodes: Vec<NodeId> = nodes.collect();
                ctx.schedule(net, now, StepLabel::source("send"), move |net, ctx| {
                    for to in nodes {
                        net.send(ctx, to, message.clone());
                    }
                });
            }
            Recipients::Node(to) => {
                ctx.schedule(net, now, StepLabel::source("unicast"), move |net, ctx| {
                    net.send(ctx, to, message)
                });
            }
            Recipients::Nodes(nodes) => {
                ctx.schedule(net, now, StepLabel::source("multicast"), move |net, ctx| {
                    for to in nodes {
                        net.send(ctx, to, message.clone());
                    }
                });
            }
        }
    }
}

impl<R> SimClient for SimNode<R>
where
    R: Runtime + Send + 'static,
    R::Message: Clone + Send + 'static,
{
    type Addr = NodeId;
    type Message = SimMessage<R::Message>;

    fn wire(&mut self, me: Handle<Self>, net: Handle<SimNet<R::Message>>) {
        self.me = Some(me);
        self.net = Some(net);
    }

    fn receive(&mut self, from: NodeId, message: SimMessage<R::Message>, ctx: &mut Ctx) {
        match message {
            SimMessage::Cadence(message) => {
                // this is where the message validation would happen in a real
                // network.
                let now = to_timestamp(ctx.now());
                let inbound = Inbound {
                    sender: from,
                    message,
                };
                self.runtime.receive(now, inbound);
                self.process(ctx);
            }
            SimMessage::Da(announcement) => {
                // nodes without a mock DA instance drop announcements (and
                // vote negative on the affected proposals). The mock layer
                // checks the announced lane against the proposer schedule;
                // see the module docs of crate::da.
                if let Some(harness) = &mut self.proposer {
                    harness.da.handle_announcement(announcement);
                }
                self.process(ctx);
            }
        }
    }
}
