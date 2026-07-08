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

use std::time::Duration;

use monad_mcp_chorus::{
    NodeEvent, Runtime, WakeId,
    types::{NodeId, Timestamp, TimestampDelta, Validated},
};
use monad_sim::{Ctx, Handle, StepLabel, Time};
use monad_sim_swarm::{Net, SimClient};

// using NodeId as Addr
type SimNet<M> = Net<NodeId, M>;

// 1 chorus tick = 1ms of monad-sim time; Timestamp::GENESIS = Time(0).
pub(crate) fn time_of(at: Timestamp) -> Time {
    Time(0) + Duration::from_millis(at.ticks())
}

fn duration_of(delta: TimestampDelta) -> Duration {
    Duration::from_millis(delta.ticks())
}

pub(crate) fn to_timestamp(time: Time) -> Timestamp {
    let nanos = time.0;
    Timestamp::new((nanos / 1_000_000i128) as u64)
}

// A monad-sim process that contains a cadence runtime and
// participates in a simulated network. Translates between node events
// and monad-sim steps.
pub struct SimNode<M> {
    id: NodeId,
    runtime: Box<dyn Runtime<M>>,
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
            me: None,
            net: None,
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

    // interpret all pending runtime events into monad-sim steps.
    fn process(&mut self, ctx: &mut Ctx) {
        while let Some(event) = self.runtime.poll() {
            self.interpret(event, ctx);
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
    type Message = M;

    fn wire(&mut self, me: Handle<Self>, net: Handle<SimNet<M>>) {
        self.me = Some(me);
        self.net = Some(net);
    }

    fn receive(&mut self, from: NodeId, message: M, ctx: &mut Ctx) {
        // this is where the message validation would happen in a real
        // network.
        let message = Validated::new_unchecked(message, from);
        let now = to_timestamp(ctx.now());
        self.runtime.receive(now, message);
        self.process(ctx);
    }
}
