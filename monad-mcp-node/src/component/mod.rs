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

mod cadence;
mod da;
mod proposing;
mod repeater;

use std::future::pending;

use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinSet,
    time::sleep_until,
};
use tracing::{Instrument as _, Span};

pub use self::{
    cadence::{Cadence, CadenceInput, CadenceOutput, CadenceWireMsg},
    da::{DA, DAInput},
    proposing::{Proposing, ProposingInput, ProposingOutput},
    repeater::{Recipients, Repeater, RepeaterConfig, RepeaterInput, RepeaterOutput},
};
use crate::{chorus::types::Timestamp, clock::Clock};

// a component is a deterministic state machine with an optional timer
// effect.
pub trait Component {
    type Input;
    type Output;

    fn handle(&mut self, now: Timestamp, input: Self::Input);
    fn poll(&mut self) -> Option<Self::Output>;

    // a component without timers keeps the defaults
    fn next_due(&self) -> Option<Timestamp> {
        None
    }
    fn handle_due(&mut self, _now: Timestamp) {}
}

// where a composite sends the effects its wiring decides on
pub trait Dispatch<E> {
    fn dispatch(&mut self, effect: E);
}

impl<E> Dispatch<E> for Vec<E> {
    fn dispatch(&mut self, effect: E) {
        self.push(effect);
    }
}

// runs a component in its own task, the link its inputs and outputs
pub fn spawn<C>(component: C, clock: Clock, tasks: &mut JoinSet<()>) -> Link<C::Input, C::Output>
where
    C: Component + Send + 'static,
    C::Input: Send + 'static,
    C::Output: Send + 'static,
{
    let (ours, theirs) = Link::pair();
    tasks.spawn(run(component, clock, theirs).instrument(Span::current()));
    ours
}

async fn run<C>(mut component: C, clock: Clock, mut link: Link<C::Output, C::Input>)
where
    C: Component,
{
    flush(&mut component, &link);
    loop {
        tokio::select! {
            input = link.recv() => {
                let Some(input) = input else { return };
                component.handle(clock.now(), input);
            }
            () = sleep_until_due(&clock, component.next_due()) => {
                component.handle_due(clock.now());
            }
        }
        flush(&mut component, &link);
    }
}

fn flush<C>(component: &mut C, link: &Link<C::Output, C::Input>)
where
    C: Component,
{
    while let Some(output) = component.poll() {
        link.send(output);
    }
}

// pends forever while no timer is armed
async fn sleep_until_due(clock: &Clock, due: Option<Timestamp>) {
    let Some(due) = due else {
        return pending().await;
    };
    sleep_until(clock.instant_of(due).into()).await
}

// one end of a two-way channel between a component and whoever drives it
pub struct Link<Out, In> {
    sender: UnboundedSender<Out>,
    receiver: UnboundedReceiver<In>,
}

impl<Out, In> Link<Out, In> {
    pub fn pair() -> (Link<Out, In>, Link<In, Out>) {
        let (out_sender, out_receiver) = unbounded_channel();
        let (in_sender, in_receiver) = unbounded_channel();
        let ours = Link {
            sender: out_sender,
            receiver: in_receiver,
        };
        let theirs = Link {
            sender: in_sender,
            receiver: out_receiver,
        };
        (ours, theirs)
    }

    // dropped silently once the other end is gone
    pub fn send(&self, message: Out) {
        self.sender.send(message).ok();
    }

    // None once the other end is gone
    pub async fn recv(&mut self) -> Option<In> {
        self.receiver.recv().await
    }
}
