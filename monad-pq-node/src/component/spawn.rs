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

use std::future::pending;

use tokio::{task::JoinSet, time::sleep_until};
use tracing::{Instrument as _, Span};

use super::{Component, Link};
use crate::{chorus::types::Timestamp, clock::Clock};

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
