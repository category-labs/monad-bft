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

mod metrics;
pub mod timed_event;

use std::{ops::DerefMut, pin::Pin};

pub use metrics::{ExecutorMetrics, ExecutorMetricsChain, Histogram};

/// An Executor executes Commands
/// Commands generally are output by State
pub trait Executor {
    type Command;

    fn exec(&mut self, commands: Vec<Self::Command>);
    fn metrics(&self) -> ExecutorMetricsChain;

    fn boxed<'a>(self) -> BoxExecutor<'a, Self::Command>
    where
        Self: Sized + Send + Unpin + 'a,
    {
        Box::pin(self)
    }
}

impl<E: Executor + ?Sized> Executor for Box<E> {
    type Command = E::Command;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        (**self).exec(commands)
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        (**self).metrics()
    }
}

impl<P> Executor for Pin<P>
where
    P: DerefMut,
    P::Target: Executor + Unpin,
{
    type Command = <P::Target as Executor>::Command;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        Pin::get_mut(Pin::as_mut(self)).exec(commands)
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        Pin::get_ref(Pin::as_ref(self)).metrics()
    }
}

pub type BoxExecutor<'a, C> = Pin<Box<dyn Executor<Command = C> + Send + Unpin + 'a>>;

// State is updated by an event and can output a list of commands in order to apply
// side-effects of the update.
// Commands are executed by Executors
// Generally, updaters produce an Event to update State
