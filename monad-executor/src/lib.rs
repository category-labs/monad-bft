mod metrics;
pub mod timed_event;

use std::{ops::DerefMut, pin::Pin};

pub use metrics::{ExecutorMetrics, ExecutorMetricsChain};

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
