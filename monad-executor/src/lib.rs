use std::{ops::DerefMut, pin::Pin};

pub mod timed_event;

/// An Executor executes Commands
/// Commands generally are output by State
pub trait Executor {
    type Command;
    type Metrics;

    fn exec(&mut self, commands: Vec<Self::Command>);
    fn metrics(&self) -> &Self::Metrics;

    fn boxed<'a>(self) -> BoxExecutor<'a, Self::Command, Self::Metrics>
    where
        Self: Sized + Send + Unpin + 'a,
    {
        Box::pin(self)
    }
}

impl<E> Executor for Box<E>
where
    E: Executor + ?Sized,
{
    type Command = E::Command;
    type Metrics = E::Metrics;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        (**self).exec(commands)
    }

    fn metrics(&self) -> &Self::Metrics {
        (**self).metrics()
    }
}

impl<P> Executor for Pin<P>
where
    P: DerefMut,
    P::Target: Executor + Unpin,
{
    type Command = <P::Target as Executor>::Command;
    type Metrics = <P::Target as Executor>::Metrics;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        Pin::get_mut(Pin::as_mut(self)).exec(commands)
    }

    fn metrics(&self) -> &Self::Metrics {
        Pin::get_ref(Pin::as_ref(self)).metrics()
    }
}

pub type BoxExecutor<'a, C, M> =
    Pin<Box<dyn Executor<Command = C, Metrics = M> + Send + Unpin + 'a>>;

// State is updated by an event and can output a list of commands in order to apply
// side-effects of the update.
// Commands are executed by Executors
// Generally, updaters produce an Event to update State
