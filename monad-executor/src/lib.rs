use std::{ops::DerefMut, pin::Pin};

use monad_metrics::MetricsPolicy;

pub mod timed_event;

/// An Executor executes Commands
/// Commands generally are output by State
pub trait Executor<MP>
where
    MP: MetricsPolicy,
{
    type Command;
    type Metrics;

    fn exec(&mut self, commands: Vec<Self::Command>);
    fn metrics(&self) -> &Self::Metrics;

    fn boxed<'a>(self) -> BoxExecutor<'a, MP, Self::Command, Self::Metrics>
    where
        Self: Sized + Send + Unpin + 'a,
    {
        Box::pin(self)
    }
}

impl<E, MP> Executor<MP> for Box<E>
where
    E: Executor<MP> + ?Sized,
    MP: MetricsPolicy,
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

impl<P, MP> Executor<MP> for Pin<P>
where
    P: DerefMut,
    P::Target: Executor<MP> + Unpin,
    MP: MetricsPolicy,
{
    type Command = <P::Target as Executor<MP>>::Command;
    type Metrics = <P::Target as Executor<MP>>::Metrics;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        Pin::get_mut(Pin::as_mut(self)).exec(commands)
    }

    fn metrics(&self) -> &Self::Metrics {
        Pin::get_ref(Pin::as_ref(self)).metrics()
    }
}

pub type BoxExecutor<'a, MP, C, M> =
    Pin<Box<dyn Executor<MP, Command = C, Metrics = M> + Send + Unpin + 'a>>;

// State is updated by an event and can output a list of commands in order to apply
// side-effects of the update.
// Commands are executed by Executors
// Generally, updaters produce an Event to update State
