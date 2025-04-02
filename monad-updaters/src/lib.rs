use std::{marker::PhantomData, pin::Pin};

use futures::Stream;
use monad_executor::Executor;
use monad_metrics::MetricsPolicy;
use pin_project::pin_project;

pub mod checkpoint;
pub mod ledger;
pub mod loopback;
pub mod parent;
pub mod state_root_hash;
pub mod statesync;
pub mod timestamp;
pub mod txpool;

#[cfg(feature = "tokio")]
pub mod config_loader;

#[cfg(all(feature = "tokio", feature = "monad-triedb"))]
pub mod triedb_state_root_hash;

#[cfg(feature = "tokio")]
pub mod timer;

#[cfg(feature = "tokio")]
pub mod tokio_timestamp;

#[cfg(feature = "tokio")]
pub mod local_router;

/// An Updater executes commands and produces events for State
pub trait Updater<E, MP>: Executor<MP> + Stream<Item = E>
where
    MP: MetricsPolicy,
{
    fn boxed<'a>(self) -> BoxUpdater<'a, E, MP, Self::Command, Self::Metrics>
    where
        Self: Sized + Send + Unpin + 'a,
    {
        Box::pin(self)
    }
}
impl<U, E, MP> Updater<E, MP> for U
where
    U: Executor<MP> + Stream<Item = E> + Send + Unpin,
    MP: MetricsPolicy,
{
}

pub type BoxUpdater<'a, E, MP, C, M> =
    Pin<Box<dyn Updater<E, MP, Command = C, Metrics = M> + Send + Unpin + 'a>>;

#[pin_project]
pub struct VoidMetricUpdater<U, E, MP>
where
    U: Updater<E, MP>,
    MP: MetricsPolicy,
{
    #[pin]
    updater: U,
    _phantom: PhantomData<(E, MP)>,
}

impl<U, E, MP> VoidMetricUpdater<U, E, MP>
where
    U: Updater<E, MP>,
    MP: MetricsPolicy,
{
    pub fn new(updater: U) -> Self {
        Self {
            updater,
            _phantom: PhantomData,
        }
    }
}

impl<U, E, MP> Stream for VoidMetricUpdater<U, E, MP>
where
    U: Updater<E, MP>,
    MP: MetricsPolicy,
{
    type Item = E;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().updater.poll_next(cx)
    }
}

impl<U, E, MP> Executor<MP> for VoidMetricUpdater<U, E, MP>
where
    U: Updater<E, MP>,
    MP: MetricsPolicy,
{
    type Command = U::Command;
    type Metrics = ();

    fn exec(&mut self, commands: Vec<Self::Command>) {
        self.updater.exec(commands);
    }

    fn metrics(&self) -> &Self::Metrics {
        &()
    }
}
