use std::{marker::PhantomData, pin::Pin};

use futures::Stream;
use monad_executor::Executor;
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
pub trait Updater<E>: Executor + Stream<Item = E> {
    fn boxed<'a>(self) -> BoxUpdater<'a, E, Self::Command, Self::Metrics>
    where
        Self: Sized + Send + Unpin + 'a,
    {
        Box::pin(self)
    }
}
impl<U, E> Updater<E> for U where U: Executor + Stream<Item = E> + Send + Unpin {}

pub type BoxUpdater<'a, E, C, M> =
    Pin<Box<dyn Updater<E, Command = C, Metrics = M> + Send + Unpin + 'a>>;

#[pin_project]
pub struct VoidMetricUpdater<U, E>
where
    U: Updater<E>,
{
    #[pin]
    updater: U,
    _phantom: PhantomData<E>,
}

impl<U, E> VoidMetricUpdater<U, E>
where
    U: Updater<E>,
{
    pub fn new(updater: U) -> Self {
        Self {
            updater,
            _phantom: PhantomData,
        }
    }
}

impl<U, E> Stream for VoidMetricUpdater<U, E>
where
    U: Updater<E>,
{
    type Item = E;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().updater.poll_next(cx)
    }
}

impl<U, E> Executor for VoidMetricUpdater<U, E>
where
    U: Updater<E>,
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
