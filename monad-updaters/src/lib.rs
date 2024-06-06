use std::pin::Pin;

use futures::Stream;
use monad_executor::Executor;

pub mod checkpoint;
pub mod ipc;
pub mod ledger;
pub mod loopback;
pub mod nop_metrics;
pub mod parent;
pub mod state_root_hash;
pub mod validator_set;

#[cfg(all(feature = "tokio", feature = "monad-triedb"))]
pub mod triedb_state_root_hash;

#[cfg(feature = "tokio")]
pub mod timer;

#[cfg(feature = "tokio")]
pub mod local_router;

/// An Updater executes commands and produces events for State
pub trait Updater<E>: Executor + Stream<Item = E> {
    fn boxed<'a>(self) -> BoxUpdater<'a, Self::Command, E>
    where
        Self: Sized + Send + Unpin + 'a,
    {
        Box::pin(self)
    }
}
impl<U, E> Updater<E> for U where U: Executor + Stream<Item = E> + Send + Unpin {}

pub type BoxUpdater<'a, C, E> = Pin<Box<dyn Updater<E, Command = C> + Send + Unpin + 'a>>;
