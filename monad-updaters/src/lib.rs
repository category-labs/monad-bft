use std::pin::Pin;

use futures::Stream;
use monad_executor::Executor;

pub mod checkpoint;
pub mod validator_set;
pub mod execution_ledger;
pub mod ledger;
pub mod parent;
pub mod state_root_hash;

#[cfg(feature = "tokio")]
pub mod mempool;

#[cfg(feature = "tokio")]
pub mod timer;

#[cfg(feature = "tokio")]
pub mod local_router;

pub trait Updater<C, E>: Executor<Command = C> + Stream<Item = E> {}
impl<U, C, E> Updater<C, E> for U where U: Executor<Command = C> + Stream<Item = E> {}

pub type BoxUpdater<C, E> = Pin<Box<dyn Updater<C, E> + Send + Unpin>>;
