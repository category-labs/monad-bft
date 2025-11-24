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

use std::pin::Pin;

use futures::Stream;
use monad_executor::Executor;

pub mod config_file;
pub mod ledger;
pub mod loopback;
pub mod parent;
pub mod statesync;
pub mod timestamp;
pub mod txpool;
pub mod val_set;

#[cfg(feature = "tokio")]
pub mod config_loader;

#[cfg(all(feature = "tokio", feature = "monad-triedb"))]
pub mod triedb_val_set;

#[cfg(feature = "tokio")]
pub mod timer;

#[cfg(feature = "tokio")]
pub mod tokio_timestamp;

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
impl<U, E> Updater<E> for U where U: Executor + Stream<Item = E> {}

pub type BoxUpdater<'a, C, E> = Pin<Box<dyn Updater<E, Command = C> + Send + Unpin + 'a>>;
