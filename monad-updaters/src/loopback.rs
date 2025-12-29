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

use std::{
    collections::VecDeque,
    ops::DerefMut,
    task::{Poll, Waker},
};

use futures::Stream;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::LoopbackCommand;

/// The loopback executor routes outputs from one child state to another. The
/// update happens asynchronously and the event/operation must be idempotent on
/// the target state as replay can cause duplicate event to be applied.
///
/// e.g. StateRootUpdate for ConsensusState is idempotent because inserting the
/// same value to a map multiple times doesn't change the final state
pub struct LoopbackExecutor<E> {
    /// Buffered events to send back
    buffer: VecDeque<E>,
    waker: Option<Waker>,
    metrics: ExecutorMetrics,
}

impl<E> Default for LoopbackExecutor<E> {
    fn default() -> Self {
        Self {
            buffer: Default::default(),
            waker: Default::default(),
            metrics: Default::default(),
        }
    }
}

impl<E> Executor for LoopbackExecutor<E> {
    type Command = LoopbackCommand<E>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for cmd in commands {
            match cmd {
                LoopbackCommand::Forward(event) => self.buffer.push_back(event),
            }
        }

        if self.ready() {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        self.metrics.as_ref().into()
    }
}

impl<E> Stream for LoopbackExecutor<E>
where
    Self: Unpin,
{
    type Item = E;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some(e) = this.buffer.pop_front() {
            Poll::Ready(Some(e))
        } else {
            this.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<E> LoopbackExecutor<E> {
    pub fn ready(&self) -> bool {
        !self.buffer.is_empty()
    }
}
