use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use monad_executor::Executor;
use monad_executor_glue::MetricsCommand;

/// A no-op executor for executing metrics commands.
pub struct NopMetricsExecutor<E> {
    _phantom: PhantomData<E>,
}

impl<E> Default for NopMetricsExecutor<E> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<E> Executor for NopMetricsExecutor<E> {
    type Command = MetricsCommand;

    fn replay(&mut self, mut _commands: Vec<Self::Command>) {}

    fn exec(&mut self, _commands: Vec<Self::Command>) {}
}

impl<E> Stream for NopMetricsExecutor<E> {
    type Item = E;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
