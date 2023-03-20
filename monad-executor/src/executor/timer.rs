use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{Executor, TimerCommand};

use futures::{FutureExt, Stream};

struct TokioTimer<E> {
    state: Option<(Pin<Box<tokio::time::Sleep>>, E)>,
}
impl<E> TokioTimer<E> {
    pub fn new() -> Self {
        Self { state: None }
    }
}
impl<E> Executor for TokioTimer<E> {
    type Command = TimerCommand<E>;
    fn exec(&mut self, commands: Vec<TimerCommand<E>>) {
        let mut timer = None;
        for command in commands {
            match command {
                TimerCommand::Schedule {
                    duration,
                    on_timeout,
                } => timer = Some((duration, on_timeout)),
                TimerCommand::Unschedule => timer = None,
            }
        }

        self.state = timer.map(|(duration, e)| (Box::pin(tokio::time::sleep(duration)), e));
    }
}
impl<E> Stream for TokioTimer<E>
where
    Self: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();
        match this.state.as_mut() {
            None => Poll::Ready(None),
            Some((sleep, _)) => sleep
                .poll_unpin(cx)
                .map(|()| this.state.take().map(|(_, event)| event)),
        }
    }
}
