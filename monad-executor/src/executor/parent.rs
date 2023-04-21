use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{Command, Executor, Message, RouterCommand, TimerCommand};

use futures::Stream;
use futures::StreamExt;

pub struct ParentExecutor<R, T> {
    pub router: R,
    pub timer: T,
}

impl<R, T, M, OM> Executor for ParentExecutor<R, T>
where
    R: Executor<Command = RouterCommand<M, OM>>,
    T: Executor<Command = TimerCommand<M::Event>>,

    M: Message,
{
    type Command = Command<M, OM>;
    fn exec(&mut self, commands: Vec<Command<M, OM>>) {
        let (router_cmds, timer_cmds) = Command::split_commands(commands);
        self.router.exec(router_cmds);
        self.timer.exec(timer_cmds);
    }
}

impl<E, R, T> Stream for ParentExecutor<R, T>
where
    R: Stream<Item = E> + Unpin,
    T: Stream<Item = E> + Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();
        futures::stream::select(&mut this.router, &mut this.timer).poll_next_unpin(cx)
    }
}
