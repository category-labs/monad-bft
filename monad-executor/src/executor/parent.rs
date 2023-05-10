use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{Command, Message, RouterCommand, TimerCommand};
use monad_types::{CounterCommand, Executor};

use futures::Stream;
use futures::StreamExt;

pub struct ParentExecutor<R, T, C> {
    pub router: R,
    pub timer: T,
    pub counter: C,
}

impl<R, T, C, M, OM> Executor for ParentExecutor<R, T, C>
where
    R: Executor<Command = RouterCommand<M, OM>>,
    T: Executor<Command = TimerCommand<M::Event>>,
    C: Executor<Command = CounterCommand>,

    M: Message,
{
    type Command = Command<M, OM>;
    fn exec(&mut self, commands: Vec<Command<M, OM>>) {
        let (router_cmds, timer_cmds, counter_cmds) = Command::split_commands(commands);
        self.router.exec(router_cmds);
        self.timer.exec(timer_cmds);
        self.counter.exec(counter_cmds);
    }
}

impl<E, R, T, C> Stream for ParentExecutor<R, T, C>
where
    R: Stream<Item = E> + Unpin,
    T: Stream<Item = E> + Unpin,
    C: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();
        futures::stream::select(&mut this.router, &mut this.timer).poll_next_unpin(cx)
    }
}
