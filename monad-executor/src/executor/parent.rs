use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{Command, Executor, MempoolCommand, Message, RouterCommand, TimerCommand};

use futures::Stream;
use futures::StreamExt;

pub struct ParentExecutor<R, T, M> {
    pub router: R,
    pub timer: T,
    pub mempool: M,
    // if you add an executor here, you must add it to BOTH exec AND poll_next !
}

impl<RE, TE, ME, M, OM> Executor for ParentExecutor<RE, TE, ME>
where
    RE: Executor<Command = RouterCommand<M, OM>>,
    TE: Executor<Command = TimerCommand<M::Event>>,

    ME: Executor<Command = MempoolCommand<M::Event>>,

    M: Message,
{
    type Command = Command<M, OM>;
    fn exec(&mut self, commands: Vec<Command<M, OM>>) {
        let (router_cmds, timer_cmds, mempool_cmds) = Command::split_commands(commands);
        self.router.exec(router_cmds);
        self.timer.exec(timer_cmds);
        self.mempool.exec(mempool_cmds);
    }
}

impl<E, R, T, M> Stream for ParentExecutor<R, T, M>
where
    R: Stream<Item = E> + Unpin,
    T: Stream<Item = E> + Unpin,
    M: Stream<Item = E> + Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        // FIXME boxing is unnecessary
        futures::stream::select_all(vec![
            this.router.by_ref().boxed_local(),
            this.timer.by_ref().boxed_local(),
            this.mempool.by_ref().boxed_local(),
        ])
        .poll_next_unpin(cx)
    }
}
