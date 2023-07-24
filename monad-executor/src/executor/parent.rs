use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};

use crate::{
    Command, Executor, LedgerCommand, MempoolCommand, Message, RouterCommand, TimerCommand,
};

pub struct ParentExecutor<R, T, M, L> {
    pub router: R,
    pub timer: T,
    pub mempool: M,
    pub ledger: L,
    // if you add an executor here, you must add it to BOTH exec AND poll_next !
}

impl<RE, TE, ME, LE, M, OM, B> Executor for ParentExecutor<RE, TE, ME, LE>
where
    RE: Executor<Command = RouterCommand<M, OM>>,
    TE: Executor<Command = TimerCommand<M::Event>>,

    ME: Executor<Command = MempoolCommand<M::Event>>,
    LE: Executor<Command = LedgerCommand<B>>,

    M: Message,
{
    type Command = Command<M, OM, B>;
    fn exec(&mut self, commands: Vec<Command<M, OM, B>>) {
        let (router_cmds, timer_cmds, mempool_cmds, ledger_cmds) =
            Command::split_commands(commands);
        self.router.exec(router_cmds);
        self.timer.exec(timer_cmds);
        self.mempool.exec(mempool_cmds);
        self.ledger.exec(ledger_cmds);
    }
}

impl<E, R, T, M, L> Stream for ParentExecutor<R, T, M, L>
where
    R: Stream<Item = E> + Unpin,
    T: Stream<Item = E> + Unpin,
    M: Stream<Item = E> + Unpin,
    Self: Unpin,
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

impl<R, T, M, L> ParentExecutor<R, T, M, L> {
    pub fn ledger(&self) -> &L {
        &self.ledger
    }
}
