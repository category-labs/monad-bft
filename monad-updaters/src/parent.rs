use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};
use monad_executor::Executor;
use monad_executor_glue::{
    CheckpointCommand, Command, LedgerCommand, MempoolCommand, Message, RouterCommand, TimerCommand,
};

pub struct ParentExecutor<R, T, M, L, C> {
    pub router: R,
    pub timer: T,
    pub mempool: M,
    pub ledger: L,
    pub checkpoint: C,
    // if you add an executor here, you must add it to BOTH exec AND poll_next !
}

impl<RE, TE, ME, LE, CE, M, OM, B, C> Executor for ParentExecutor<RE, TE, ME, LE, CE>
where
    RE: Executor<Command = RouterCommand<M, OM>>,
    TE: Executor<Command = TimerCommand<M::Event>>,

    CE: Executor<Command = CheckpointCommand<C>>,
    LE: Executor<Command = LedgerCommand<B, M::Event>>,
    ME: Executor<Command = MempoolCommand<M::Event>>,

    M: Message,
{
    type Command = Command<M, OM, B, C>;
    fn exec(&mut self, commands: Vec<Command<M, OM, B, C>>) {
        let (
            router_cmds,
            timer_cmds,
            mempool_cmds,
            ledger_cmds,
            checkpoint_cmds,
            _state_root_hash_cmds,
        ) = Command::split_commands(commands);
        self.router.exec(router_cmds);
        self.timer.exec(timer_cmds);
        self.mempool.exec(mempool_cmds);
        self.ledger.exec(ledger_cmds);
        self.checkpoint.exec(checkpoint_cmds);
    }
}

impl<E, R, T, M, L, C> Stream for ParentExecutor<R, T, M, L, C>
where
    R: Stream<Item = E> + Unpin,
    T: Stream<Item = E> + Unpin,
    M: Stream<Item = E> + Unpin,
    L: Stream<Item = E> + Unpin,
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
            this.ledger.by_ref().boxed_local(),
        ])
        .poll_next_unpin(cx)
    }
}

impl<R, T, M, L, C> ParentExecutor<R, T, M, L, C> {
    pub fn ledger(&self) -> &L {
        &self.ledger
    }
}
