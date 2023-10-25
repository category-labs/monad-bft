use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{FutureExt, Stream, StreamExt};
use monad_executor::Executor;
use monad_executor_glue::{
    CheckpointCommand, Command, ExecutionLedgerCommand, LedgerCommand, MempoolCommand,
    RouterCommand, TimerCommand,
};

pub struct ParentExecutor<R, T, M, L, EL, C> {
    pub router: R,
    pub timer: T,
    pub mempool: M,
    pub ledger: L,
    pub execution_ledger: EL,
    pub checkpoint: C,
    // if you add an executor here, you must add it to BOTH exec AND poll_next !
}

impl<RE, TE, ME, LE, EL, CE, E, OM, B, C, S> Executor for ParentExecutor<RE, TE, ME, LE, EL, CE>
where
    RE: Executor<Command = RouterCommand<OM>>,
    TE: Executor<Command = TimerCommand<E>>,

    CE: Executor<Command = CheckpointCommand<C>>,
    LE: Executor<Command = LedgerCommand<B, E>>,
    EL: Executor<Command = ExecutionLedgerCommand<S>>,
    ME: Executor<Command = MempoolCommand<S>>,
{
    type Command = Command<E, OM, B, C, S>;
    fn exec(&mut self, commands: Vec<Command<E, OM, B, C, S>>) {
        let (
            router_cmds,
            timer_cmds,
            mempool_cmds,
            ledger_cmds,
            execution_ledger_cmds,
            checkpoint_cmds,
            _state_root_hash_cmds,
        ) = Command::split_commands(commands);

        self.router.exec(router_cmds);
        self.timer.exec(timer_cmds);
        self.mempool.exec(mempool_cmds);
        self.ledger.exec(ledger_cmds);
        self.execution_ledger.exec(execution_ledger_cmds);
        self.checkpoint.exec(checkpoint_cmds);
    }
}

impl<E, R, T, M, L, EL, C> Stream for ParentExecutor<R, T, M, L, EL, C>
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

        futures::future::select_all(vec![
            this.timer.next().boxed_local(),
            this.mempool.next().boxed_local(),
            this.ledger.next().boxed_local(),
            this.router.next().boxed_local(),
        ])
        .map(|(event, _, _)| event)
        .poll_unpin(cx)
    }
}

impl<R, T, M, L, EL, C> ParentExecutor<R, T, M, L, EL, C> {
    pub fn ledger(&self) -> &L {
        &self.ledger
    }
}
