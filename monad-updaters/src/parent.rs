use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{FutureExt, Stream, StreamExt};
use monad_executor::Executor;
use monad_executor_glue::{
    CheckpointCommand, Command, ExecutionLedgerCommand, LedgerCommand, MempoolCommand,
    RouterCommand, StateRootHashCommand, TimerCommand,
};

/// Single top-level executor for all other required by a node.
/// This executor will distribute commands to the appropriate sub-executor
/// and will poll them for events
pub struct ParentExecutor<R, T, M, L, EL, C, S> {
    pub router: R,
    pub timer: T,
    pub mempool: M,
    pub ledger: L,
    pub execution_ledger: EL,
    pub checkpoint: C,
    pub state_root_hash: S,
    // if you add an executor here, you must add it to BOTH exec AND poll_next !
}

impl<RE, TE, ME, LE, EL, CE, SE, E, OM, B, C, S> Executor
    for ParentExecutor<RE, TE, ME, LE, EL, CE, SE>
where
    RE: Executor<Command = RouterCommand<OM>>,
    TE: Executor<Command = TimerCommand<E>>,

    CE: Executor<Command = CheckpointCommand<C>>,
    LE: Executor<Command = LedgerCommand<B, E>>,
    EL: Executor<Command = ExecutionLedgerCommand<S>>,
    ME: Executor<Command = MempoolCommand<S>>,
    SE: Executor<Command = StateRootHashCommand<B>>,
{
    type Command = Command<E, OM, B, C, S>;

    fn replay(&mut self, commands: Vec<Self::Command>) {
        let _exec_span = tracing::info_span!("replay_span", num_cmds = commands.len()).entered();
        let (
            router_cmds,
            timer_cmds,
            mempool_cmds,
            ledger_cmds,
            execution_ledger_cmds,
            checkpoint_cmds,
            _state_root_hash_cmds,
        ) = Command::split_commands(commands);

        self.router.replay(router_cmds);
        self.timer.replay(timer_cmds);
        self.mempool.replay(mempool_cmds);
        self.ledger.replay(ledger_cmds);
        self.execution_ledger.replay(execution_ledger_cmds);
        self.checkpoint.replay(checkpoint_cmds);
    }

    fn exec(&mut self, commands: Vec<Command<E, OM, B, C, S>>) {
        let _exec_span = tracing::info_span!("exec_span", num_cmds = commands.len()).entered();
        let (
            router_cmds,
            timer_cmds,
            mempool_cmds,
            ledger_cmds,
            execution_ledger_cmds,
            checkpoint_cmds,
            state_root_hash_cmds,
        ) = Command::split_commands(commands);

        self.router.exec(router_cmds);
        self.timer.exec(timer_cmds);
        self.mempool.exec(mempool_cmds);
        self.ledger.exec(ledger_cmds);
        self.execution_ledger.exec(execution_ledger_cmds);
        self.checkpoint.exec(checkpoint_cmds);
        self.state_root_hash.exec(state_root_hash_cmds);
    }
}

impl<E, R, T, M, L, EL, C, S> Stream for ParentExecutor<R, T, M, L, EL, C, S>
where
    R: Stream<Item = E> + Unpin,
    T: Stream<Item = E> + Unpin,
    M: Stream<Item = E> + Unpin,
    L: Stream<Item = E> + Unpin,
    S: Stream<Item = E> + Unpin,
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
            this.state_root_hash.next().boxed_local(),
        ])
        .map(|(event, _, _)| event)
        .poll_unpin(cx)
    }
}

impl<R, T, M, L, EL, C, S> ParentExecutor<R, T, M, L, EL, C, S> {
    pub fn ledger(&self) -> &L {
        &self.ledger
    }
}
