use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{FutureExt, Stream, StreamExt};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{
    CheckpointCommand, Command, ControlPanelCommand, LedgerCommand, LoopbackCommand, RouterCommand,
    StateRootHashCommand, StateSyncCommand, TimerCommand, TimestampCommand,
};

/// Single top-level executor for all other required by a node.
/// This executor will distribute commands to the appropriate sub-executor
/// and will poll them for events
pub struct ParentExecutor<R, T, L, C, S, IPC, CP, LO, TS, SS> {
    pub router: R,
    pub timer: T,
    pub ledger: L,
    pub checkpoint: C,
    pub state_root_hash: S,
    pub timestamp: TS,
    /// ipc is a Stream, not an executor
    pub ipc: IPC,
    pub control_panel: CP,
    pub loopback: LO,
    pub state_sync: SS,
    // if you add an executor here, you must add it to BOTH exec AND poll_next !
}

impl<RE, TE, LE, CE, SE, IPCE, CPE, LOE, TSE, SSE, E, OM, SCT: SignatureCollection> Executor
    for ParentExecutor<RE, TE, LE, CE, SE, IPCE, CPE, LOE, TSE, SSE>
where
    RE: Executor<Command = RouterCommand<SCT::NodeIdPubKey, OM>>,
    TE: Executor<Command = TimerCommand<E>>,

    CE: Executor<Command = CheckpointCommand<SCT>>,
    LE: Executor<Command = LedgerCommand<SCT>>,
    SE: Executor<Command = StateRootHashCommand>,
    CPE: Executor<Command = ControlPanelCommand<SCT>>,
    LOE: Executor<Command = LoopbackCommand<E>>,
    TSE: Executor<Command = TimestampCommand>,
    SSE: Executor<Command = StateSyncCommand<SCT::NodeIdPubKey>>,
{
    type Command = Command<E, OM, SCT>;

    fn exec(&mut self, commands: Vec<Command<E, OM, SCT>>) {
        let _exec_span = tracing::trace_span!("exec_span", num_cmds = commands.len()).entered();
        let (
            router_cmds,
            timer_cmds,
            ledger_cmds,
            checkpoint_cmds,
            state_root_hash_cmds,
            loopback_cmds,
            control_panel_cmds,
            timestamp_cmds,
            state_sync_cmds,
        ) = Command::split_commands(commands);

        self.router.exec(router_cmds);
        self.timer.exec(timer_cmds);
        self.ledger.exec(ledger_cmds);
        self.checkpoint.exec(checkpoint_cmds);
        self.state_root_hash.exec(state_root_hash_cmds);
        self.timestamp.exec(timestamp_cmds);
        self.loopback.exec(loopback_cmds);
        self.control_panel.exec(control_panel_cmds);
        self.state_sync.exec(state_sync_cmds);
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        ExecutorMetricsChain::default()
            .chain(self.router.metrics())
            .chain(self.timer.metrics())
            .chain(self.ledger.metrics())
            .chain(self.checkpoint.metrics())
            .chain(self.state_root_hash.metrics())
            .chain(self.timestamp.metrics())
            .chain(self.loopback.metrics())
            .chain(self.control_panel.metrics())
            .chain(self.state_sync.metrics())
    }
}

impl<E, R, T, L, C, S, IPC, CP, LO, TS, SS> Stream
    for ParentExecutor<R, T, L, C, S, IPC, CP, LO, TS, SS>
where
    R: Stream<Item = E> + Unpin,
    T: Stream<Item = E> + Unpin,
    L: Stream<Item = E> + Unpin,
    S: Stream<Item = E> + Unpin,
    IPC: Stream<Item = E> + Unpin,
    CP: Stream<Item = E> + Unpin,
    LO: Stream<Item = E> + Unpin,
    TS: Stream<Item = E> + Unpin,
    SS: Stream<Item = E> + Unpin,
    Self: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        futures::future::select_all(vec![
            this.timer.next().boxed_local(),
            this.control_panel.next().boxed_local(),
            this.ledger.next().boxed_local(),
            this.state_root_hash.next().boxed_local(),
            this.timestamp.next().boxed_local(),
            this.loopback.next().boxed_local(),
            this.router.next().boxed_local(), // TODO: consensus msgs should be prioritized
            this.ipc.next().boxed_local(),    // ingesting txs is lowest priority
            this.state_sync.next().boxed_local(),
        ])
        .map(|(event, _, _)| event)
        .poll_unpin(cx)
    }
}

impl<R, T, L, C, S, IPC, CP, LO, TS, SS> ParentExecutor<R, T, L, C, S, IPC, CP, LO, TS, SS> {
    pub fn ledger(&self) -> &L {
        &self.ledger
    }
}
