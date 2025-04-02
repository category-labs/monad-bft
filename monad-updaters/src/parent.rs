use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{FutureExt, Stream, StreamExt};
use monad_consensus_types::{block::BlockPolicy, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::Executor;
use monad_executor_glue::{
    CheckpointCommand, Command, ConfigReloadCommand, ControlPanelCommand, LedgerCommand,
    LoopbackCommand, RouterCommand, StateRootHashCommand, StateSyncCommand, TimerCommand,
    TimestampCommand, TxPoolCommand,
};
use monad_metrics::MetricsPolicy;
use monad_state_backend::StateBackend;
use monad_types::ExecutionProtocol;

// TODO(andr-dev): Move MP forward here \/

/// Single top-level executor for all other required by a node.
/// This executor will distribute commands to the appropriate sub-executor
/// and will poll them for events
pub struct ParentExecutor<R, T, L, C, S, TS, TP, CP, LO, SS, CL, E, OM, ST, SCT, EPT, BPT, SBT, MP>
{
    pub router: R,
    pub timer: T,
    pub ledger: L,
    pub checkpoint: C,
    pub state_root_hash: S,
    pub timestamp: TS,
    pub txpool: TP,
    pub control_panel: CP,
    pub loopback: LO,
    pub state_sync: SS,
    pub config_loader: CL,
    // if you add an executor here, you must add it to:
    //   1) ParentExecutorMetrics
    //   2) exec
    //   3) poll_next
    pub _phantom: PhantomData<(E, OM, ST, SCT, EPT, BPT, SBT, MP)>,
}

pub struct ParentExecutorMetrics<'a, RM, TM, LM, CM, SM, TSM, TPM, CPM, LOM, SSM, CLM> {
    pub router: &'a RM,
    pub timer: &'a TM,
    pub ledger: &'a LM,
    pub checkpoint: &'a CM,
    pub state_root_hash: &'a SM,
    pub timestamp: &'a TSM,

    pub txpool: &'a TPM,
    pub control_panel: &'a CPM,
    pub loopback: &'a LOM,
    pub state_sync: &'a SSM,
    pub config_loader: &'a CLM,
}

impl<RE, TE, LE, CE, SE, TSE, TPE, CPE, LOE, SSE, CLE, E, OM, ST, SCT, EPT, BPT, SBT, MP>
    ParentExecutor<
        RE,
        TE,
        LE,
        CE,
        SE,
        TSE,
        TPE,
        CPE,
        LOE,
        SSE,
        CLE,
        E,
        OM,
        ST,
        SCT,
        EPT,
        BPT,
        SBT,
        MP,
    >
where
    RE: Executor<MP, Command = RouterCommand<SCT::NodeIdPubKey, OM>>,
    TE: Executor<MP, Command = TimerCommand<E>>,
    LE: Executor<MP, Command = LedgerCommand<ST, SCT, EPT>>,
    CE: Executor<MP, Command = CheckpointCommand<SCT>>,
    SE: Executor<MP, Command = StateRootHashCommand<SCT>>,
    TSE: Executor<MP, Command = TimestampCommand>,

    TPE: Executor<MP, Command = TxPoolCommand<ST, SCT, EPT, BPT, SBT>>,
    CPE: Executor<MP, Command = ControlPanelCommand<SCT>>,
    LOE: Executor<MP, Command = LoopbackCommand<E>>,
    SSE: Executor<MP, Command = StateSyncCommand<ST, EPT>>,
    CLE: Executor<MP, Command = ConfigReloadCommand>,

    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    MP: MetricsPolicy,
{
    pub fn exec(&mut self, commands: Vec<Command<E, OM, ST, SCT, EPT, BPT, SBT>>) {
        let _exec_span = tracing::trace_span!("exec_span", num_cmds = commands.len()).entered();
        let (
            router_cmds,
            timer_cmds,
            ledger_cmds,
            checkpoint_cmds,
            state_root_hash_cmds,
            timestamp_cmds,
            txpool_cmds,
            control_panel_cmds,
            loopback_cmds,
            state_sync_cmds,
            config_reload_cmds,
        ) = Command::split_commands(commands);

        self.router.exec(router_cmds);
        self.timer.exec(timer_cmds);
        self.ledger.exec(ledger_cmds);
        self.checkpoint.exec(checkpoint_cmds);
        self.state_root_hash.exec(state_root_hash_cmds);
        self.timestamp.exec(timestamp_cmds);
        self.txpool.exec(txpool_cmds);
        self.control_panel.exec(control_panel_cmds);
        self.loopback.exec(loopback_cmds);
        self.state_sync.exec(state_sync_cmds);
        self.config_loader.exec(config_reload_cmds);
    }

    pub fn metrics(
        &self,
    ) -> ParentExecutorMetrics<
        '_,
        RE::Metrics,
        TE::Metrics,
        LE::Metrics,
        CE::Metrics,
        SE::Metrics,
        TSE::Metrics,
        TPE::Metrics,
        CPE::Metrics,
        LOE::Metrics,
        SSE::Metrics,
        CLE::Metrics,
    > {
        ParentExecutorMetrics {
            router: self.router.metrics(),
            timer: self.timer.metrics(),
            ledger: self.ledger.metrics(),
            checkpoint: self.checkpoint.metrics(),
            state_root_hash: self.state_root_hash.metrics(),
            timestamp: self.timestamp.metrics(),
            txpool: self.txpool.metrics(),
            control_panel: self.control_panel.metrics(),
            loopback: self.loopback.metrics(),
            state_sync: self.state_sync.metrics(),
            config_loader: self.config_loader.metrics(),
        }
    }
}

impl<R, T, L, C, S, TS, TP, CP, LO, SS, CL, E, OM, ST, SCT, EPT, BPT, SBT, MP> Stream
    for ParentExecutor<R, T, L, C, S, TS, TP, CP, LO, SS, CL, E, OM, ST, SCT, EPT, BPT, SBT, MP>
where
    R: Stream<Item = E> + Unpin,
    T: Stream<Item = E> + Unpin,
    L: Stream<Item = E> + Unpin,
    S: Stream<Item = E> + Unpin,
    TS: Stream<Item = E> + Unpin,

    TP: Stream<Item = E> + Unpin,
    CP: Stream<Item = E> + Unpin,
    LO: Stream<Item = E> + Unpin,
    SS: Stream<Item = E> + Unpin,
    CL: Stream<Item = E> + Unpin,

    Self: Unpin,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        futures::future::select_all(vec![
            this.timer.next().boxed_local(),
            this.control_panel.next().boxed_local(),
            this.ledger.next().boxed_local(),
            this.txpool.next().boxed_local(), // TODO: ingesting txs should be deprioritized
            this.state_root_hash.next().boxed_local(),
            this.timestamp.next().boxed_local(),
            this.loopback.next().boxed_local(),
            this.router.next().boxed_local(), // TODO: consensus msgs should be prioritized
            this.state_sync.next().boxed_local(),
            this.config_loader.next().boxed_local(),
        ])
        .map(|(event, _, _)| event)
        .poll_unpin(cx)
    }
}

impl<R, T, L, C, S, TS, TP, CP, LO, SS, CL, E, OM, ST, SCT, EPT, BPT, SBT, MP>
    ParentExecutor<R, T, L, C, S, TS, TP, CP, LO, SS, CL, E, OM, ST, SCT, EPT, BPT, SBT, MP>
{
    pub fn ledger(&self) -> &L {
        &self.ledger
    }
}
