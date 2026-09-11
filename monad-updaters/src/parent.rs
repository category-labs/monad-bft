// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    fmt::Debug,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::{FutureExt, Stream, StreamExt};
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::block::BlockPolicy;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_execution_state_read::ExecutionStateRead;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{
    Command, ConfigFileCommand, ConfigReloadCommand, ControlPanelCommand, LedgerCommand,
    LoopbackCommand, RouterCommand, StateSyncCommand, TimerCommand, TimestampCommand,
    TxPoolCommand, ValSetCommand,
};
use monad_types::ExecutionProtocol;
use monad_validator::signature_collection::SignatureCollection;

monad_executor::metric_consts! {
    GAUGE_PARENT_TOTAL_EXEC_US {
        name: "monad.executor.parent.total_exec_us",
        help: "Total parent executor execution time in microseconds",
    }
    GAUGE_LEDGER_TOTAL_EXEC_US {
        name: "monad.executor.ledger.total_exec_us",
        help: "Total ledger executor execution time in microseconds",
    }
    GAUGE_CONFIG_FILE_TOTAL_EXEC_US {
        name: "monad.executor.config_file.total_exec_us",
        help: "Total config file executor execution time in microseconds",
    }
    GAUGE_TXPOOL_TOTAL_EXEC_US {
        name: "monad.executor.txpool.total_exec_us",
        help: "Total TxPool executor execution time in microseconds",
    }
    GAUGE_ROUTER_TOTAL_EXEC_US {
        name: "monad.executor.router.total_exec_us",
        help: "Total router executor execution time in microseconds",
    }
    GAUGE_STATESYNC_TOTAL_EXEC_US {
        name: "monad.executor.statesync.total_exec_us",
        help: "Total StateSync executor execution time in microseconds",
    }
    GAUGE_PARENT_TOTAL_POLL_US {
        name: "monad.executor.parent.total_poll_us",
        help: "Total parent executor poll time in microseconds",
    }
    GAUGE_LEDGER_TOTAL_POLL_US {
        name: "monad.executor.ledger.total_poll_us",
        help: "Total ledger executor poll time in microseconds",
    }
    GAUGE_TXPOOL_TOTAL_POLL_US {
        name: "monad.executor.txpool.total_poll_us",
        help: "Total TxPool executor poll time in microseconds",
    }
    GAUGE_ROUTER_TOTAL_POLL_US {
        name: "monad.executor.router.total_poll_us",
        help: "Total router executor poll time in microseconds",
    }
    GAUGE_STATESYNC_TOTAL_POLL_US {
        name: "monad.executor.statesync.total_poll_us",
        help: "Total StateSync executor poll time in microseconds",
    }
}

fn init_executor_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(&[
        GAUGE_PARENT_TOTAL_EXEC_US,
        GAUGE_LEDGER_TOTAL_EXEC_US,
        GAUGE_CONFIG_FILE_TOTAL_EXEC_US,
        GAUGE_TXPOOL_TOTAL_EXEC_US,
        GAUGE_ROUTER_TOTAL_EXEC_US,
        GAUGE_STATESYNC_TOTAL_EXEC_US,
        GAUGE_PARENT_TOTAL_POLL_US,
        GAUGE_LEDGER_TOTAL_POLL_US,
        GAUGE_TXPOOL_TOTAL_POLL_US,
        GAUGE_ROUTER_TOTAL_POLL_US,
        GAUGE_STATESYNC_TOTAL_POLL_US,
    ])
}

pub struct CoreExecutor<T, L, C, V, TS, LO> {
    metrics: ParentExecutorMetrics,
    pub timer: T,
    pub ledger: L,
    pub config_file: C,
    pub val_set: V,
    pub timestamp: TS,
    pub loopback: LO,
}

impl<T, L, C, V, TS, LO> CoreExecutor<T, L, C, V, TS, LO> {
    pub fn new(
        timer: T,
        ledger: L,
        config_file: C,
        val_set: V,
        timestamp: TS,
        loopback: LO,
    ) -> Self {
        Self {
            metrics: Default::default(),
            timer,
            ledger,
            config_file,
            val_set,
            timestamp,
            loopback,
        }
    }

    pub fn exec<E, ST, SCT, EPT>(
        &mut self,
        timer: Vec<TimerCommand<E>>,
        ledger: Vec<LedgerCommand<ST, SCT, EPT>>,
        config_file: Vec<ConfigFileCommand<ST, SCT, EPT>>,
        val_set: Vec<ValSetCommand>,
        timestamp: Vec<TimestampCommand>,
        loopback: Vec<LoopbackCommand<E>>,
    ) where
        T: Executor<Command = TimerCommand<E>>,
        L: Executor<Command = LedgerCommand<ST, SCT, EPT>>,
        C: Executor<Command = ConfigFileCommand<ST, SCT, EPT>>,
        V: Executor<Command = ValSetCommand>,
        TS: Executor<Command = TimestampCommand>,
        LO: Executor<Command = LoopbackCommand<E>>,
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
    {
        let guard = ParentExecutorMetricsGuard::new(&mut self.metrics, GAUGE_PARENT_TOTAL_EXEC_US);
        self.timer.exec(timer);
        guard
            .metrics
            .record(GAUGE_LEDGER_TOTAL_EXEC_US, || self.ledger.exec(ledger));
        guard.metrics.record(GAUGE_CONFIG_FILE_TOTAL_EXEC_US, || {
            self.config_file.exec(config_file)
        });
        self.val_set.exec(val_set);
        self.timestamp.exec(timestamp);
        self.loopback.exec(loopback);
    }

    pub fn ledger(&self) -> &L {
        &self.ledger
    }
}

pub struct ExternalExecutor<R, TP, CP, SS, CL> {
    metrics: ParentExecutorMetrics,
    pub router: R,
    pub txpool: TP,
    pub control_panel: CP,
    pub state_sync: SS,
    pub config_loader: CL,
}

impl<R, TP, CP, SS, CL> ExternalExecutor<R, TP, CP, SS, CL> {
    pub fn new(
        router: R,
        txpool: TP,
        control_panel: CP,
        state_sync: SS,
        config_loader: CL,
    ) -> Self {
        Self {
            metrics: Default::default(),
            router,
            txpool,
            control_panel,
            state_sync,
            config_loader,
        }
    }

    pub fn exec<OM, ST, SCT, EPT, BPT, ESRT, CCT, CRT>(
        &mut self,
        router: Vec<RouterCommand<ST, OM>>,
        txpool: Vec<TxPoolCommand<ST, SCT, EPT, BPT, ESRT, CCT, CRT>>,
        control_panel: Vec<ControlPanelCommand<ST>>,
        state_sync: Vec<StateSyncCommand<ST, EPT>>,
        config_reload: Vec<ConfigReloadCommand>,
    ) where
        R: Executor<Command = RouterCommand<ST, OM>>,
        TP: Executor<Command = TxPoolCommand<ST, SCT, EPT, BPT, ESRT, CCT, CRT>>,
        CP: Executor<Command = ControlPanelCommand<ST>>,
        SS: Executor<Command = StateSyncCommand<ST, EPT>>,
        CL: Executor<Command = ConfigReloadCommand>,
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
        BPT: BlockPolicy<ST, SCT, EPT, ESRT, CCT, CRT>,
        ESRT: ExecutionStateRead<ST, SCT>,
        CCT: ChainConfig<CRT>,
        CRT: ChainRevision,
    {
        let guard = ParentExecutorMetricsGuard::new(&mut self.metrics, GAUGE_PARENT_TOTAL_EXEC_US);
        guard
            .metrics
            .record(GAUGE_ROUTER_TOTAL_EXEC_US, || self.router.exec(router));
        guard
            .metrics
            .record(GAUGE_TXPOOL_TOTAL_EXEC_US, || self.txpool.exec(txpool));
        self.control_panel.exec(control_panel);
        guard.metrics.record(GAUGE_STATESYNC_TOTAL_EXEC_US, || {
            self.state_sync.exec(state_sync)
        });
        self.config_loader.exec(config_reload);
    }
}

/// Single top-level executor for all other required by a node.
/// This executor will distribute commands to the appropriate sub-executor
/// and will poll them for events
pub struct ParentExecutor<R, T, L, C, V, TS, TP, CP, LO, SS, CL> {
    pub core: CoreExecutor<T, L, C, V, TS, LO>,
    pub external: ExternalExecutor<R, TP, CP, SS, CL>,
}

impl<R, T, L, C, V, TS, TP, CP, LO, SS, CL> ParentExecutor<R, T, L, C, V, TS, TP, CP, LO, SS, CL> {
    pub fn new(
        mut core: CoreExecutor<T, L, C, V, TS, LO>,
        mut external: ExternalExecutor<R, TP, CP, SS, CL>,
    ) -> Self {
        let metrics = ParentExecutorMetrics::default();
        core.metrics = metrics.clone();
        external.metrics = metrics;
        Self { core, external }
    }

    pub fn ledger(&self) -> &L {
        self.core.ledger()
    }
}

impl<
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
        ESRT,
        CCT,
        CRT,
    > Executor for ParentExecutor<RE, TE, LE, CE, SE, TSE, TPE, CPE, LOE, SSE, CLE>
where
    RE: Executor<Command = RouterCommand<ST, OM>>,
    TE: Executor<Command = TimerCommand<E>>,
    LE: Executor<Command = LedgerCommand<ST, SCT, EPT>>,
    CE: Executor<Command = ConfigFileCommand<ST, SCT, EPT>>,
    SE: Executor<Command = ValSetCommand>,
    TSE: Executor<Command = TimestampCommand>,

    TPE: Executor<Command = TxPoolCommand<ST, SCT, EPT, BPT, ESRT, CCT, CRT>>,
    CPE: Executor<Command = ControlPanelCommand<ST>>,
    LOE: Executor<Command = LoopbackCommand<E>>,
    SSE: Executor<Command = StateSyncCommand<ST, EPT>>,
    CLE: Executor<Command = ConfigReloadCommand>,

    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, ESRT, CCT, CRT>,
    ESRT: ExecutionStateRead<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    type Command = Command<E, OM, ST, SCT, EPT, BPT, ESRT, CCT, CRT>;

    fn exec(&mut self, commands: Vec<Command<E, OM, ST, SCT, EPT, BPT, ESRT, CCT, CRT>>) {
        let _exec_span = tracing::trace_span!("exec_span", num_cmds = commands.len()).entered();
        let (
            router,
            timer,
            ledger,
            config_file,
            val_set,
            timestamp,
            txpool,
            control_panel,
            loopback,
            state_sync,
            config_reload,
        ) = Command::split_commands(commands);
        let guard =
            ParentExecutorMetricsGuard::new(&mut self.core.metrics, GAUGE_PARENT_TOTAL_EXEC_US);

        guard.metrics.record(GAUGE_ROUTER_TOTAL_EXEC_US, || {
            self.external.router.exec(router)
        });
        self.core.timer.exec(timer);
        guard
            .metrics
            .record(GAUGE_LEDGER_TOTAL_EXEC_US, || self.core.ledger.exec(ledger));
        guard.metrics.record(GAUGE_CONFIG_FILE_TOTAL_EXEC_US, || {
            self.core.config_file.exec(config_file)
        });
        self.core.val_set.exec(val_set);
        self.core.timestamp.exec(timestamp);
        guard.metrics.record(GAUGE_TXPOOL_TOTAL_EXEC_US, || {
            self.external.txpool.exec(txpool)
        });
        self.external.control_panel.exec(control_panel);
        self.core.loopback.exec(loopback);
        guard.metrics.record(GAUGE_STATESYNC_TOTAL_EXEC_US, || {
            self.external.state_sync.exec(state_sync)
        });
        self.external.config_loader.exec(config_reload);
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::default()
            .push(&self.core.metrics.0)
            .chain(self.external.router.metrics())
            .chain(self.core.timer.metrics())
            .chain(self.core.ledger.metrics())
            .chain(self.core.config_file.metrics())
            .chain(self.core.val_set.metrics())
            .chain(self.core.timestamp.metrics())
            .chain(self.external.txpool.metrics())
            .chain(self.external.control_panel.metrics())
            .chain(self.core.loopback.metrics())
            .chain(self.external.state_sync.metrics())
            .chain(self.external.config_loader.metrics())
    }
}

impl<E, R, T, L, C, S, TS, TP, CP, LO, SS, CL> Stream
    for ParentExecutor<R, T, L, C, S, TS, TP, CP, LO, SS, CL>
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
    E: Debug,

    Self: Unpin,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();
        let guard =
            ParentExecutorMetricsGuard::new(&mut this.core.metrics, GAUGE_PARENT_TOTAL_POLL_US);

        if let Poll::Ready(Some(e)) = this.core.timer.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = this.external.control_panel.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = guard.metrics.record(GAUGE_LEDGER_TOTAL_POLL_US, || {
            this.core.ledger.next().poll_unpin(cx)
        }) {
            return Poll::Ready(Some(e));
        }
        // TODO: ingesting txs should be deprioritized
        if let Poll::Ready(Some(e)) = guard.metrics.record(GAUGE_TXPOOL_TOTAL_POLL_US, || {
            this.external.txpool.next().poll_unpin(cx)
        }) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = this.core.val_set.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = this.core.timestamp.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = this.core.loopback.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }
        // TODO: consensus msgs should be prioritized
        if let Poll::Ready(Some(e)) = guard.metrics.record(GAUGE_ROUTER_TOTAL_POLL_US, || {
            this.external.router.next().poll_unpin(cx)
        }) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = guard.metrics.record(GAUGE_STATESYNC_TOTAL_POLL_US, || {
            this.external.state_sync.next().poll_unpin(cx)
        }) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = this.external.config_loader.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }

        Poll::Pending
    }
}

impl<E, T, L, C, V, TS, LO> Stream for CoreExecutor<T, L, C, V, TS, LO>
where
    T: Stream<Item = E> + Unpin,
    L: Stream<Item = E> + Unpin,
    V: Stream<Item = E> + Unpin,
    TS: Stream<Item = E> + Unpin,
    LO: Stream<Item = E> + Unpin,
    E: Debug,
    Self: Unpin,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();
        let guard = ParentExecutorMetricsGuard::new(&mut this.metrics, GAUGE_PARENT_TOTAL_POLL_US);

        if let Poll::Ready(Some(e)) = this.timer.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = guard.metrics.record(GAUGE_LEDGER_TOTAL_POLL_US, || {
            this.ledger.next().poll_unpin(cx)
        }) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = this.val_set.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = this.timestamp.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = this.loopback.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }

        Poll::Pending
    }
}

impl<E, R, TP, CP, SS, CL> Stream for ExternalExecutor<R, TP, CP, SS, CL>
where
    R: Stream<Item = E> + Unpin,
    TP: Stream<Item = E> + Unpin,
    CP: Stream<Item = E> + Unpin,
    SS: Stream<Item = E> + Unpin,
    CL: Stream<Item = E> + Unpin,
    E: Debug,
    Self: Unpin,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();
        let guard = ParentExecutorMetricsGuard::new(&mut this.metrics, GAUGE_PARENT_TOTAL_POLL_US);

        if let Poll::Ready(Some(e)) = this.control_panel.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }
        // TODO: ingesting txs should be deprioritized
        if let Poll::Ready(Some(e)) = guard.metrics.record(GAUGE_TXPOOL_TOTAL_POLL_US, || {
            this.txpool.next().poll_unpin(cx)
        }) {
            return Poll::Ready(Some(e));
        }
        // TODO: consensus msgs should be prioritized
        if let Poll::Ready(Some(e)) = guard.metrics.record(GAUGE_ROUTER_TOTAL_POLL_US, || {
            this.router.next().poll_unpin(cx)
        }) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = guard.metrics.record(GAUGE_STATESYNC_TOTAL_POLL_US, || {
            this.state_sync.next().poll_unpin(cx)
        }) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = this.config_loader.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }

        Poll::Pending
    }
}

#[derive(Clone)]
pub struct ParentExecutorMetrics(ExecutorMetrics);

impl Default for ParentExecutorMetrics {
    fn default() -> Self {
        Self(init_executor_metrics())
    }
}

fn duration_micros_u64(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

impl ParentExecutorMetrics {
    fn record<T>(
        &mut self,
        metric: &'static monad_executor::MetricDef,
        f: impl FnOnce() -> T,
    ) -> T {
        let start = Instant::now();
        let e = f();
        self.0
            .gauge(metric)
            .add(duration_micros_u64(start.elapsed()));
        e
    }
}

pub struct ParentExecutorMetricsGuard<'a> {
    metrics: &'a mut ParentExecutorMetrics,
    guard_metric: &'static monad_executor::MetricDef,
    start: Instant,
}

impl<'a> ParentExecutorMetricsGuard<'a> {
    fn new(
        metrics: &'a mut ParentExecutorMetrics,
        guard_metric: &'static monad_executor::MetricDef,
    ) -> Self {
        Self {
            metrics,
            guard_metric,
            start: Instant::now(),
        }
    }
}

impl<'a> Drop for ParentExecutorMetricsGuard<'a> {
    fn drop(&mut self) {
        self.metrics
            .0
            .gauge(self.guard_metric)
            .add(duration_micros_u64(self.start.elapsed()));
    }
}
