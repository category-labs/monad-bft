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
    time::Instant,
};

use futures::{FutureExt, Stream, StreamExt};
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::block::BlockPolicy;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{
    Command, ConfigFileCommand, ConfigReloadCommand, ControlPanelCommand, LedgerCommand,
    LoopbackCommand, RouterCommand, StateSyncCommand, TimerCommand, TimestampCommand,
    TxPoolCommand, ValSetCommand,
};
use monad_state_backend::StateBackend;
use monad_types::ExecutionProtocol;
use monad_validator::signature_collection::SignatureCollection;

const GAUGE_PARENT_TOTAL_EXEC_US: &str = "monad.executor.parent.total_exec_us";
const GAUGE_LEDGER_TOTAL_EXEC_US: &str = "monad.executor.ledger.total_exec_us";
const GAUGE_CONFIG_FILE_TOTAL_EXEC_US: &str = "monad.executor.config_file.total_exec_us";
const GAUGE_TXPOOL_TOTAL_EXEC_US: &str = "monad.executor.txpool.total_exec_us";
const GAUGE_ROUTER_TOTAL_EXEC_US: &str = "monad.executor.router.total_exec_us";
const GAUGE_STATESYNC_TOTAL_EXEC_US: &str = "monad.executor.statesync.total_exec_us";

const GAUGE_PARENT_TOTAL_POLL_US: &str = "monad.executor.parent.total_poll_us";
const GAUGE_LEDGER_TOTAL_POLL_US: &str = "monad.executor.ledger.total_poll_us";
const GAUGE_TXPOOL_TOTAL_POLL_US: &str = "monad.executor.txpool.total_poll_us";
const GAUGE_ROUTER_TOTAL_POLL_US: &str = "monad.executor.router.total_poll_us";
const GAUGE_STATESYNC_TOTAL_POLL_US: &str = "monad.executor.statesync.total_poll_us";

/// Single top-level executor for all other required by a node.
/// This executor will distribute commands to the appropriate sub-executor
/// and will poll them for events
pub struct ParentExecutor<R, T, L, C, V, TS, TP, CP, LO, SS, CL> {
    pub metrics: ParentExecutorMetrics,

    pub router: R,
    pub timer: T,
    pub ledger: L,
    pub config_file: C,
    pub val_set: V,
    pub timestamp: TS,
    pub txpool: TP,
    pub control_panel: CP,
    pub loopback: LO,
    pub state_sync: SS,
    pub config_loader: CL,
    // if you add an executor here, you must add it to BOTH exec AND poll_next !
}

impl<RE, TE, LE, CE, SE, TSE, TPE, CPE, LOE, SSE, CLE, E, OM, ST, SCT, EPT, BPT, SBT, CCT, CRT>
    Executor for ParentExecutor<RE, TE, LE, CE, SE, TSE, TPE, CPE, LOE, SSE, CLE>
where
    RE: Executor<Command = RouterCommand<ST, OM>>,
    TE: Executor<Command = TimerCommand<E>>,
    LE: Executor<Command = LedgerCommand<ST, SCT, EPT>>,
    CE: Executor<Command = ConfigFileCommand<ST, SCT, EPT>>,
    SE: Executor<Command = ValSetCommand>,
    TSE: Executor<Command = TimestampCommand>,

    TPE: Executor<Command = TxPoolCommand<ST, SCT, EPT, BPT, SBT, CCT, CRT>>,
    CPE: Executor<Command = ControlPanelCommand<ST>>,
    LOE: Executor<Command = LoopbackCommand<E>>,
    SSE: Executor<Command = StateSyncCommand<ST, EPT>>,
    CLE: Executor<Command = ConfigReloadCommand>,

    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    type Command = Command<E, OM, ST, SCT, EPT, BPT, SBT, CCT, CRT>;

    fn exec(&mut self, commands: Vec<Command<E, OM, ST, SCT, EPT, BPT, SBT, CCT, CRT>>) {
        let _exec_span = tracing::trace_span!("exec_span", num_cmds = commands.len()).entered();
        let guard = ParentExecutorMetricsGuard::new(&mut self.metrics, GAUGE_PARENT_TOTAL_EXEC_US);
        let (
            router_cmds,
            timer_cmds,
            ledger_cmds,
            config_file_cmds,
            val_set_cmds,
            timestamp_cmds,
            txpool_cmds,
            control_panel_cmds,
            loopback_cmds,
            state_sync_cmds,
            config_reload_cmds,
        ) = Command::split_commands(commands);

        guard
            .metrics
            .record(GAUGE_ROUTER_TOTAL_EXEC_US, || self.router.exec(router_cmds));
        self.timer.exec(timer_cmds);
        guard
            .metrics
            .record(GAUGE_LEDGER_TOTAL_EXEC_US, || self.ledger.exec(ledger_cmds));
        guard.metrics.record(GAUGE_CONFIG_FILE_TOTAL_EXEC_US, || {
            self.config_file.exec(config_file_cmds)
        });
        self.val_set.exec(val_set_cmds);
        self.timestamp.exec(timestamp_cmds);
        guard
            .metrics
            .record(GAUGE_TXPOOL_TOTAL_EXEC_US, || self.txpool.exec(txpool_cmds));
        self.control_panel.exec(control_panel_cmds);
        self.loopback.exec(loopback_cmds);
        guard.metrics.record(GAUGE_STATESYNC_TOTAL_EXEC_US, || {
            self.state_sync.exec(state_sync_cmds)
        });
        self.config_loader.exec(config_reload_cmds);
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::default()
            .push(&self.metrics.0)
            .chain(self.router.metrics())
            .chain(self.timer.metrics())
            .chain(self.ledger.metrics())
            .chain(self.config_file.metrics())
            .chain(self.val_set.metrics())
            .chain(self.timestamp.metrics())
            .chain(self.txpool.metrics())
            .chain(self.control_panel.metrics())
            .chain(self.loopback.metrics())
            .chain(self.state_sync.metrics())
            .chain(self.config_loader.metrics())
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
        let guard = ParentExecutorMetricsGuard::new(&mut this.metrics, GAUGE_PARENT_TOTAL_POLL_US);

        if let Poll::Ready(Some(e)) = this.timer.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = this.control_panel.next().poll_unpin(cx) {
            return Poll::Ready(Some(e));
        }
        if let Poll::Ready(Some(e)) = guard.metrics.record(GAUGE_LEDGER_TOTAL_POLL_US, || {
            this.ledger.next().poll_unpin(cx)
        }) {
            return Poll::Ready(Some(e));
        }
        // TODO: ingesting txs should be deprioritized
        if let Poll::Ready(Some(e)) = guard.metrics.record(GAUGE_TXPOOL_TOTAL_POLL_US, || {
            this.txpool.next().poll_unpin(cx)
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

impl<R, T, L, C, S, TS, TP, CP, LO, SS, CL> ParentExecutor<R, T, L, C, S, TS, TP, CP, LO, SS, CL> {
    pub fn ledger(&self) -> &L {
        &self.ledger
    }
}

#[derive(Default)]
pub struct ParentExecutorMetrics(ExecutorMetrics);

impl ParentExecutorMetrics {
    fn record<T>(&mut self, executor_name: &'static str, f: impl FnOnce() -> T) -> T {
        let start = Instant::now();
        let e = f();
        self.0[executor_name] += start.elapsed().as_micros() as u64;
        e
    }
}

pub struct ParentExecutorMetricsGuard<'a> {
    metrics: &'a mut ParentExecutorMetrics,
    guard_metric: &'static str,
    start: Instant,
}

impl<'a> ParentExecutorMetricsGuard<'a> {
    fn new(metrics: &'a mut ParentExecutorMetrics, guard_metric: &'static str) -> Self {
        Self {
            metrics,
            guard_metric,
            start: Instant::now(),
        }
    }
}

impl<'a> Drop for ParentExecutorMetricsGuard<'a> {
    fn drop(&mut self) {
        self.metrics.0[self.guard_metric] += self.start.elapsed().as_micros() as u64;
    }
}
