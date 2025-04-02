use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{Stream, StreamExt};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::Executor;
use monad_executor_glue::{MonadEvent, StateSyncCommand, StateSyncEvent, StateSyncNetworkMessage};
use monad_metrics::{Gauge, MetricsPolicy};
use monad_types::{NodeId, SeqNum};

pub use self::metrics::StateSyncExecutorMetrics;
use self::{ffi::SyncRequest, ipc::StateSyncIpc};

#[allow(dead_code, non_camel_case_types, non_upper_case_globals)]
pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/state_sync.rs"));
}

mod ffi;
mod ipc;
mod metrics;
mod outbound_requests;

pub struct StateSync<ST, SCT, MP>
where
    ST: CertificateSignatureRecoverable,
    MP: MetricsPolicy,
{
    incoming_request_timeout: Duration,
    uds_path: String,

    mode: StateSyncMode<CertificateSignaturePubKey<ST>>,

    waker: Option<Waker>,
    metrics: StateSyncExecutorMetrics<MP>,
    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT, MP> StateSync<ST, SCT, MP>
where
    ST: CertificateSignatureRecoverable,
    MP: MetricsPolicy,
{
    pub fn new(
        db_paths: Vec<String>,
        genesis_path: String,
        sq_thread_cpu: Option<u32>,
        state_sync_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
        max_parallel_requests: usize,
        request_timeout: Duration,
        incoming_request_timeout: Duration,
        uds_path: String,
        metrics: StateSyncExecutorMetrics<MP>,
    ) -> Self {
        Self {
            incoming_request_timeout,
            uds_path,

            mode: StateSyncMode::Sync(ffi::StateSync::start(
                &db_paths,
                &genesis_path,
                sq_thread_cpu,
                &state_sync_peers,
                max_parallel_requests,
                request_timeout,
            )),

            waker: None,
            metrics,
            _phantom: Default::default(),
        }
    }

    fn update_syncing_metrics(&mut self) {
        self.metrics.syncing.set(match &self.mode {
            StateSyncMode::Sync(_) => 1,
            StateSyncMode::Live(_) => 0,
        });
    }
}

enum StateSyncMode<PT: PubKey> {
    Sync(ffi::StateSync<PT>),
    /// transitions to Live once the StartExecution command is executed
    /// note that Live -> Sync is not a valid state transition
    Live(StateSyncIpc<PT>),
}

impl<ST, SCT, MP> Executor<MP> for StateSync<ST, SCT, MP>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    MP: MetricsPolicy,
{
    type Command = StateSyncCommand<ST, EthExecutionProtocol>;
    type Metrics = StateSyncExecutorMetrics<MP>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                StateSyncCommand::RequestSync(header) => {
                    let statesync = match &mut self.mode {
                        StateSyncMode::Sync(sync) => sync,
                        StateSyncMode::Live(_) => {
                            unreachable!("Live -> Sync is not a valid state transition")
                        }
                    };
                    self.metrics.last_target.set(header.0.number);
                    statesync.update_target(header.0);
                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
                StateSyncCommand::Message((from, StateSyncNetworkMessage::Response(response))) => {
                    let statesync = match &mut self.mode {
                        StateSyncMode::Sync(sync) => sync,
                        StateSyncMode::Live(_) => {
                            tracing::trace!(
                                ?from,
                                "dropping statesync response, already done syncing"
                            );
                            continue;
                        }
                    };
                    statesync.handle_response(from, response);
                }
                StateSyncCommand::Message((
                    from,
                    StateSyncNetworkMessage::BadVersion(bad_version),
                )) => {
                    let statesync = match &mut self.mode {
                        StateSyncMode::Sync(sync) => sync,
                        StateSyncMode::Live(_) => {
                            tracing::trace!(
                                ?from,
                                "dropping statesync bad version, already done syncing"
                            );
                            continue;
                        }
                    };
                    statesync.handle_bad_version(from, bad_version);
                }
                StateSyncCommand::Message((from, StateSyncNetworkMessage::Request(request))) => {
                    let execution_ipc = match &mut self.mode {
                        StateSyncMode::Sync(_) => {
                            tracing::trace!(?from, "dropping statesync request, still syncing");
                            continue;
                        }
                        StateSyncMode::Live(live) => live,
                    };
                    if execution_ipc.request_tx.try_send((from, request)).is_err() {
                        tracing::warn!("dropping inbound statesync request, execution backlogged?")
                    }
                }
                StateSyncCommand::StartExecution => {
                    let valid_transition = match self.mode {
                        StateSyncMode::Sync(_) => true,
                        StateSyncMode::Live(_) => false,
                    };
                    assert!(valid_transition);
                    self.mode = StateSyncMode::Live(StateSyncIpc::new(
                        &self.uds_path,
                        self.incoming_request_timeout,
                    ));
                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
            }
        }

        self.update_syncing_metrics();
    }

    fn metrics(&self) -> &Self::Metrics {
        &self.metrics
    }
}

impl<ST, SCT, MP> Stream for StateSync<ST, SCT, MP>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Unpin,
    MP: MetricsPolicy,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        match &mut this.mode {
            StateSyncMode::Sync(sync) => {
                if let Some(progress) = sync.progress_estimate() {
                    this.metrics.progress_estimate.set(progress.0);
                }

                if let Poll::Ready(event) = sync.poll_next_unpin(cx) {
                    let event = match event.expect("StateSyncMode::Sync event channel dropped") {
                        SyncRequest::Request((servicer, request)) => {
                            tracing::debug!(?request, ?servicer, "sending request");
                            StateSyncEvent::Outbound(
                                servicer,
                                StateSyncNetworkMessage::Request(request),
                                None, // we don't care about completions for requests
                            )
                        }
                        SyncRequest::DoneSync(target) => {
                            StateSyncEvent::DoneSync(SeqNum(target.number))
                        }
                    };
                    return Poll::Ready(Some(MonadEvent::StateSyncEvent(event)));
                }
            }
            StateSyncMode::Live(execution_ipc) => {
                if let Poll::Ready(maybe_response) = execution_ipc.response_rx.poll_recv(cx) {
                    let (to, message, completion) = maybe_response.expect("did StateSyncIpc die?");
                    tracing::debug!(
                        ?to,
                        ?message,
                        upserts_len = match &message {
                            StateSyncNetworkMessage::Response(response) => response.response.len(),
                            _ => 0,
                        },
                        "sending response"
                    );
                    return Poll::Ready(Some(MonadEvent::StateSyncEvent(
                        StateSyncEvent::Outbound(to, message, Some(completion)),
                    )));
                }
            }
        };

        Poll::Pending
    }
}
