use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{Stream, StreamExt};
use ipc::StateSyncIpc;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, StateSyncCommand, StateSyncEvent, StateSyncNetworkMessage};
use monad_types::NodeId;

use crate::ffi::Target;

#[allow(dead_code, non_camel_case_types, non_upper_case_globals)]
pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/state_sync.rs"));
}

mod ffi;
mod ipc;
mod outbound_requests;

const GAUGE_STATESYNC_PROGRESS_ESTIMATE: &str = "monad.statesync.progress_estimate";
const GAUGE_STATESYNC_LAST_TARGET: &str = "monad.statesync.last_target";

pub struct StateSync<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
{
    db_paths: Vec<String>,
    genesis_path: String,
    state_sync_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    max_parallel_requests: usize,
    request_timeout: Duration,
    incoming_request_timeout: Duration,
    uds_path: String,

    state_sync: Option<ffi::StateSync<CertificateSignaturePubKey<ST>>>,

    // initialized once StartExecution command is executed
    execution_ipc: Option<StateSyncIpc<CertificateSignaturePubKey<ST>>>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> StateSync<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new(
        db_paths: Vec<String>,
        genesis_path: String,
        state_sync_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
        max_parallel_requests: usize,
        request_timeout: Duration,
        incoming_request_timeout: Duration,
        uds_path: String,
    ) -> Self {
        Self {
            db_paths,
            genesis_path,
            state_sync_peers,
            max_parallel_requests,
            request_timeout,
            incoming_request_timeout,
            uds_path,

            state_sync: None,
            execution_ipc: None,

            waker: None,
            metrics: Default::default(),
            _phantom: Default::default(),
        }
    }
}

impl<ST, SCT> Executor for StateSync<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = StateSyncCommand<CertificateSignaturePubKey<ST>>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                StateSyncCommand::RequestSync(state_root_info) => {
                    tracing::info!(?state_root_info, "initiating statesync");
                    assert!(self.state_sync.is_none());
                    self.state_sync = Some(ffi::StateSync::start(
                        &self.db_paths,
                        &self.genesis_path,
                        &self.state_sync_peers,
                        self.max_parallel_requests,
                        self.request_timeout,
                        Target {
                            n: state_root_info.seq_num,
                            state_root: state_root_info.state_root_hash.0 .0,
                        },
                    ));
                    self.metrics[GAUGE_STATESYNC_LAST_TARGET] = state_root_info.seq_num.0;
                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
                StateSyncCommand::Message((from, StateSyncNetworkMessage::Response(response))) => {
                    let Some(state_sync) = &mut self.state_sync else {
                        tracing::trace!(
                            ?from,
                            "dropping statesync response, already done sync'ing"
                        );
                        continue;
                    };
                    state_sync.handle_response(from, response);
                    if let Some(progress) = state_sync.progress_estimate() {
                        self.metrics[GAUGE_STATESYNC_PROGRESS_ESTIMATE] = progress.0;
                    }
                }
                StateSyncCommand::Message((from, StateSyncNetworkMessage::Request(request))) => {
                    if let Some(execution_ipc) = &mut self.execution_ipc {
                        if execution_ipc.request_tx.try_send((from, request)).is_err() {
                            tracing::warn!(
                                "dropping inbound statesync request, execution backlogged?"
                            )
                        }
                    }
                }
                StateSyncCommand::StartExecution => {
                    assert!(self.execution_ipc.is_none());
                    self.execution_ipc = Some(StateSyncIpc::new(
                        &self.uds_path,
                        self.incoming_request_timeout,
                    ));
                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT> Stream for StateSync<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Unpin,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if let Some(state_sync) = &mut this.state_sync {
            match state_sync.poll_next_unpin(cx) {
                Poll::Ready(Some((servicer, request))) => {
                    tracing::debug!(?request, ?servicer, "sending request");
                    return Poll::Ready(Some(MonadEvent::StateSyncEvent(
                        StateSyncEvent::Outbound(
                            servicer,
                            StateSyncNetworkMessage::Request(request),
                        ),
                    )));
                }
                Poll::Ready(None) => {
                    // done statesync
                    let target = state_sync.target();
                    this.state_sync = None;
                    self.metrics[GAUGE_STATESYNC_PROGRESS_ESTIMATE] = target.0;
                    return Poll::Ready(Some(MonadEvent::StateSyncEvent(
                        StateSyncEvent::DoneSync(target),
                    )));
                }
                Poll::Pending => {}
            }
        }

        if let Some(execution_ipc) = &mut this.execution_ipc {
            if let Poll::Ready(maybe_response) = execution_ipc.response_rx.poll_recv(cx) {
                let (to, response) = maybe_response.expect("did StateSyncIpc die?");
                tracing::debug!(
                    ?to,
                    ?response,
                    upserts_len = response.response.len(),
                    "sending response"
                );
                return Poll::Ready(Some(MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                    to,
                    StateSyncNetworkMessage::Response(response),
                ))));
            }
        }

        Poll::Pending
    }
}
