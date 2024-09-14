use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use ffi::SyncRequest;
use futures::{Stream, StreamExt};
use ipc::StateSyncIpc;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, StateSyncCommand, StateSyncEvent, StateSyncNetworkMessage};
use monad_types::NodeId;

use crate::ffi::Target;

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
    incoming_request_timeout: Duration,
    uds_path: String,

    mode: StateSyncMode<CertificateSignaturePubKey<ST>>,

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
            incoming_request_timeout,
            uds_path,

            mode: StateSyncMode::Sync(ffi::StateSync::start(
                &db_paths,
                &genesis_path,
                &state_sync_peers,
                max_parallel_requests,
                request_timeout,
            )),

            waker: None,
            metrics: Default::default(),
            _phantom: Default::default(),
        }
    }
}

enum StateSyncMode<PT: PubKey> {
    Sync(ffi::StateSync<PT>),
    /// transitions to Live once the StartExecution command is executed
    /// note that Live -> Sync is not a valid state transition
    Live(StateSyncIpc<PT>),
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
                    let statesync = match &mut self.mode {
                        StateSyncMode::Sync(sync) => sync,
                        StateSyncMode::Live(_) => {
                            unreachable!("Live -> Sync is not a valid state transition")
                        }
                    };
                    statesync.update_target(Target {
                        n: state_root_info.seq_num,
                        state_root: state_root_info.state_root_hash.0 .0,
                    });

                    self.metrics[GAUGE_STATESYNC_LAST_TARGET] = state_root_info.seq_num.0;
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

        match &mut this.mode {
            StateSyncMode::Sync(sync) => {
                if let Some(progress) = sync.progress_estimate() {
                    this.metrics[GAUGE_STATESYNC_PROGRESS_ESTIMATE] = progress.0;
                }

                if let Poll::Ready(event) = sync.poll_next_unpin(cx) {
                    let event = match event.expect("StateSyncMode::Sync event channel dropped") {
                        SyncRequest::Request((servicer, request)) => {
                            tracing::debug!(?request, ?servicer, "sending request");
                            StateSyncEvent::Outbound(
                                servicer,
                                StateSyncNetworkMessage::Request(request),
                            )
                        }
                        SyncRequest::DoneSync(target) => StateSyncEvent::DoneSync(target.n),
                    };
                    return Poll::Ready(Some(MonadEvent::StateSyncEvent(event)));
                }
            }
            StateSyncMode::Live(execution_ipc) => {
                if let Poll::Ready(maybe_response) = execution_ipc.response_rx.poll_recv(cx) {
                    let (to, response) = maybe_response.expect("did StateSyncIpc die?");
                    tracing::debug!(
                        ?to,
                        ?response,
                        upserts_len = response.response.len(),
                        "sending response"
                    );
                    return Poll::Ready(Some(MonadEvent::StateSyncEvent(
                        StateSyncEvent::Outbound(to, StateSyncNetworkMessage::Response(response)),
                    )));
                }
            }
        };

        Poll::Pending
    }
}
