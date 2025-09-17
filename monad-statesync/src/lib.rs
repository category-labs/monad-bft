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
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use ffi::SyncRequest;
use futures::{Stream, StreamExt};
use ipc::StateSyncIpc;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, StateSyncCommand, StateSyncEvent, StateSyncNetworkMessage};
use monad_types::{NodeId, SeqNum};
use monad_validator::signature_collection::SignatureCollection;

#[allow(dead_code, non_camel_case_types, non_upper_case_globals)]
pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/state_sync.rs"));
}

mod ffi;
mod ipc;
mod outbound_requests;

const GAUGE_STATESYNC_SYNCING: &str = "monad.statesync.syncing";
const GAUGE_STATESYNC_PROGRESS_ESTIMATE: &str = "monad.statesync.progress_estimate";
const GAUGE_STATESYNC_LAST_TARGET: &str = "monad.statesync.last_target";
const GAUGE_STATESYNC_SERVER_PENDING_REQUESTS: &str = "monad.statesync.server_pending_requests";
const GAUGE_STATESYNC_SERVER_NUM_SYNCDONE_SUCCESS: &str =
    "monad.statesync.server_num_syncdone_success";
const GAUGE_STATESYNC_SERVER_NUM_SYNCDONE_FAILED: &str =
    "monad.statesync.server_num_syncdone_failed";
const GAUGE_STATESYNC_SERVER_TOTAL_SERVICE_TIME_US: &str =
    "monad.statesync.server_total_service_time_us";

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
        sq_thread_cpu: Option<u32>,
        state_sync_init_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
        max_parallel_requests: usize,
        request_timeout: Duration,
        incoming_request_timeout: Duration,
        uds_path: String,
    ) -> Self {
        monad_cxx::init_cxx_logging(tracing::Level::WARN);

        let mut this = Self {
            incoming_request_timeout,
            uds_path,

            mode: StateSyncMode::Sync(ffi::StateSync::start(
                &db_paths,
                sq_thread_cpu,
                &state_sync_init_peers,
                max_parallel_requests,
                request_timeout,
            )),

            waker: None,
            metrics: Default::default(),
            _phantom: Default::default(),
        };

        this.update_syncing_metrics();

        this
    }

    fn update_syncing_metrics(&mut self) {
        self.metrics[GAUGE_STATESYNC_SYNCING] = match &self.mode {
            StateSyncMode::Sync(_) => 1,
            StateSyncMode::Live(_) => 0,
        };
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
    type Command = StateSyncCommand<ST, EthExecutionProtocol>;

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
                    self.metrics[GAUGE_STATESYNC_LAST_TARGET] = header.0.number;
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
                            tracing::warn!(
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
                            tracing::warn!(?from, "dropping statesync request, still syncing");
                            continue;
                        }
                        StateSyncMode::Live(live) => live,
                    };
                    if execution_ipc
                        .request_tx
                        .try_send((from, StateSyncNetworkMessage::Request(request)))
                        .is_err()
                    {
                        tracing::warn!("dropping inbound statesync request, execution backlogged?")
                    }
                }
                StateSyncCommand::Message((
                    from,
                    StateSyncNetworkMessage::Completion(completion),
                )) => {
                    let execution_ipc = match &mut self.mode {
                        StateSyncMode::Sync(_) => {
                            tracing::warn!(?from, "dropping statesync completion, still syncing");
                            continue;
                        }
                        StateSyncMode::Live(live) => live,
                    };
                    if execution_ipc
                        .request_tx
                        .try_send((from, StateSyncNetworkMessage::Completion(completion)))
                        .is_err()
                    {
                        tracing::warn!(
                            "dropping inbound statesync completion, execution backlogged?"
                        )
                    }
                }
                StateSyncCommand::Message((from, StateSyncNetworkMessage::NotWhitelisted)) => {
                    let statesync = match &mut self.mode {
                        StateSyncMode::Sync(sync) => sync,
                        StateSyncMode::Live(_) => {
                            tracing::warn!(
                                ?from,
                                "dropping statesync not whitelisted, already done syncing"
                            );
                            continue;
                        }
                    };
                    statesync.handle_not_whitelisted(from);
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
                StateSyncCommand::ExpandUpstreamPeers(new_peers) => {
                    let statesync = match &mut self.mode {
                        StateSyncMode::Sync(sync) => sync,
                        StateSyncMode::Live(_) => {
                            tracing::warn!(
                                "dropping statesync expand upstream peers, already done syncing"
                            );
                            continue;
                        }
                    };
                    statesync.expand_upstream_peers(&new_peers);
                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
            }
        }

        self.update_syncing_metrics();
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT> Stream for StateSync<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Unpin,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some(waker) = this.waker.as_mut() {
            waker.clone_from(cx.waker());
        } else {
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
                                None, // we don't care about completions for requests
                            )
                        }
                        SyncRequest::DoneSync(target) => {
                            StateSyncEvent::DoneSync(SeqNum(target.number))
                        }
                        SyncRequest::Completion((servicer, session_id)) => {
                            tracing::debug!(?servicer, "sending completion");
                            StateSyncEvent::Outbound(
                                servicer,
                                StateSyncNetworkMessage::Completion(session_id),
                                None, // we don't care about completions for completions
                            )
                        }
                    };
                    return Poll::Ready(Some(MonadEvent::StateSyncEvent(event)));
                }
            }
            StateSyncMode::Live(execution_ipc) => {
                this.metrics[GAUGE_STATESYNC_SERVER_PENDING_REQUESTS] =
                    execution_ipc.pending_request_len() as u64
                        + execution_ipc.is_servicing_request() as u64;
                this.metrics[GAUGE_STATESYNC_SERVER_NUM_SYNCDONE_SUCCESS] =
                    execution_ipc.num_syncdone_success() as u64;
                this.metrics[GAUGE_STATESYNC_SERVER_NUM_SYNCDONE_FAILED] =
                    execution_ipc.num_syncdone_failed() as u64;
                this.metrics[GAUGE_STATESYNC_SERVER_TOTAL_SERVICE_TIME_US] =
                    execution_ipc.total_service_time_us() as u64;
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
