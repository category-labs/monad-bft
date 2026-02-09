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
    collections::VecDeque,
    pin::pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use futures::{
    channel::oneshot::{self, Canceled},
    FutureExt,
};
use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::{
    SessionId, StateSyncBadVersion, StateSyncNetworkMessage, StateSyncRequest, StateSyncResponse,
    StateSyncUpsertType, StateSyncUpsertV1, SELF_STATESYNC_VERSION, STATESYNC_VERSION_MIN,
    STATESYNC_VERSION_V2,
};
use monad_types::NodeId;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};

use crate::bindings;

/// StateSyncIpc encapsulates a connection to a live execution client, used for servicing statesync
/// requests
pub(crate) struct StateSyncIpc<PT: PubKey> {
    /// request_tx accepts (from, request) pairs
    pub request_tx: tokio::sync::mpsc::Sender<(NodeId<PT>, StateSyncNetworkMessage)>,
    /// response_rx yields (to, response, completion) tuples
    /// `completion` gets resolved when the message is serialized + written to the socket
    pub response_rx:
        tokio::sync::mpsc::Receiver<(NodeId<PT>, StateSyncNetworkMessage, oneshot::Sender<()>)>,

    pending_request_len: Arc<AtomicUsize>,
    is_servicing_request: Arc<AtomicBool>,
    num_syncdone_success: Arc<AtomicUsize>,
    num_syncdone_failed: Arc<AtomicUsize>,
    total_service_time_us: Arc<AtomicUsize>,
}

impl<PT: PubKey> StateSyncIpc<PT> {
    pub fn pending_request_len(&self) -> usize {
        self.pending_request_len.load(Ordering::Relaxed)
    }

    pub fn is_servicing_request(&self) -> bool {
        self.is_servicing_request.load(Ordering::Relaxed)
    }

    pub fn num_syncdone_success(&self) -> usize {
        self.num_syncdone_success.load(Ordering::Relaxed)
    }

    pub fn num_syncdone_failed(&self) -> usize {
        self.num_syncdone_failed.load(Ordering::Relaxed)
    }

    pub fn total_service_time_us(&self) -> usize {
        self.total_service_time_us.load(Ordering::Relaxed)
    }
}

// Flow:
//
// Execution                    StateSyncExecutor                  Consensus
//            SyncTarget(n)                        SyncTarget(n)
//           <------------------                  <---------------
//
//            SyncRequest(n, prefix)               SyncRequest(n, prefix)
//           ------------------>                  --------------->
//
//            SyncUpsert(n, prefix)                SyncUpsert(n, prefix)
//           <------------------                  <---------------
//            SyncUpsert                           SyncUpsert(id)
//           <------------------                  <---------------
//            SyncDone                             SyncDone(id)
//           <------------------                  <---------------
pub enum ExecutionMessage {
    SyncRequest(bindings::monad_sync_request),
    SyncUpsert(StateSyncUpsertType, Vec<u8>),
    SyncDone(bindings::monad_sync_done),
}

/// maximum number of upserts we can send in a single response
/// at 75 bytes per upsert, approx 1.5MB
const MAX_UPSERTS_PER_RESPONSE: usize = 20_000;
/// maximum amount of data in upserts we can send in a single response
const MAX_RESPONSE_SIZE: usize = 1_500_000;
/// max number of chunked responses to send out before blocking on completions
const MAX_PENDING_RESPONSES: usize = 2;

impl<PT: PubKey> StateSyncIpc<PT> {
    pub fn new(uds_path: &str, request_timeout: Duration) -> Self {
        let listener = UnixListener::bind(uds_path)
            .unwrap_or_else(|e| panic!("invalid UDS path={:?}, err={:?}", uds_path, e));

        let (request_tx_writer, mut request_tx_reader) = tokio::sync::mpsc::channel(10);
        let (mut response_rx_writer, response_rx_reader) = tokio::sync::mpsc::channel(100);

        let pending_request_len = Arc::new(AtomicUsize::new(0));
        let pending_request_len_clone = pending_request_len.clone();
        let is_servicing_request = Arc::new(AtomicBool::new(false));
        let is_servicing_request_clone = is_servicing_request.clone();
        let num_syncdone_success = Arc::new(AtomicUsize::new(0));
        let num_syncdone_success_clone = num_syncdone_success.clone();
        let num_syncdone_failed = Arc::new(AtomicUsize::new(0));
        let num_syncdone_failed_clone = num_syncdone_failed.clone();
        let total_service_time_us = Arc::new(AtomicUsize::new(0));
        let total_service_time_us_clone = total_service_time_us.clone();

        tokio::spawn(async move {
            let pending_request_len = pending_request_len_clone;
            let is_servicing_request = is_servicing_request_clone;
            let num_syncdone_success = num_syncdone_success_clone;
            let num_syncdone_failed = num_syncdone_failed_clone;
            let total_service_time_us = total_service_time_us_clone;
            loop {
                let execution_stream = {
                    let conn_fut = async {
                        let (stream, _addr) = listener
                            .accept()
                            .await
                            .expect("failed to accept statesync UDS connection");
                        stream
                    };
                    let drain_fut = async {
                        // this future exists to make sure that the request channel is drained
                        // while waiting for execution to connect
                        loop {
                            let _request: (NodeId<PT>, StateSyncNetworkMessage) =
                                request_tx_reader.recv().await.expect("request_tx dropped");
                        }
                    };
                    match futures::future::select(pin!(conn_fut), pin!(drain_fut)).await {
                        futures::future::Either::Left((stream, _)) => stream,
                        futures::future::Either::Right(_) => {
                            unreachable!("drain_fut yields forever")
                        }
                    }
                };

                let mut stream_state = StreamState::new(
                    request_timeout,
                    execution_stream,
                    &mut request_tx_reader,
                    &mut response_rx_writer,
                );
                while let Ok(()) = stream_state.poll().await {
                    pending_request_len
                        .store(stream_state.pending_requests.len(), Ordering::Relaxed);
                    is_servicing_request
                        .store(stream_state.wip_response.is_some(), Ordering::Relaxed);
                    num_syncdone_success
                        .store(stream_state.metric_syncdone_success, Ordering::Relaxed);
                    num_syncdone_failed
                        .store(stream_state.metric_syncdone_failed, Ordering::Relaxed);
                    total_service_time_us
                        .store(stream_state.metric_total_service_time_us, Ordering::Relaxed);
                }

                tracing::warn!("UDS socket error, retrying");
                pending_request_len.store(0, Ordering::Relaxed);
                is_servicing_request.store(false, Ordering::Relaxed);
                num_syncdone_success.store(0, Ordering::Relaxed);
                num_syncdone_failed.store(0, Ordering::Relaxed);
                total_service_time_us.store(0, Ordering::Relaxed);
            }
        });

        Self {
            request_tx: request_tx_writer,
            response_rx: response_rx_reader,
            pending_request_len,
            is_servicing_request,
            num_syncdone_success,
            num_syncdone_failed,
            total_service_time_us,
        }
    }
}

struct StreamState<'a, PT: PubKey> {
    /// drop pending requests older than this
    request_timeout: Duration,
    stream: BufReader<UnixStream>,
    request_tx_reader: &'a mut tokio::sync::mpsc::Receiver<(NodeId<PT>, StateSyncNetworkMessage)>,
    response_rx_writer: &'a mut tokio::sync::mpsc::Sender<(
        NodeId<PT>,
        StateSyncNetworkMessage,
        oneshot::Sender<()>,
    )>,

    pending_requests: VecDeque<PendingRequest<PT>>,
    wip_response: Option<WipResponse<PT>>,

    metric_syncdone_success: usize,
    metric_syncdone_failed: usize,
    metric_total_service_time_us: usize,
}

#[derive(Debug)]
struct PendingRequest<PT: PubKey> {
    from: NodeId<PT>,
    rx_time: Instant,
    request: StateSyncRequest,
}

// maximum number of responses we can send before client needs to acknowledge
const MAX_UNACKNOWLEDGED_RESPONSES: usize = 100;

#[derive(Debug)]
struct WipResponse<PT: PubKey> {
    from: NodeId<PT>,
    rx_time: Instant,
    service_start_time: Instant,

    /// send next message after this time even if we don't have a full batch
    next_message_schedule: Instant,

    /// last completion message received from the client
    completion_time: Instant,

    /// number of bytes used by upserts in the response
    response_size: usize,

    response: StateSyncResponse,

    /// outstanding unacknowledged responses
    unacknowledged_responses: usize,

    // list of pending completions, max size of MAX_PENDING_RESPONSES
    // set to none if any of the completions have failed
    pending_response_completions: Option<VecDeque<oneshot::Receiver<()>>>,
}

// Schedule messages to be sent every second even if we don't have a full batch
// This prevents the client from timing out as long as the server stays up
const MESSAGE_SCHEDULE_DURATION: Duration = Duration::from_millis(500);

// Timeout for the client to acknowledge a message
const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);

impl<PT: PubKey> WipResponse<PT> {
    pub fn new(from: NodeId<PT>, rx_time: Instant, request: StateSyncRequest) -> Self {
        Self {
            from,
            rx_time,
            service_start_time: Instant::now(),
            next_message_schedule: Instant::now() + MESSAGE_SCHEDULE_DURATION,
            completion_time: Instant::now(),
            response_size: 0,
            response: StateSyncResponse {
                version: request.version,
                nonce: rand::random(),
                response_index: 0,
                request,
                response: Vec::new(),
                response_n: 0, // This gets set in handle_execution_message
            },
            pending_response_completions: Some(VecDeque::with_capacity(MAX_PENDING_RESPONSES)),
            unacknowledged_responses: 0,
        }
    }

    // Wait for both tcp completions and acknowledgements from client
    pub fn can_send(&self) -> bool {
        self.pending_response_completions.as_ref().is_none_or(|r| {
            r.len() < MAX_PENDING_RESPONSES
                && (self.response.version < STATESYNC_VERSION_V2
                    || self.unacknowledged_responses < MAX_UNACKNOWLEDGED_RESPONSES)
        })
    }

    // Check if we have more space in the buffer
    pub fn has_space(&self) -> bool {
        self.response_size < MAX_RESPONSE_SIZE
            && self.response.response.len() < MAX_UPSERTS_PER_RESPONSE
    }
}

impl<'a, PT: PubKey> StreamState<'a, PT> {
    fn new(
        request_timeout: Duration,
        stream: UnixStream,
        request_tx_reader: &'a mut tokio::sync::mpsc::Receiver<(
            NodeId<PT>,
            StateSyncNetworkMessage,
        )>,
        response_rx_writer: &'a mut tokio::sync::mpsc::Sender<(
            NodeId<PT>,
            StateSyncNetworkMessage,
            oneshot::Sender<()>,
        )>,
    ) -> Self {
        Self {
            request_timeout,
            stream: BufReader::with_capacity(
                1024 * 1024, // 1 MB
                stream,
            ),
            request_tx_reader,
            response_rx_writer,

            pending_requests: Default::default(),
            wip_response: Default::default(),

            metric_syncdone_success: 0,
            metric_syncdone_failed: 0,
            metric_total_service_time_us: 0,
        }
    }

    /// Top-level function that's used to step StreamState forward
    async fn poll(&mut self) -> Result<(), tokio::io::Error> {
        enum Event<PT: PubKey> {
            Execution(Result<u8, tokio::io::Error>),
            Message((NodeId<PT>, StateSyncNetworkMessage)),
            TcpCompletion(Result<(), Canceled>),
            Timeout,
            ClientTimeout,
        }
        let fut = async {
            let maybe_request = self.request_tx_reader.recv().await;
            Event::Message(maybe_request.expect("request_tx_writer dropped"))
        };
        let mut futures = vec![fut.boxed()];

        if let Some(wip_response) = self.wip_response.as_mut() {
            // Read from the execution socket only if have space in the buffer. Execution will
            // eventually block on the socket send if we don't read from it.
            if wip_response.has_space() {
                let fut = async {
                    let event = self.stream.read_u8().await;
                    Event::Execution(event)
                };
                futures.push(fut.boxed());
            }

            // If not waiting on a client, schedule next send after a timeout
            if wip_response.can_send() {
                let fut = async {
                    tokio::time::sleep_until(wip_response.next_message_schedule.into()).await;
                    Event::Timeout
                };
                futures.push(fut.boxed());
            }

            // If we have a pending response, schedule a timeout for the client that supports completions
            if wip_response.unacknowledged_responses > 0
                && wip_response.pending_response_completions.is_some()
                && wip_response.response.version >= STATESYNC_VERSION_V2
            {
                let fut = async {
                    tokio::time::sleep_until(
                        (wip_response.completion_time + CLIENT_TIMEOUT).into(),
                    )
                    .await;
                    Event::ClientTimeout
                };
                futures.push(fut.boxed());
            }

            // Receive the first pending completion
            if let Some(completion) = wip_response
                .pending_response_completions
                .as_mut()
                .and_then(VecDeque::front_mut)
            {
                let fut = async { Event::TcpCompletion(completion.await) };
                futures.push(fut.boxed());
            }
        }
        let (event, _, _) = futures::future::select_all(futures).await;

        match event {
            Event::Execution(maybe_msg_type) => {
                let msg_type = maybe_msg_type?;
                let execution_message = self.read_execution_message(msg_type).await?;
                self.handle_execution_message(execution_message).await
            }
            Event::Message((from, request)) => self.handle_message(from, request).await,
            Event::Timeout => self.handle_timeout(),
            Event::TcpCompletion(res) => self.handle_tcp_completion(res),
            Event::ClientTimeout => self.handle_client_timeout(),
        }
    }

    fn write_response(
        response_writer: &mut tokio::sync::mpsc::Sender<(
            NodeId<PT>,
            StateSyncNetworkMessage,
            oneshot::Sender<()>,
        )>,
        to: NodeId<PT>,
        response: StateSyncNetworkMessage,
    ) -> oneshot::Receiver<()> {
        let (completion_sender, completion_receiver) = oneshot::channel();
        if response_writer
            .try_send((to, response, completion_sender))
            .is_err()
        {
            tracing::warn!("dropping outbound statesync response, consensus state backlogged?");
        }
        completion_receiver
    }

    fn send_batch(&mut self) {
        let Some(wip_response) = &mut self.wip_response else {
            tracing::warn!("send_batch called with no wip_response");
            return;
        };

        let response = StateSyncResponse {
            version: wip_response.response.version,
            nonce: wip_response.response.nonce,
            response_index: wip_response.response.response_index,

            request: wip_response.response.request,
            response: std::mem::take(&mut wip_response.response.response),
            response_n: wip_response.response.response_n,
        };
        wip_response.response.response_index += 1;
        // unnecessary but keeping for clarity
        wip_response.response.response.clear();
        wip_response.response_size = 0;

        // Reset timeout when sending message
        wip_response.next_message_schedule = Instant::now() + MESSAGE_SCHEDULE_DURATION;

        if let Some(pending_response_completions) = &mut wip_response.pending_response_completions {
            wip_response.unacknowledged_responses += 1;
            let completion = Self::write_response(
                self.response_rx_writer,
                wip_response.from,
                StateSyncNetworkMessage::Response(response),
            );
            pending_response_completions.push_back(completion);
        } else {
            tracing::warn!(
                ?wip_response.from,
                request =? response.request,
                nonce =? response.nonce,
                response_index = response.response_index,
                "not sending statesync response, previous send failed"
            )
        }
    }

    fn maybe_send_batch(&mut self) {
        if let Some(wip_response) = &mut self.wip_response {
            if !wip_response.has_space() && wip_response.can_send() {
                self.send_batch()
            }
        }
    }

    async fn handle_execution_message(
        &mut self,
        message: ExecutionMessage,
    ) -> Result<(), tokio::io::Error> {
        match message {
            ExecutionMessage::SyncRequest(request) => {
                panic!("live-mode execution sent unexpected SyncRequest: {request:?}");
            }
            ExecutionMessage::SyncUpsert(upsert_type, data) => {
                let wip_response = self
                    .wip_response
                    .as_mut()
                    .expect("SyncUpsert has wip_response");
                wip_response.response_size += data.len();
                wip_response
                    .response
                    .response
                    .push(StateSyncUpsertV1::new(upsert_type, data.into()));
                self.maybe_send_batch();
            }
            ExecutionMessage::SyncDone(done) => {
                // only one request can be handled at once - because no way of identifying which
                // requests upserts point to
                let mut wip_response = self.wip_response.take().expect("SyncDone has wip_response");
                let service_elapsed = wip_response.service_start_time.elapsed();
                self.metric_total_service_time_us += service_elapsed.as_micros() as usize;
                tracing::debug!(
                    rx_elapsed =? wip_response.rx_time.elapsed(),
                    ?service_elapsed,
                    from =? wip_response.from,
                    ?done,
                    "received SyncDone"
                );
                if done.success {
                    self.metric_syncdone_success += 1;
                    assert_eq!(wip_response.response.request.prefix, done.prefix);
                    // response_n is overloaded to indicate that the response is done
                    wip_response.response.response_n = done.n;
                    if wip_response.pending_response_completions.is_some() {
                        let _completion = Self::write_response(
                            self.response_rx_writer,
                            wip_response.from,
                            StateSyncNetworkMessage::Response(wip_response.response),
                        );
                    } else {
                        tracing::warn!(
                            from =? wip_response.from,
                            request =? wip_response.response.request,
                            nonce =? wip_response.response.nonce,
                            response_index = wip_response.response.response_index,
                            response_n = wip_response.response.response_n,
                            "not sending statesync response (final), previous send failed"
                        )
                    }
                } else {
                    self.metric_syncdone_failed += 1;
                    // request failed, so don't send finish the response. we've dropped the
                    // wip_response at this point.
                    tracing::warn!(
                        from =? wip_response.from,
                        request =? wip_response.response.request,
                        nonce =? wip_response.response.nonce,
                        response_index = wip_response.response.response_index,
                        "received SyncDone with failure, dropping pending response"
                    );
                }
                self.try_queue_response().await?;
            }
        };
        Ok(())
    }

    fn handle_timeout(&mut self) -> Result<(), tokio::io::Error> {
        tracing::warn!(
            "pushing incomplete batch due to timeout for prefix {:?}",
            self.wip_response
                .as_ref()
                .map(|wip_response| wip_response.response.request.prefix)
        );
        self.send_batch();
        Ok(())
    }

    fn handle_tcp_completion(&mut self, res: Result<(), Canceled>) -> Result<(), tokio::io::Error> {
        if let Some(wip_response) = self.wip_response.as_mut() {
            if let Some(pending_response_completions) =
                &mut wip_response.pending_response_completions
            {
                pending_response_completions.pop_front();
            }
            if res.is_err() {
                wip_response.pending_response_completions = None;
            }
            self.maybe_send_batch();
        }
        Ok(())
    }

    fn handle_client_timeout(&mut self) -> Result<(), tokio::io::Error> {
        if let Some(wip_response) = self.wip_response.as_mut() {
            tracing::warn!(
                "client {} timed out, dropping pending response",
                wip_response.from
            );

            // drop any further messages to this client
            wip_response.pending_response_completions = None;
        }
        Ok(())
    }

    async fn handle_message(
        &mut self,
        from: NodeId<PT>,
        message: StateSyncNetworkMessage,
    ) -> Result<(), tokio::io::Error> {
        match message {
            StateSyncNetworkMessage::Request(request) => self.handle_request(from, request).await,
            StateSyncNetworkMessage::Completion(session_id) => {
                self.handle_completion(from, session_id)
            }
            _ => {
                tracing::warn!(?from, ?message, "unexpected message from client");
                Ok(())
            }
        }
    }

    fn handle_completion(
        &mut self,
        from: NodeId<PT>,
        session_id: SessionId,
    ) -> Result<(), tokio::io::Error> {
        if let Some(wip_response) = self.wip_response.as_mut() {
            if wip_response.from == from && wip_response.response.nonce == session_id.0 {
                wip_response.unacknowledged_responses =
                    wip_response.unacknowledged_responses.saturating_sub(1);
            } else {
                tracing::debug!(?from, ?session_id, "unexpected completion from client");
            }
            wip_response.completion_time = Instant::now();
            self.maybe_send_batch();
        } else {
            tracing::debug!(
                ?from,
                ?session_id,
                "unexpected completion from client, no pending response"
            );
        }
        Ok(())
    }

    async fn handle_request(
        &mut self,
        from: NodeId<PT>,
        request: StateSyncRequest,
    ) -> Result<(), tokio::io::Error> {
        if !request.version.is_compatible() {
            tracing::debug!(
                ?from,
                ?request,
                ?SELF_STATESYNC_VERSION,
                "incompatible statesync request version"
            );
            let bad_version = StateSyncBadVersion {
                min_version: STATESYNC_VERSION_MIN,
                max_version: SELF_STATESYNC_VERSION,
            };
            let (completion_sender, _) = oneshot::channel();
            if self
                .response_rx_writer
                .try_send((
                    from,
                    StateSyncNetworkMessage::BadVersion(bad_version),
                    completion_sender,
                ))
                .is_err()
            {
                tracing::warn!("dropping outbound statesync response, consensus state backlogged?");
            }

            return Ok(());
        }

        self.pending_requests.retain(|pending_request| {
            // delete any requests from this peer for an old target
            // delete any duplicate requests from this peer
            !(pending_request.from == from
                && (pending_request.request.target != request.target
                    || pending_request.request == request))
        });
        if self
            .wip_response
            .as_ref()
            .is_some_and(|wip_response| wip_response.response.request == request)
        {
            tracing::debug!(
                ?from,
                ?request,
                "ignoring duplicate request, already servicing this request"
            );
            // we are already servicing this request, drop the new one
            return Ok(());
        }
        tracing::debug!(
            ?from,
            ?request,
            "received statesync request, num pending {}, in progress {}",
            self.pending_requests.len(),
            if self.wip_response.is_some() { 1 } else { 0 }
        );
        self.pending_requests.push_back(PendingRequest {
            from,
            request,
            rx_time: Instant::now(),
        });
        self.try_queue_response().await
    }

    async fn try_queue_response(&mut self) -> Result<(), tokio::io::Error> {
        if self.wip_response.is_some() {
            // we already are servicing a pending request, so ignore
            return Ok(());
        }

        let request = loop {
            let Some(request) = self.pending_requests.pop_front() else {
                // no more pending requests
                return Ok(());
            };

            if request.rx_time.elapsed() > self.request_timeout {
                tracing::debug!(
                    ?request,
                    request_timeout =? self.request_timeout,
                    "dropping stale request"
                );
                continue;
            }

            break request;
        };

        self.wip_response = Some(WipResponse::new(
            request.from,
            request.rx_time,
            request.request,
        ));

        self.write_execution_request(bindings::monad_sync_request {
            prefix: request.request.prefix,
            prefix_bytes: request.request.prefix_bytes,
            target: request.request.target,
            from: request.request.from,
            until: request.request.until,
            old_target: request.request.old_target,
        })
        .await?;

        Ok(())
    }

    async fn read_execution_message(
        &mut self,
        msg_type: u8,
    ) -> Result<ExecutionMessage, tokio::io::Error> {
        let execution_msg = match msg_type {
            bindings::monad_sync_type_SYNC_TYPE_TARGET => {
                panic!("live-mode execution sent unexpected SyncTarget");
            }
            bindings::monad_sync_type_SYNC_TYPE_REQUEST => {
                let mut buf = [0_u8; std::mem::size_of::<bindings::monad_sync_request>()];
                self.stream.read_exact(&mut buf).await?;
                ExecutionMessage::SyncRequest(unsafe {
                    #[allow(clippy::missing_transmute_annotations)]
                    std::mem::transmute(buf)
                })
            }
            bindings::monad_sync_type_SYNC_TYPE_UPSERT_CODE => {
                self.read_sync_upsert(StateSyncUpsertType::Code).await?
            }
            bindings::monad_sync_type_SYNC_TYPE_UPSERT_ACCOUNT => {
                self.read_sync_upsert(StateSyncUpsertType::Account).await?
            }
            bindings::monad_sync_type_SYNC_TYPE_UPSERT_STORAGE => {
                self.read_sync_upsert(StateSyncUpsertType::Storage).await?
            }
            bindings::monad_sync_type_SYNC_TYPE_UPSERT_ACCOUNT_DELETE => {
                self.read_sync_upsert(StateSyncUpsertType::AccountDelete)
                    .await?
            }
            bindings::monad_sync_type_SYNC_TYPE_UPSERT_STORAGE_DELETE => {
                self.read_sync_upsert(StateSyncUpsertType::StorageDelete)
                    .await?
            }
            bindings::monad_sync_type_SYNC_TYPE_UPSERT_HEADER => {
                self.read_sync_upsert(StateSyncUpsertType::Header).await?
            }
            bindings::monad_sync_type_SYNC_TYPE_DONE => {
                let mut buf = [0_u8; std::mem::size_of::<bindings::monad_sync_done>()];
                self.stream.read_exact(&mut buf).await?;
                ExecutionMessage::SyncDone(unsafe {
                    #[allow(clippy::missing_transmute_annotations)]
                    std::mem::transmute(buf)
                })
            }
            t => panic!("unknown msg_type={t}"),
        };
        Ok(execution_msg)
    }

    async fn read_sync_upsert(
        &mut self,
        upsert_type: StateSyncUpsertType,
    ) -> Result<ExecutionMessage, tokio::io::Error> {
        let data_len = self.stream.read_u64_le().await?;
        let mut data = vec![0_u8; data_len as usize];
        self.stream.read_exact(&mut data).await?;
        Ok(ExecutionMessage::SyncUpsert(upsert_type, data))
    }

    async fn write_execution_request(
        &mut self,
        request: bindings::monad_sync_request,
    ) -> Result<(), tokio::io::Error> {
        self.stream
            .write_u8(bindings::monad_sync_type_SYNC_TYPE_REQUEST)
            .await?;
        let request: [u8; std::mem::size_of::<bindings::monad_sync_request>()] = unsafe {
            #[allow(clippy::missing_transmute_annotations)]
            std::mem::transmute(request)
        };
        self.stream.write_all(&request).await?;
        Ok(())
    }
}
