use std::{
    collections::VecDeque,
    pin::pin,
    time::{Duration, Instant},
};

use futures::FutureExt;
use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::{StateSyncRequest, StateSyncResponse, StateSyncUpsertType};
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
    pub request_tx: tokio::sync::mpsc::Sender<(NodeId<PT>, StateSyncRequest)>,
    /// response_rx yields (to, response) pairs
    pub response_rx: tokio::sync::mpsc::Receiver<(NodeId<PT>, StateSyncResponse)>,
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
    SyncTarget(bindings::monad_sync_target),
    SyncRequest(bindings::monad_sync_request),
    SyncUpsert(StateSyncUpsertType, Vec<u8>),
    SyncDone(bindings::monad_sync_done),
}

impl<PT: PubKey> StateSyncIpc<PT> {
    pub fn new(uds_path: &str, request_timeout: Duration) -> Self {
        let listener = UnixListener::bind(uds_path)
            .unwrap_or_else(|e| panic!("invalid UDS path={:?}, err={:?}", uds_path, e));

        let (request_tx_writer, mut request_tx_reader) = tokio::sync::mpsc::channel(10);
        let (mut response_rx_writer, response_rx_reader) = tokio::sync::mpsc::channel(10);
        tokio::spawn(async move {
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
                            let _request: (NodeId<PT>, StateSyncRequest) =
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
                while let Ok(()) = stream_state.poll().await {}

                tracing::warn!("UDS socket error, retrying");
            }
        });

        Self {
            request_tx: request_tx_writer,
            response_rx: response_rx_reader,
        }
    }
}

struct StreamState<'a, PT: PubKey> {
    /// drop pending requests older than this
    request_timeout: Duration,
    stream: BufReader<UnixStream>,
    request_tx_reader: &'a mut tokio::sync::mpsc::Receiver<(NodeId<PT>, StateSyncRequest)>,
    response_rx_writer: &'a mut tokio::sync::mpsc::Sender<(NodeId<PT>, StateSyncResponse)>,

    pending_requests: VecDeque<PendingRequest<PT>>,
    wip_response: Option<WipResponse<PT>>,
}

#[derive(Debug)]
struct PendingRequest<PT: PubKey> {
    from: NodeId<PT>,
    rx_time: Instant,
    request: StateSyncRequest,
}

#[derive(Debug)]
struct WipResponse<PT: PubKey> {
    from: NodeId<PT>,
    rx_time: Instant,
    service_start_time: Instant,
    response: StateSyncResponse,
}

impl<'a, PT: PubKey> StreamState<'a, PT> {
    fn new(
        request_timeout: Duration,
        stream: UnixStream,
        request_tx_reader: &'a mut tokio::sync::mpsc::Receiver<(NodeId<PT>, StateSyncRequest)>,
        response_rx_writer: &'a mut tokio::sync::mpsc::Sender<(NodeId<PT>, StateSyncResponse)>,
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
        }
    }

    /// Top-level function that's used to step StreamState forward
    async fn poll(&mut self) -> Result<(), tokio::io::Error> {
        enum Event<PT: PubKey> {
            Execution(Result<u8, tokio::io::Error>),
            Request((NodeId<PT>, StateSyncRequest)),
        }
        let fut1 = async {
            let event = self.stream.read_u8().await;
            Event::Execution(event)
        };
        let fut2 = async {
            let maybe_request = self.request_tx_reader.recv().await;
            Event::Request(maybe_request.expect("request_tx_writer dropped"))
        };
        let (event, _, _) = futures::future::select_all(vec![fut1.boxed(), fut2.boxed()]).await;

        match event {
            Event::Execution(maybe_msg_type) => {
                let msg_type = maybe_msg_type?;
                let execution_message = self.read_execution_message(msg_type).await?;
                self.handle_execution_message(execution_message).await
            }
            Event::Request((from, request)) => self.handle_request(from, request).await,
        }
    }

    async fn handle_execution_message(
        &mut self,
        message: ExecutionMessage,
    ) -> Result<(), tokio::io::Error> {
        match message {
            ExecutionMessage::SyncTarget(_target) => {
                panic!("live-mode execution shouldn't send SyncTarget")
            }
            ExecutionMessage::SyncRequest(_request) => {
                panic!("live-mode execution shouldn't send SyncRequest")
            }
            ExecutionMessage::SyncUpsert(upsert_type, data) => {
                let wip_response = self
                    .wip_response
                    .as_mut()
                    .expect("SyncUpsert with no pending_response");
                wip_response.response.response.push((upsert_type, data));
            }
            ExecutionMessage::SyncDone(done) => {
                // only one request can be handled at once - because no way of identifying which
                // requests upserts point to
                let mut wip_response = self
                    .wip_response
                    .take()
                    .expect("syncdone received with no pending_response");
                tracing::debug!(
                    rx_elapsed =? wip_response.rx_time.elapsed(),
                    service_elapsed =? wip_response.service_start_time.elapsed(),
                    from =? wip_response.from,
                    ?done,
                    "received SyncDone"
                );
                if done.success {
                    assert_eq!(wip_response.response.request.prefix, done.prefix);
                    wip_response.response.response_n = done.n;
                    self.write_response(wip_response.from, wip_response.response);
                } else {
                    // request failed, so don't send back a response. we've dropped the
                    // wip_response at this point.
                }
                self.try_queue_response().await?;
            }
        };
        Ok(())
    }

    async fn handle_request(
        &mut self,
        from: NodeId<PT>,
        request: StateSyncRequest,
    ) -> Result<(), tokio::io::Error> {
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

        self.wip_response = Some(WipResponse {
            from: request.from,
            rx_time: request.rx_time,
            service_start_time: Instant::now(),
            response: StateSyncResponse {
                request: request.request,
                response: Vec::new(),

                // this gets set in handle_execution_message(ExecutionMessage::SyncDone(_))
                response_n: 0,
            },
        });

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
            bindings::monad_sync_type_SyncTypeTarget => {
                let mut buf = [0_u8; std::mem::size_of::<bindings::monad_sync_target>()];
                self.stream.read_exact(&mut buf).await?;
                ExecutionMessage::SyncTarget(unsafe {
                    #[allow(clippy::missing_transmute_annotations)]
                    std::mem::transmute(buf)
                })
            }
            bindings::monad_sync_type_SyncTypeRequest => {
                let mut buf = [0_u8; std::mem::size_of::<bindings::monad_sync_request>()];
                self.stream.read_exact(&mut buf).await?;
                ExecutionMessage::SyncRequest(unsafe {
                    #[allow(clippy::missing_transmute_annotations)]
                    std::mem::transmute(buf)
                })
            }
            bindings::monad_sync_type_SyncTypeUpsertCode => {
                let data_len = self.stream.read_u64_le().await?;
                let mut data = vec![0_u8; data_len as usize];
                self.stream.read_exact(&mut data).await?;
                ExecutionMessage::SyncUpsert(StateSyncUpsertType::Code, data)
            }
            bindings::monad_sync_type_SyncTypeUpsertAccount => {
                let data_len = self.stream.read_u64_le().await?;
                let mut data = vec![0_u8; data_len as usize];
                self.stream.read_exact(&mut data).await?;
                ExecutionMessage::SyncUpsert(StateSyncUpsertType::Account, data)
            }
            bindings::monad_sync_type_SyncTypeUpsertStorage => {
                let data_len = self.stream.read_u64_le().await?;
                let mut data = vec![0_u8; data_len as usize];
                self.stream.read_exact(&mut data).await?;
                ExecutionMessage::SyncUpsert(StateSyncUpsertType::Storage, data)
            }
            bindings::monad_sync_type_SyncTypeUpsertAccountDelete => {
                let data_len = self.stream.read_u64_le().await?;
                let mut data = vec![0_u8; data_len as usize];
                self.stream.read_exact(&mut data).await?;
                ExecutionMessage::SyncUpsert(StateSyncUpsertType::AccountDelete, data)
            }
            bindings::monad_sync_type_SyncTypeUpsertStorageDelete => {
                let data_len = self.stream.read_u64_le().await?;
                let mut data = vec![0_u8; data_len as usize];
                self.stream.read_exact(&mut data).await?;
                ExecutionMessage::SyncUpsert(StateSyncUpsertType::StorageDelete, data)
            }
            bindings::monad_sync_type_SyncTypeDone => {
                let mut buf = [0_u8; std::mem::size_of::<bindings::monad_sync_done>()];
                self.stream.read_exact(&mut buf).await?;
                ExecutionMessage::SyncDone(unsafe {
                    #[allow(clippy::missing_transmute_annotations)]
                    std::mem::transmute(buf)
                })
            }
            t => panic!("unknown msg_type={}", t),
        };
        Ok(execution_msg)
    }

    async fn write_execution_request(
        &mut self,
        request: bindings::monad_sync_request,
    ) -> Result<(), tokio::io::Error> {
        self.stream
            .write_u8(bindings::monad_sync_type_SyncTypeRequest)
            .await?;
        let request: [u8; std::mem::size_of::<bindings::monad_sync_request>()] = unsafe {
            #[allow(clippy::missing_transmute_annotations)]
            std::mem::transmute(request)
        };
        self.stream.write_all(&request).await?;
        Ok(())
    }

    fn write_response(&mut self, to: NodeId<PT>, response: StateSyncResponse) {
        if self.response_rx_writer.try_send((to, response)).is_err() {
            tracing::warn!("dropping outbound statesync response, consensus state backlogged?")
        }
    }
}
