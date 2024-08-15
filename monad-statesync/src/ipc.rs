use std::{pin::pin, time::Instant};

use futures::FutureExt;
use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::{StateSyncRequest, StateSyncResponse};
use monad_types::NodeId;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
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
    SyncUpsert(bindings::monad_sync_upsert_header, Vec<u8>, Vec<u8>),
    SyncDone(bindings::monad_sync_done),
}

impl<PT: PubKey> StateSyncIpc<PT> {
    pub fn new(uds_path: &str) -> Self {
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
                        // this future exists to make sure that the response channel is drained
                        // while waiting for requests
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
    stream: UnixStream,
    request_tx_reader: &'a mut tokio::sync::mpsc::Receiver<(NodeId<PT>, StateSyncRequest)>,
    response_rx_writer: &'a mut tokio::sync::mpsc::Sender<(NodeId<PT>, StateSyncResponse)>,

    inbound_request: Option<(NodeId<PT>, StateSyncResponse, Instant)>,
}

impl<'a, PT: PubKey> StreamState<'a, PT> {
    fn new(
        stream: UnixStream,
        request_tx_reader: &'a mut tokio::sync::mpsc::Receiver<(NodeId<PT>, StateSyncRequest)>,
        response_rx_writer: &'a mut tokio::sync::mpsc::Sender<(NodeId<PT>, StateSyncResponse)>,
    ) -> Self {
        Self {
            stream,
            request_tx_reader,
            response_rx_writer,

            inbound_request: Default::default(),
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
                self.handle_execution_message(execution_message)
            }
            Event::Request((from, request)) => self.handle_request(from, request).await,
        }
    }

    fn handle_execution_message(
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
            ExecutionMessage::SyncUpsert(upsert, key, value) => {
                assert_eq!(upsert.key_size, key.len() as u64);
                assert_eq!(upsert.value_size, value.len() as u64);
                let (_, inbound_request, _) = self
                    .inbound_request
                    .as_mut()
                    .expect("SyncUpsert with no pending request");
                inbound_request.response.push((upsert.code, key, value));
            }
            ExecutionMessage::SyncDone(done) => {
                // only one request can be handled at once - because no way of identifying which
                // requests upserts point to
                let (from, mut inbound_request, start) = self
                    .inbound_request
                    .take()
                    .expect("syncdone received with no pending request");
                tracing::debug!(
                    elapsed =? start.elapsed(),
                    ?from,
                    ?done,
                    "received SyncDone"
                );
                if done.success {
                    assert_eq!(inbound_request.request.prefix, done.prefix);
                    inbound_request.response_n = done.n;
                    self.write_response(from, inbound_request);
                } else {
                    // request failed, so don't send back a response. we've dropped the
                    // inbound_request at this point.
                }
            }
        };
        Ok(())
    }

    async fn handle_request(
        &mut self,
        from: NodeId<PT>,
        request: StateSyncRequest,
    ) -> Result<(), tokio::io::Error> {
        if self.inbound_request.is_some() {
            // we already are servicing a pending request, so ignore
            tracing::debug!("dropping state-sync request, already servicing one");
            return Ok(());
        }
        self.write_execution_message(ExecutionMessage::SyncRequest(
            bindings::monad_sync_request {
                prefix: request.prefix,
                prefix_bytes: request.prefix_bytes,
                target: request.target,
                from: request.from,
                until: request.until,
                old_target: request.old_target,
            },
        ))
        .await?;

        self.inbound_request = Some((
            from,
            StateSyncResponse {
                request,
                response: Vec::new(),

                // this gets set in handle_execution_message(ExecutionMessage::SyncDone(_))
                response_n: 0,
            },
            Instant::now(),
        ));
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
            bindings::monad_sync_type_SyncTypeUpsertHeader => {
                let mut buf = [0_u8; std::mem::size_of::<bindings::monad_sync_upsert_header>()];
                self.stream.read_exact(&mut buf).await?;
                let upsert_header: bindings::monad_sync_upsert_header = unsafe {
                    #[allow(clippy::missing_transmute_annotations)]
                    std::mem::transmute(buf)
                };
                let mut key = vec![0_u8; upsert_header.key_size as usize];
                self.stream.read_exact(&mut key).await?;
                let mut value = vec![0_u8; upsert_header.value_size as usize];
                self.stream.read_exact(&mut value).await?;
                ExecutionMessage::SyncUpsert(upsert_header, key, value)
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

    async fn write_execution_message(
        &mut self,
        message: ExecutionMessage,
    ) -> Result<(), tokio::io::Error> {
        match message {
            ExecutionMessage::SyncTarget(target) => {
                self.stream
                    .write_u8(bindings::monad_sync_type_SyncTypeTarget)
                    .await?;
                let target: [u8; std::mem::size_of::<bindings::monad_sync_target>()] = unsafe {
                    #[allow(clippy::missing_transmute_annotations)]
                    std::mem::transmute(target)
                };
                self.stream.write_all(&target).await?;
            }
            ExecutionMessage::SyncRequest(request) => {
                self.stream
                    .write_u8(bindings::monad_sync_type_SyncTypeRequest)
                    .await?;
                let request: [u8; std::mem::size_of::<bindings::monad_sync_request>()] = unsafe {
                    #[allow(clippy::missing_transmute_annotations)]
                    std::mem::transmute(request)
                };
                self.stream.write_all(&request).await?;
            }
            ExecutionMessage::SyncUpsert(header, key, value) => {
                self.stream
                    .write_u8(bindings::monad_sync_type_SyncTypeUpsertHeader)
                    .await?;
                let upsert_header: [u8; std::mem::size_of::<bindings::monad_sync_upsert_header>()] = unsafe {
                    #[allow(clippy::missing_transmute_annotations)]
                    std::mem::transmute(header)
                };
                self.stream.write_all(&upsert_header).await?;
                self.stream.write_all(&key).await?;
                self.stream.write_all(&value).await?;
            }
            ExecutionMessage::SyncDone(done) => {
                self.stream
                    .write_u8(bindings::monad_sync_type_SyncTypeDone)
                    .await?;
                let done: [u8; std::mem::size_of::<bindings::monad_sync_done>()] = unsafe {
                    #[allow(clippy::missing_transmute_annotations)]
                    std::mem::transmute(done)
                };
                self.stream.write_all(&done).await?;
            }
        };
        Ok(())
    }

    fn write_response(&mut self, to: NodeId<PT>, response: StateSyncResponse) {
        if self.response_rx_writer.try_send((to, response)).is_err() {
            tracing::warn!("dropping outbound statesync response, consensus state backlogged?")
        }
    }
}
