use std::{
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{SinkExt, Stream, StreamExt};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{
    ClearMetrics, ControlPanelCommand, ControlPanelEvent, GetExecutorMetrics, GetFullNodes,
    GetMetrics, GetPeers, GetValidatorSet, MonadEvent, ReadCommand, UpdateFullNodes, UpdatePeers,
    UpdateValidatorSet, WriteCommand,
};
use tokio::{
    net::{unix::OwnedReadHalf, UnixListener},
    sync::{broadcast, mpsc},
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, error, warn};
use tracing_subscriber::{reload::Handle, EnvFilter, Registry};

pub type ReloadHandle = Handle<EnvFilter, Registry>;

pub struct ControlPanelIpcReceiver<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    receiver: mpsc::Receiver<MonadEvent<ST, SCT>>,
    client_sender: broadcast::Sender<ControlPanelCommand<SCT>>,

    metrics: ExecutorMetrics,

    reload_handle: ReloadHandle,
}

impl<ST, SCT> Stream for ControlPanelIpcReceiver<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl<ST, SCT> ControlPanelIpcReceiver<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(
        bind_path: PathBuf,
        reload_handle: ReloadHandle,
        buf_size: usize,
    ) -> Result<Self, std::io::Error> {
        let (sender, receiver) = mpsc::channel(buf_size);
        let (client_sender, _client_receiver) =
            broadcast::channel::<ControlPanelCommand<SCT>>(buf_size);
        let client_sender_clone = client_sender.clone();

        let r = Self {
            receiver,
            client_sender,
            reload_handle,

            metrics: Default::default(),
        };

        let listener = UnixListener::bind(bind_path)?;
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, sockaddr)) => {
                        debug!("new ipc connection sockaddr={:?}", sockaddr);

                        let (read, write) = stream.into_split();

                        let read = FramedRead::new(read, LengthDelimitedCodec::default());
                        let mut write = FramedWrite::new(write, LengthDelimitedCodec::default());

                        let mut client_receiver = client_sender_clone.subscribe();
                        tokio::spawn(async move {
                            while let Ok(command) = client_receiver.recv().await {
                                let Ok(encoded) = serde_json::to_string(&command) else {
                                    error!("failed to serialize {:?} message to client", &command);
                                    continue;
                                };

                                if let Err(e) = write.send(encoded.into()).await {
                                    error!("failed to send {:?} to client, they likely disconnected, exiting loop: {:?}", &command, e);
                                    break;
                                }
                            }
                        });

                        ControlPanelIpcReceiver::new_connection(read, sender.clone()).await;
                    }
                    Err(err) => {
                        warn!("listener poll accept error={:?}", err);
                        // TODO-2: handle error
                        todo!("ipc listener error");
                    }
                }
            }
        });

        Ok(r)
    }

    async fn new_connection(
        mut read: FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        event_channel: mpsc::Sender<MonadEvent<ST, SCT>>,
    ) {
        while let Some(Ok(bytes)) = read.next().await {
            debug!("control panel ipc server bytes: {:?}", &bytes);

            let bytes = bytes.freeze();
            let Ok(string) = std::str::from_utf8(&bytes) else {
                error!(
                    "failed to convert bytes from client {:?} to utf-8 string, closing connection",
                    &bytes
                );
                break;
            };
            let Ok(request) = serde_json::from_str::<ControlPanelCommand<SCT>>(string) else {
                error!(
                    "failed to deserialize bytes from client {:?}, closing connection",
                    &bytes
                );
                break;
            };

            debug!("control panel ipc request: {:?}", request);

            match request {
                ControlPanelCommand::Read(r) => match r {
                    ReadCommand::GetValidatorSet(v) => match v {
                        GetValidatorSet::Request => {
                            let event =
                                MonadEvent::ControlPanelEvent(ControlPanelEvent::GetValidatorSet);
                            let Ok(_) = event_channel.send(event.clone()).await else {
                                error!("failed to forward request {:?} to executor, closing connection", &event);
                                break;
                            };
                        }
                        m => error!("unhandled message {:?}", m),
                    },
                    ReadCommand::GetMetrics(m) => match m {
                        GetMetrics::Request => {
                            let event =
                                MonadEvent::ControlPanelEvent(ControlPanelEvent::GetMetricsEvent);
                            let Ok(_) = event_channel.send(event.clone()).await else {
                                error!("failed to forward request {:?} to executor, closing connection", &event);
                                break;
                            };
                        }
                        m => error!("unhandled message {:?}", m),
                    },
                    ReadCommand::GetExecutorMetrics(m) => match m {
                        GetExecutorMetrics::Request => {
                            let event = MonadEvent::ControlPanelEvent(
                                ControlPanelEvent::GetExecutorMetricsEvent,
                            );
                            let Ok(_) = event_channel.send(event.clone()).await else {
                                error!("failed to forward request {:?} to executor, closing connection", &event);
                                break;
                            };
                        }
                        m => error!("unhandled message {:?}", m),
                    },
                    ReadCommand::GetPeers(l) => match l {
                        GetPeers::Request => {
                            let event = MonadEvent::ControlPanelEvent(ControlPanelEvent::GetPeers(
                                GetPeers::Request,
                            ));
                            let Ok(_) = event_channel.send(event.clone()).await else {
                                error!("failed to forward request {:?} to executor, closing connection", &event);
                                break;
                            };
                        }
                        m => error!("unhandled message {:?}", m),
                    },
                    ReadCommand::GetFullNodes(get_full_nodes) => match get_full_nodes {
                        GetFullNodes::Request => {
                            let event = MonadEvent::ControlPanelEvent(
                                ControlPanelEvent::GetFullNodes(GetFullNodes::Request),
                            );
                            let Ok(_) = event_channel.send(event.clone()).await else {
                                error!("failed to forward request {:?} to executor, closing connection", &event);
                                break;
                            };
                        }
                        m => error!("unhandled message {:?}", m),
                    },
                },
                ControlPanelCommand::Write(w) => match w {
                    WriteCommand::ClearMetrics(clear_metrics) => match clear_metrics {
                        ClearMetrics::Request => {
                            let event =
                                MonadEvent::ControlPanelEvent(ControlPanelEvent::ClearMetricsEvent);
                            let Ok(_) = event_channel.send(event.clone()).await else {
                                error!("failed to forward request {:?} to executor, closing connection", &event);
                                break;
                            };
                        }
                        m => error!("unhandled message {:?}", m),
                    },
                    WriteCommand::UpdateValidatorSet(update_validator_set) => {
                        match update_validator_set {
                            UpdateValidatorSet::Request(parsed_validator_set) => {
                                let event = MonadEvent::<ST, SCT>::ControlPanelEvent(
                                    ControlPanelEvent::UpdateValidators(parsed_validator_set),
                                );
                                let Ok(_) = event_channel.send(event.clone()).await else {
                                    error!("failed to forward request {:?} to executor, closing connection", &event);
                                    break;
                                };
                            }
                            m => error!("unhandled message {:?}", m),
                        }
                    }
                    WriteCommand::UpdateLogFilter(filter) => {
                        let event = MonadEvent::ControlPanelEvent(
                            ControlPanelEvent::UpdateLogFilter(filter),
                        );
                        let Ok(_) = event_channel.send(event.clone()).await else {
                            error!(
                                "failed to forward request {:?} to executor, closing connection",
                                &event
                            );
                            break;
                        };
                    }
                    WriteCommand::UpdatePeers(update_peers) => match update_peers {
                        UpdatePeers::Request(vec) => {
                            let event = MonadEvent::ControlPanelEvent(
                                ControlPanelEvent::UpdatePeers(UpdatePeers::Request(vec)),
                            );
                            let Ok(_) = event_channel.send(event.clone()).await else {
                                error!("failed to forward request {:?} to executor, closing connection", &event);
                                break;
                            };
                        }
                        m => error!("unhandled message {:?}", m),
                    },
                    WriteCommand::UpdateFullNodes(update_full_nodes) => match update_full_nodes {
                        UpdateFullNodes::Request(vec) => {
                            let event = MonadEvent::ControlPanelEvent(
                                ControlPanelEvent::UpdateFullNodes(UpdateFullNodes::Request(vec)),
                            );
                            let Ok(_) = event_channel.send(event.clone()).await else {
                                error!("failed to forward request {:?} to executor, closing connection", &event);
                                break;
                            };
                        }
                        m => error!("unhandled message {:?}", m),
                    },
                },
            }
        }
    }
}

impl<ST, SCT> Executor for ControlPanelIpcReceiver<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = ControlPanelCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            if let ControlPanelCommand::Write(WriteCommand::UpdateLogFilter(filter)) = &command {
                match EnvFilter::builder().parse(filter) {
                    Ok(filter) => {
                        if let Err(e) = self.reload_handle.reload(filter) {
                            panic!("failed to update logging filter: {:?}", e);
                        }
                    }
                    Err(e) => {
                        error!("failed to parse logging filter: {:?}", e);
                    }
                }
            }

            debug!(num_clients = %self.client_sender.receiver_count(), "broadcasting {:?} to clients", &command);
            self.client_sender
                .send(command)
                .expect("failed to broadcast command to clients");
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
