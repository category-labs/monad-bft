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
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{SinkExt, Stream, StreamExt};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{
    ClearMetrics, ControlPanelCommand, ControlPanelEvent, GetFullNodes, GetMetrics, GetPeers,
    MonadEvent, ReadCommand, ReloadConfig, WriteCommand,
};
use monad_types::ExecutionProtocol;
use monad_validator::signature_collection::SignatureCollection;
use tokio::{
    net::{unix::OwnedReadHalf, UnixListener},
    sync::{broadcast, mpsc},
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, error, warn};
use tracing_subscriber::EnvFilter;

use crate::TracingReload;

pub struct ControlPanelIpcReceiver<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    receiver: mpsc::Receiver<MonadEvent<ST, SCT, EPT>>,
    client_sender: broadcast::Sender<ControlPanelCommand<ST>>,

    metrics: ExecutorMetrics,

    reload_handle: Box<dyn TracingReload>,
}

impl<ST, SCT, EPT> Stream for ControlPanelIpcReceiver<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl<ST, SCT, EPT> ControlPanelIpcReceiver<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        bind_path: PathBuf,
        reload_handle: Box<dyn TracingReload>,
        buf_size: usize,
    ) -> Result<Self, std::io::Error> {
        let (sender, receiver) = mpsc::channel(buf_size);
        let (client_sender, _client_receiver) =
            broadcast::channel::<ControlPanelCommand<ST>>(buf_size);
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
        event_channel: mpsc::Sender<MonadEvent<ST, SCT, EPT>>,
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
            let Ok(request) = serde_json::from_str::<ControlPanelCommand<ST>>(string) else {
                error!(
                    "failed to deserialize bytes from client {:?}, closing connection",
                    &bytes
                );
                break;
            };

            debug!("control panel ipc request: {:?}", request);

            match request {
                ControlPanelCommand::Read(r) => match r {
                    ReadCommand::GetMetrics(m) => match m {
                        GetMetrics::Request => {
                            let event = ControlPanelEvent::GetMetricsEvent;
                            let Ok(_) = event_channel
                                .send(MonadEvent::ControlPanelEvent(event.clone()))
                                .await
                            else {
                                error!("failed to forward request {:?} to executor, closing connection", &event);
                                break;
                            };
                        }
                        m => error!("unhandled message {:?}", m),
                    },
                    ReadCommand::GetPeers(l) => match l {
                        GetPeers::Request => {
                            let event = ControlPanelEvent::GetPeers(GetPeers::Request);
                            let Ok(_) = event_channel
                                .send(MonadEvent::ControlPanelEvent(event.clone()))
                                .await
                            else {
                                error!("failed to forward request {:?} to executor, closing connection", &event);
                                break;
                            };
                        }
                        m => error!("unhandled message {:?}", m),
                    },
                    ReadCommand::GetFullNodes(get_full_nodes) => match get_full_nodes {
                        GetFullNodes::Request => {
                            let event = ControlPanelEvent::GetFullNodes(GetFullNodes::Request);
                            let Ok(_) = event_channel
                                .send(MonadEvent::ControlPanelEvent(event.clone()))
                                .await
                            else {
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
                            let event = ControlPanelEvent::ClearMetricsEvent;
                            let Ok(_) = event_channel
                                .send(MonadEvent::ControlPanelEvent(event.clone()))
                                .await
                            else {
                                error!("failed to forward request {:?} to executor, closing connection", &event);
                                break;
                            };
                        }
                        m => error!("unhandled message {:?}", m),
                    },
                    WriteCommand::UpdateLogFilter(filter) => {
                        let event = ControlPanelEvent::UpdateLogFilter(filter);
                        let Ok(_) = event_channel
                            .send(MonadEvent::ControlPanelEvent(event.clone()))
                            .await
                        else {
                            error!(
                                "failed to forward request {:?} to executor, closing connection",
                                &event
                            );
                            break;
                        };
                    }
                    WriteCommand::ReloadConfig(reload_config) => match reload_config {
                        ReloadConfig::Request => {
                            let event = ControlPanelEvent::ReloadConfig(ReloadConfig::Request);
                            let Ok(_) = event_channel
                                .send(MonadEvent::ControlPanelEvent(event.clone()))
                                .await
                            else {
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

impl<ST, SCT, EPT> Executor for ControlPanelIpcReceiver<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Command = ControlPanelCommand<ST>;

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

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        self.metrics.as_ref().into()
    }
}
