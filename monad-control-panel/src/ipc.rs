use std::{
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{SinkExt, Stream, StreamExt};
use monad_consensus_types::{
    signature_collection::SignatureCollection,
    validator_data::{ParsedValidatorData, ValidatorSetData},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::Executor;
use monad_executor_glue::{
    ClearMetrics, ControlPanelCommand, ControlPanelEvent, GetValidatorSet, MonadEvent, ReadCommand,
    UpdateValidatorSet, ValidatorEvent, WriteCommand,
};
use tokio::{
    net::{unix::OwnedReadHalf, UnixListener},
    sync::{broadcast, mpsc},
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, error, warn};

pub struct ControlPanelIpcReceiver<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    receiver: mpsc::Receiver<MonadEvent<ST, SCT>>,
    client_sender: broadcast::Sender<ControlPanelCommand<SCT>>,
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
    pub fn new(bind_path: PathBuf, buf_size: usize) -> Result<Self, std::io::Error> {
        let (sender, receiver) = mpsc::channel(buf_size);
        let (client_sender, _client_receiver) =
            broadcast::channel::<ControlPanelCommand<SCT>>(buf_size);
        let client_sender_clone = client_sender.clone();

        let r = Self {
            receiver,
            client_sender,
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
                                let Ok(encoded) = bincode::serialize(&command) else {
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

            let Ok(request) = bincode::deserialize::<ControlPanelCommand<SCT>>(&bytes) else {
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
                                let ParsedValidatorData::<SCT> { epoch, validators } =
                                    parsed_validator_set;
                                let validators =
                                    validators.into_iter().map(Into::into).collect::<Vec<_>>();
                                let event = MonadEvent::<ST, SCT>::ValidatorEvent(
                                    ValidatorEvent::<SCT>::UpdateValidators((
                                        ValidatorSetData(validators),
                                        epoch,
                                    )),
                                );
                                let Ok(_) = event_channel.send(event.clone()).await else {
                                    error!("failed to forward request {:?} to executor, closing connection", &event);
                                    break;
                                };
                            }
                            m => error!("unhandled message {:?}", m),
                        }
                    }
                    m => error!("unhandled message {:?}", m),
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

    fn replay(&mut self, mut commands: Vec<Self::Command>) {
        commands.retain(|command| match command {
            ControlPanelCommand::Read(_) => false,
            // commands that write/change state should be replayed
            ControlPanelCommand::Write(_) => true,
        });
        self.exec(commands);
    }

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            debug!(num_clients = %self.client_sender.receiver_count(), "broadcasting {:?} to clients", &command);
            self.client_sender
                .send(command)
                .expect("failed to broadcast command to clients");
        }
    }
}
