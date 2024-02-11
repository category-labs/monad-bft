use std::{path::PathBuf, task::Poll};

use alloy_rlp::Decodable;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_tx::EthTransaction;
use monad_executor_glue::{MempoolEvent, MonadEvent};
use rand::distributions::{Alphanumeric, DistString};
use tokio::{
    net::{UnixListener, UnixStream},
    time::{Duration, Instant},
};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, warn};

const DEFAULT_MEMPOOL_BIND_PATH_BASE: &str = "./monad_mempool";
const DEFAULT_MEMPOOL_BIND_PATH_EXT: &str = ".sock";
const MEMPOOL_RANDOMIZE_UDS_PATH_ENVVAR: &str = "MONAD_MEMPOOL_RNDUDS";

pub fn generate_uds_path() -> String {
    let randomize = cfg!(test)
        || std::env::var(MEMPOOL_RANDOMIZE_UDS_PATH_ENVVAR)
            .ok()
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or_default();

    format!(
        "{}{}{}",
        DEFAULT_MEMPOOL_BIND_PATH_BASE,
        if randomize {
            format!(
                "_{}",
                Alphanumeric.sample_string(&mut rand::thread_rng(), 8)
            )
        } else {
            "".to_string()
        },
        DEFAULT_MEMPOOL_BIND_PATH_EXT
    )
}

pub struct IpcReceiver<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    read_events_recv: flume::Receiver<MonadEvent<ST, SCT>>,
}

impl<ST, SCT> IpcReceiver<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(bind_path: PathBuf, buf_size: usize) -> Result<Self, std::io::Error> {
        let (read_events_send, read_events_recv) = flume::bounded(buf_size);

        let r = Self { read_events_recv };

        let listener = UnixListener::bind(bind_path)?;
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, sockaddr)) => {
                        debug!("new ipc connection sockaddr={:?}", sockaddr);
                        IpcReceiver::new_connection(stream, read_events_send.clone(), buf_size);
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

    fn new_connection(
        stream: UnixStream,
        event_channel: flume::Sender<MonadEvent<ST, SCT>>,
        buf_size: usize,
    ) {
        let mut reader = FramedRead::new(stream, LengthDelimitedCodec::default());

        let send_batch = move |tx: &mut Vec<Bytes>| {
            if tx.is_empty() {
                return;
            }
            match event_channel.try_send(MonadEvent::MempoolEvent(MempoolEvent::UserTxns(
                tx.to_vec(),
            ))) {
                Ok(_) => debug!("bytes received from IPC and sent to channel"),
                Err(flume::TrySendError::Full(_)) => todo!(
                    "IPC recv channel full, max_capacity={:?}",
                    event_channel.capacity()
                ),
                Err(flume::TrySendError::Disconnected(_)) => {
                    warn!("failed to send, channel closed")
                }
            }
            tx.clear();
        };

        tokio::spawn(async move {
            let mut txns = Vec::with_capacity(buf_size);
            let batch_timeout = tokio::time::sleep(Duration::from_secs(100));
            tokio::pin!(batch_timeout);

            loop {
                tokio::select! {
                    read = reader.next() => {
                        match read {
                            Some(Ok(bytes)) => {
                                let bytes = bytes.freeze();
                                if !validate_ethtx(&mut bytes.as_ref()) {
                                    break;
                                }

                                txns.push(bytes);
                                if txns.len() >= buf_size {
                                    send_batch(&mut txns);
                                } else {
                                    batch_timeout.as_mut().reset(Instant::now() + Duration::from_millis(250));
                                }
                            }
                            Some(Err(err)) => {
                                warn!("framed reader error err={:?}", err);
                                break;
                            }
                            None => {
                                debug!("done reading");
                                break;
                            }
                        }
                    }
                    () = &mut batch_timeout => {
                        send_batch(&mut txns);
                        batch_timeout.as_mut().reset(Instant::now() + Duration::from_secs(100));
                    }
                }
            }

            send_batch(&mut txns);
        });
    }
}

fn validate_ethtx(bytes: &mut &[u8]) -> bool {
    match EthTransaction::decode(bytes) {
        Ok(_) => true,
        Err(err) => {
            warn!("tx decoder error error={:?}", err);
            false
        }
    }
}

impl<ST, SCT> Stream for IpcReceiver<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut pinned = std::pin::pin!(self.read_events_recv.stream());
        pinned.as_mut().poll_next(cx)
    }
}
