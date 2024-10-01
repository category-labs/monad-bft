use std::{marker::PhantomData, path::PathBuf, pin::Pin, task::Poll};

use alloy_rlp::Decodable;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_tx::EthSignedTransaction;
use monad_executor_glue::{MempoolEvent, MonadEvent};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::mpsc,
    time::{Duration, Instant},
};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, error, trace, warn};

pub struct IpcReceiver<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    read_tx_batch_recv: mpsc::Receiver<Vec<Bytes>>,
    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> IpcReceiver<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    /// tx_batch_size: number of txs per batch
    /// max_queued_batches: max number of batches to queue
    /// queued_batches_watermark: warn if number of queued batches exceeds this
    pub fn new(
        bind_path: PathBuf,
        tx_batch_size: usize,
        max_queued_batches: usize,
        queued_batches_watermark: usize,
    ) -> Result<Self, std::io::Error> {
        assert!(queued_batches_watermark <= max_queued_batches);
        let (read_tx_batch_send, read_tx_batch_recv) = mpsc::channel(max_queued_batches);

        let r = Self {
            read_tx_batch_recv,
            _phantom: Default::default(),
        };

        let listener = UnixListener::bind(bind_path)?;
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, sockaddr)) => {
                        debug!("new ipc connection sockaddr={:?}", sockaddr);
                        Self::new_connection(
                            stream,
                            read_tx_batch_send.clone(),
                            tx_batch_size,
                            queued_batches_watermark,
                        );
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
        tx_batch_channel: mpsc::Sender<Vec<Bytes>>,
        tx_batch_size: usize,
        queued_batches_watermark: usize,
    ) {
        let mut reader = FramedRead::new(stream, LengthDelimitedCodec::default());

        let send_batch = move |tx: &mut Vec<Bytes>| {
            if tx.is_empty() {
                return;
            }
            match tx_batch_channel.try_send(tx.to_vec()) {
                Ok(()) => {
                    trace!("bytes received from IPC and sent to channel");
                    let capacity = tx_batch_channel.capacity();
                    let num_queued = tx_batch_channel.max_capacity() - capacity;
                    if num_queued > queued_batches_watermark {
                        warn!(
                            queued_batches_watermark,
                            num_queued,
                            max_capacity = tx_batch_channel.max_capacity(),
                            "transaction IPC recv channel exceeded watermark, are transactions being ingested fast enough?"
                        );
                    }
                }
                Err(mpsc::error::TrySendError::Full(dropped_batch)) => {
                    error!(
                        dropped_batch_len = dropped_batch.len(),
                        max_capacity = tx_batch_channel.max_capacity(),
                        "transaction IPC recv channel full, dropping batch"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    warn!("failed to send, channel closed")
                }
            }
            tx.clear();
        };

        tokio::spawn(async move {
            let mut txns = Vec::with_capacity(tx_batch_size);
            let mut batch_timeout = ArmedSleep::new();

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
                                batch_timeout.arm();
                                if txns.len() >= tx_batch_size {
                                    send_batch(&mut txns);
                                    batch_timeout.reset();
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
                    () = &mut batch_timeout.sleep => {
                        send_batch(&mut txns);
                        batch_timeout.reset();
                    }
                }
            }

            send_batch(&mut txns);
        });
    }
}
fn validate_ethtx(bytes: &mut &[u8]) -> bool {
    match EthSignedTransaction::decode(bytes) {
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
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.read_tx_batch_recv.poll_recv(cx).map(|maybe_batch| {
            let batch = maybe_batch.expect("read_tx_batch_send should never be dropped");
            Some(MonadEvent::MempoolEvent(MempoolEvent::UserTxns(batch)))
        })
    }
}

struct ArmedSleep {
    sleep: Pin<Box<tokio::time::Sleep>>,
    armed: bool,
}

impl ArmedSleep {
    fn new() -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(Duration::from_secs(100))),
            armed: false,
        }
    }

    fn reset(&mut self) {
        self.sleep
            .as_mut()
            .reset(Instant::now() + Duration::from_secs(100));
        self.armed = false;
    }

    fn arm(&mut self) {
        if self.armed {
            return;
        }

        self.sleep
            .as_mut()
            .reset(Instant::now() + Duration::from_millis(25));
        self.armed = true;
    }
}
