use std::{marker::PhantomData, path::Path, task::Poll};

use alloy_rlp::Decodable;
use futures::{stream::SelectAll, Stream, StreamExt};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthTransaction;
use monad_executor_glue::{MempoolEvent, MonadEvent};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, info, warn};

pub struct IpcReceiver<ST, SCT> {
    /// Listener for incoming connections on the socket
    listener: UnixListener,
    /// A reader is created per stream on the Unix socket
    readers: SelectAll<FramedRead<UnixStream, LengthDelimitedCodec>>,

    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> IpcReceiver<ST, SCT> {
    pub fn new(bind_path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            listener: UnixListener::bind(bind_path)?,
            readers: SelectAll::default(),
            _phantom: PhantomData,
        })
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
        if let Poll::Ready(result) = self.listener.poll_accept(cx) {
            match result {
                Ok((stream, sockaddr)) => {
                    debug!("new ipc connection sockaddr={:?}", sockaddr);
                    self.readers
                        .push(FramedRead::new(stream, LengthDelimitedCodec::default()));
                }
                Err(err) => {
                    warn!("listener poll accept error={:?}", err);
                    // TODO-2: handle error
                    todo!("ipc listener error");
                }
            }
        }

        while !self.readers.is_empty() {
            let bytes = match self.readers.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(bytes))) => bytes,
                Poll::Ready(Some(Err(err))) => {
                    warn!("framed reader error err={:?}", err);
                    continue;
                }
                Poll::Ready(None) => {
                    // SelectAll is empty: all streams have terminated
                    debug!("all streams terminated");
                    break;
                }
                Poll::Pending => break,
            };

            let user_tx = match EthTransaction::decode(&mut bytes.freeze().as_ref()) {
                Ok(eth_tx) => eth_tx,
                Err(err) => {
                    info!("tx decoder error error={:?}", err);
                    continue;
                }
            };

            return Poll::Ready(Some(MonadEvent::MempoolEvent(MempoolEvent::UserTx(
                user_tx,
            ))));
        }

        Poll::Pending
    }
}

pub struct MockIpcReceiver<ST, SCT> {
    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> Default for MockIpcReceiver<ST, SCT> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT> Stream for MockIpcReceiver<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // TODO: generate user tx
        Poll::Ready(None)
    }
}
