use std::{
    path::{Path, PathBuf},
    task::Poll,
};

use futures::{Sink, SinkExt, Stream, StreamExt};
use monad_mempool_types::tx::EthTx;
use prost::Message;
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub struct MempoolTxIpcSender {
    writer: FramedWrite<UnixStream, LengthDelimitedCodec>,
}

impl MempoolTxIpcSender {
    pub async fn new<P>(bind_path: P) -> Result<Self, std::io::Error>
    where
        P: AsRef<Path>,
    {
        Ok(Self {
            writer: FramedWrite::new(
                UnixStream::connect(bind_path).await?,
                LengthDelimitedCodec::default(),
            ),
        })
    }
}

impl Sink<EthTx> for MempoolTxIpcSender {
    type Error = std::io::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.writer.poll_ready_unpin(cx)
    }

    fn start_send(mut self: std::pin::Pin<&mut Self>, tx: EthTx) -> Result<(), Self::Error> {
        let buf = tx.encode_to_vec();

        self.writer.start_send_unpin(buf.into())
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.writer.poll_flush_unpin(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.writer.poll_close_unpin(cx)
    }
}

pub(crate) struct MempoolTxIpcReceiver {
    path: PathBuf,
    listener: UnixListener,
    readers: Vec<FramedRead<UnixStream, LengthDelimitedCodec>>,
}

impl MempoolTxIpcReceiver {
    pub fn new(bind_path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            path: bind_path.as_ref().into(),
            listener: UnixListener::bind(bind_path)?,
            readers: Vec::default(),
        })
    }
}

impl Stream for MempoolTxIpcReceiver {
    type Item = Result<EthTx, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(result) = self.listener.poll_accept(cx) {
            match result {
                Ok((stream, _)) => {
                    // TODO: log new socket connection

                    self.readers
                        .push(FramedRead::new(stream, LengthDelimitedCodec::default()));
                }
                Err(_) => {
                    // TODO: handle error

                    return Poll::Ready(None);
                }
            }
        }

        for reader in self.readers.iter_mut() {
            let result = match reader.poll_next_unpin(cx) {
                Poll::Ready(Some(result)) => result,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => continue,
            };

            let bytes_mut = match result {
                Ok(bytes_mut) => bytes_mut,
                Err(e) => return Poll::Ready(Some(Err(e))),
            };

            return Poll::Ready(Some(Ok(prost::Message::decode(bytes_mut)?)));
        }

        Poll::Pending
    }
}

impl Drop for MempoolTxIpcReceiver {
    fn drop(&mut self) {
        std::fs::remove_file(AsRef::<Path>::as_ref(&self.path)).unwrap();
    }
}
