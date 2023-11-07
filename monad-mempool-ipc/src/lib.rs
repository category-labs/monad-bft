use std::{
    path::{Path, PathBuf},
    task::Poll,
};

use futures::{Sink, SinkExt, Stream, StreamExt};
use rand::distributions::{Alphanumeric, DistString};
use reth_primitives::TransactionSigned;
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

#[cfg(target_os = "linux")]
const DEFAULT_MEMPOOL_BIND_PATH_BASE: &str = "/run/monad_mempool";
#[cfg(target_os = "macos")]
const DEFAULT_MEMPOOL_BIND_PATH_BASE: &str = "/var/run/monad_mempool";

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

impl Sink<TransactionSigned> for MempoolTxIpcSender {
    type Error = std::io::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.writer.poll_ready_unpin(cx)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        tx: TransactionSigned,
    ) -> Result<(), Self::Error> {
        let buf = tx.envelope_encoded();

        self.writer.start_send_unpin(buf)
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

pub struct MempoolTxIpcReceiver {
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
    type Item = Result<TransactionSigned, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(result) = self.listener.poll_accept(cx) {
            match result {
                Ok((stream, _)) => {
                    // TODO-2: log new socket connection

                    self.readers
                        .push(FramedRead::new(stream, LengthDelimitedCodec::default()));
                }
                Err(_) => {
                    // TODO-2: handle error

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

            return Poll::Ready(Some(
                TransactionSigned::decode_enveloped(reth_primitives::Bytes(bytes_mut.freeze()))
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            ));
        }

        Poll::Pending
    }
}

impl Drop for MempoolTxIpcReceiver {
    fn drop(&mut self) {
        std::fs::remove_file(AsRef::<Path>::as_ref(&self.path)).unwrap();
    }
}
