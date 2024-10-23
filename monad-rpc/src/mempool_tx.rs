use std::{
    ffi::OsStr,
    io::ErrorKind,
    path::{Path, PathBuf},
    pin::Pin,
    task::Poll,
};

use futures::{executor::block_on, ready, Future, FutureExt, Sink, SinkExt};
use notify::{Event, RecursiveMode, Watcher};
use pin_project::pin_project;
use reth_primitives::TransactionSigned;
use tokio::{net::UnixStream, pin};
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};
use tracing::{debug, error};

const MEMPOOL_TX_IPC_FILE: &str = "mempool.sock";

#[pin_project(project = StateProj)]
enum State {
    Ready,
    Reconnect(#[pin] WriterReconnect),
    BrokenPipe(#[pin] SocketWatcher),
}

#[pin_project]
pub struct MempoolTxIpcSender {
    socket_path: PathBuf,
    writer: FramedWrite<UnixStream, LengthDelimitedCodec>,
    #[pin]
    state: State,
}

impl MempoolTxIpcSender {
    pub async fn new<P>(bind_path: P) -> Result<Self, std::io::Error>
    where
        P: AsRef<Path>,
    {
        Ok(Self {
            writer: FramedWrite::new(
                UnixStream::connect(&bind_path).await?,
                LengthDelimitedCodec::default(),
            ),
            socket_path: bind_path.as_ref().to_path_buf(),
            state: State::Ready,
        })
    }
}

struct WriterReconnect {
    task: Pin<Box<dyn Future<Output = std::io::Result<UnixStream>> + Send>>,
}

impl WriterReconnect {
    fn new(path: PathBuf) -> Self {
        let task = Box::pin(UnixStream::connect(path));
        Self { task }
    }
}

impl Future for WriterReconnect {
    type Output = std::io::Result<FramedWrite<UnixStream, LengthDelimitedCodec>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        match self.as_mut().task.poll_unpin(cx) {
            Poll::Ready(Ok(stream)) => Poll::Ready(Ok(FramedWrite::new(
                stream,
                LengthDelimitedCodec::default(),
            ))),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[pin_project]
pub struct SocketWatcher {
    socket_path: PathBuf,
    #[pin]
    watcher: notify::INotifyWatcher,
    #[pin]
    rx: tokio::sync::mpsc::Receiver<notify::Result<notify::Event>>,
}

impl SocketWatcher {
    pub fn try_new(socket_path: PathBuf) -> std::io::Result<Self> {
        let (tx, rx) = tokio::sync::mpsc::channel::<notify::Result<notify::Event>>(100);
        let mut watcher = notify::INotifyWatcher::new(
            move |res| {
                block_on(async {
                    if let Err(send) = tx.send(res).await {
                        error!("cannot send on socket watcher channel: {:?}", send);
                    };
                })
            },
            notify::Config::default(),
        )
        .map_err(|e| std::io::Error::new(ErrorKind::Other, e))?;

        let dir_path = if let Some(parent_path) = socket_path.parent() {
            parent_path
        } else {
            return Err(std::io::Error::new(
                ErrorKind::NotFound,
                "invalid socket path",
            ));
        };

        watcher
            .watch(dir_path.as_ref(), RecursiveMode::NonRecursive)
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e))?;

        Ok(Self {
            socket_path,
            watcher,
            rx,
        })
    }
}

impl Future for SocketWatcher {
    type Output = std::io::Result<()>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let event = this.rx.poll_recv(cx);
        match event {
            Poll::Ready(Some(Ok(Event { kind, paths, .. })))
                if paths.first().is_some()
                    && paths.first().unwrap().as_path().file_name()
                        == Some(OsStr::new(MEMPOOL_TX_IPC_FILE)) =>
            {
                if let notify::EventKind::Create(_) = kind {
                    debug!("new mempool socket created");
                    Poll::Ready(Ok(()))
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Poll::Ready(_) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Pending => Poll::Pending,
        }
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
        self.writer.start_send_unpin(buf.into())
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.as_mut().project();
        let mut state = this.state.as_mut();

        match state.as_mut().project() {
            StateProj::Ready => {
                let writer: Poll<Result<(), std::io::Error>> = this.writer.poll_flush_unpin(cx);
                match writer {
                    Poll::Ready(Err(ref e)) if e.kind() == ErrorKind::BrokenPipe => {
                        let sw = SocketWatcher::try_new(this.socket_path.clone())?;
                        state.set(State::BrokenPipe(sw));
                        cx.waker().wake_by_ref();
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(e));
                    }
                    v => return v,
                }
            }
            StateProj::BrokenPipe(mut sw) => {
                ready!(sw.as_mut().poll(cx))?;
                let writer = WriterReconnect::new(this.socket_path.to_path_buf());
                state.set(State::Reconnect(writer));
                cx.waker().wake_by_ref();
            }
            StateProj::Reconnect(mut writer) => {
                let res = writer.as_mut().poll(cx);
                match res {
                    Poll::Ready(stream) => {
                        *this.writer = stream?;
                        state.set(State::Ready);
                        cx.waker().wake_by_ref();
                    }
                    Poll::Pending => return Poll::Pending,
                };
            }
        }
        Poll::Pending
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.writer.poll_close_unpin(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use futures::future::join;
    use tempfile::tempdir;
    use tokio::{io::AsyncReadExt, net::UnixListener};

    use super::*;

    #[tokio::test]
    async fn test_mempool_tx_ipc_sender_broken_pipe() {
        let listener_func = |listener: UnixListener| async move {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    let mut buf = [0; 1024];
                    let _ = stream.read(&mut buf).await;
                    Ok(())
                }
                Err(e) => Err(e),
            }
        };

        let tmp_dir = tempdir().unwrap();
        let socket_path: PathBuf = tmp_dir.path().join(MEMPOOL_TX_IPC_FILE);

        let listener = UnixListener::bind(&socket_path).unwrap();
        let mut sender: MempoolTxIpcSender = MempoolTxIpcSender::new(&socket_path).await.unwrap();

        let listener_task = tokio::spawn(async move {
            listener_func(listener).await.unwrap();
        });

        sender.send(TransactionSigned::default()).await.unwrap();

        listener_task.await.unwrap();
        std::fs::remove_file(&socket_path).unwrap();

        let sender_task = tokio::spawn(async move {
            sender.send(TransactionSigned::default()).await.unwrap();
        });

        let listener_task = tokio::spawn(async move {
            File::create(tmp_dir.path().join("not-mempool-file")).unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            File::create(tmp_dir.path().join("not-mempool-file")).unwrap();
            let listener = UnixListener::bind(&socket_path).unwrap();
            listener_func(listener).await.unwrap();
        });

        let (first, second) = join(listener_task, sender_task).await;
        first.unwrap();
        second.unwrap();
    }
}
