use std::{io::ErrorKind, path::Path, task::Poll};

use futures::{executor::block_on, FutureExt, Stream};
use monad_blockdb::BlockValue;
use monad_blockdb_utils::BlockDbEnv;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use pin_project::pin_project;

#[derive(Clone)]
pub struct Deps {
    blockdb: BlockDbEnv,
}

impl Deps {
    pub fn new(blockdb_path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            blockdb: BlockDbEnv::new(blockdb_path)?,
        })
    }

    pub async fn get_latest_block(&self) -> Option<BlockValue> {
        self.blockdb
            .get_block_by_tag(monad_blockdb_utils::BlockTags::Default(
                monad_blockdb::BlockTagKey::Latest,
            ))
            .await
    }
}

pub struct DbFileWatcher {
    watcher: notify::INotifyWatcher,
    rx: tokio::sync::mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
}

impl DbFileWatcher {
    pub fn try_new(path: &Path) -> std::io::Result<Self> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<notify::Result<notify::Event>>();

        let mut watcher = notify::INotifyWatcher::new(
            move |res| {
                block_on(async {
                    tx.send(res).expect("inotify channel");
                })
            },
            notify::Config::default(),
        )
        .map_err(|e| std::io::Error::new(ErrorKind::Other, e))?;

        watcher
            .watch(path, RecursiveMode::NonRecursive)
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e))?;

        Ok(Self { watcher, rx })
    }
}

impl Stream for DbFileWatcher {
    type Item = bool;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(Event {
                kind: EventKind::Modify(_),
                ..
            }))) => Poll::Ready(Some(true)),
            Poll::Ready(_) => {
                // TODO: waker
                Poll::Pending
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[pin_project(project = StateProj)]
enum State {
    WaitingForBlock,
    FetchBlock {
        task: std::pin::Pin<Box<dyn futures::Future<Output = std::option::Option<BlockValue>>>>,
    },
}

#[pin_project]
pub struct BlockStream {
    #[pin]
    watcher: DbFileWatcher,
    deps: Deps,
    #[pin]
    state: State,
}

impl BlockStream {
    pub fn new(deps: Deps, path: &Path) -> Self {
        Self {
            watcher: DbFileWatcher::try_new(path).expect("failed to create blockdb watcher"),
            deps,
            state: State::WaitingForBlock,
        }
    }
}

impl Stream for BlockStream {
    type Item = BlockValue;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();
        let mut state = this.state.as_mut();

        match state.as_mut().project() {
            StateProj::WaitingForBlock => match this.watcher.poll_next(cx) {
                Poll::Ready(Some(true)) => {
                    let deps = this.deps.clone();
                    let task = async move { deps.get_latest_block().await };
                    let task = Box::pin(task);
                    state.set(State::FetchBlock { task });
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Ready(_) => {
                    this.state.set(State::WaitingForBlock);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            },
            StateProj::FetchBlock { task } => match task.as_mut().poll_unpin(cx) {
                Poll::Ready(Some(block)) => {
                    this.state.set(State::WaitingForBlock);
                    Poll::Ready(Some(block))
                }
                Poll::Ready(None) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }
}
