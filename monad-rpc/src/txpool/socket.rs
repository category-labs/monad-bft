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

use std::{ffi::OsStr, io, path::PathBuf, task::Poll};

use futures::{executor::block_on, Future};
use notify::{Event, RecursiveMode, Watcher};
use pin_project::pin_project;
use tokio::{pin, sync::mpsc};
use tracing::{debug, error};

const MEMPOOL_TX_IPC_FILE: &str = "mempool.sock";

#[pin_project]
pub struct SocketWatcher {
    socket_path: PathBuf,
    #[pin]
    watcher: notify::INotifyWatcher,
    #[pin]
    rx: mpsc::Receiver<notify::Result<notify::Event>>,
}

impl SocketWatcher {
    pub fn try_new(socket_path: PathBuf) -> io::Result<Self> {
        let (tx, rx) = mpsc::channel::<notify::Result<notify::Event>>(100);
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
        .map_err(io::Error::other)?;

        let dir_path = if let Some(parent_path) = socket_path.parent() {
            parent_path
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "invalid socket path",
            ));
        };

        watcher
            .watch(dir_path.as_ref(), RecursiveMode::NonRecursive)
            .map_err(io::Error::other)?;

        Ok(Self {
            socket_path,
            watcher,
            rx,
        })
    }
}

impl Future for SocketWatcher {
    type Output = io::Result<()>;

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
