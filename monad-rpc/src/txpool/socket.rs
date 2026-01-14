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
    ffi::OsString,
    io,
    path::{Path, PathBuf},
    task::Poll,
};

use futures::{Stream, StreamExt};
use inotify::{EventMask, EventStream, Inotify, WatchMask};
use pin_project::pin_project;
use tracing::{debug, error, warn};

#[pin_project]
pub struct SocketWatcher {
    inotify: EventStream<[u8; 1024]>,
    parent: PathBuf,
    filename: OsString,
}

impl SocketWatcher {
    pub fn try_new<P>(socket_path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let filename = socket_path
            .as_ref()
            .file_name()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "Socket path does not have a filename",
                )
            })?
            .to_os_string();

        let parent = socket_path
            .as_ref()
            .parent()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "Socket path does not have parent")
            })?
            .to_path_buf();

        let inotify = Inotify::init()?;

        inotify.watches().add(
            &parent,
            WatchMask::CREATE | WatchMask::MOVED_FROM | WatchMask::DELETE | WatchMask::DELETE_SELF,
        )?;

        let inotify = inotify.into_event_stream([0; 1024])?;

        Ok(Self {
            inotify,
            parent,
            filename,
        })
    }
}

#[derive(Debug)]
pub enum SocketWatcherEvent {
    Create(PathBuf),
    Delete,
}

impl Stream for SocketWatcher {
    type Item = io::Result<SocketWatcherEvent>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let event = match self.as_mut().inotify.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(Some(Ok(event))) => event,
            };

            debug!(mask =? event.mask, "socket watcher inotify event");

            if event.mask.contains(EventMask::DELETE_SELF) {
                error!("socket watcher parent directory deleted");
                return Poll::Ready(None);
            }

            let Some(name) = &event.name else {
                error!("socket watcher event does not have a name");
                continue;
            };

            if name != &self.filename {
                continue;
            }

            if event.mask.contains(EventMask::CREATE) {
                return Poll::Ready(Some(Ok(SocketWatcherEvent::Create(self.parent.join(name)))));
            }

            if event.mask.contains(EventMask::MOVED_FROM) {
                error!("socket watcher detected socket file moved");
                return Poll::Ready(None);
            }

            if event.mask.contains(EventMask::DELETE) {
                return Poll::Ready(Some(Ok(SocketWatcherEvent::Delete)));
            }

            warn!(filename =? event.name, mask =? event.mask, "socket watcher inotify unknown event");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use futures::StreamExt;
    use tempfile::TempDir;

    use super::*;

    /// Helper function to create a temporary directory for testing
    fn setup_test_dir() -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let socket_path = temp_dir.path().join("test.sock");
        (temp_dir, socket_path)
    }

    #[tokio::test]
    async fn test_try_new_valid_path() {
        let (_temp_dir, socket_path) = setup_test_dir();

        let watcher = SocketWatcher::try_new(&socket_path);
        assert!(
            watcher.is_ok(),
            "SocketWatcher should successfully initialize with valid path"
        );

        let watcher = watcher.unwrap();
        assert_eq!(
            watcher.filename,
            socket_path.file_name().unwrap(),
            "Filename should match the socket path filename"
        );
    }

    #[tokio::test]
    async fn test_try_new_no_filename() {
        let result = SocketWatcher::try_new("/");

        assert!(result.is_err(), "Should fail with path without filename");
        let err = result.err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert_eq!(
            err.to_string(),
            "Socket path does not have a filename",
            "Error message should indicate missing filename"
        );
    }

    #[tokio::test]
    async fn test_try_new_nonexistent_parent() {
        let result = SocketWatcher::try_new("/nonexistent/directory/socket.sock");

        assert!(
            result.is_err(),
            "Should fail with non-existent parent directory"
        );
    }

    #[tokio::test]
    async fn test_socket_create_event() {
        let (_temp_dir, socket_path) = setup_test_dir();

        let mut watcher = SocketWatcher::try_new(&socket_path).expect("Failed to create watcher");

        fs::write(&socket_path, b"").expect("Failed to create socket file");

        let result = tokio::time::timeout(Duration::from_secs(2), watcher.next()).await;

        assert!(result.is_ok(), "Should receive event within timeout");
        let event = result.unwrap();
        assert!(event.is_some(), "Stream should not be closed");

        let event = event.unwrap();
        assert!(event.is_ok(), "Event should not be an error");

        match event.unwrap() {
            SocketWatcherEvent::Create(path) => {
                assert_eq!(
                    path, socket_path,
                    "Created path should match socket filename"
                );
            }
            SocketWatcherEvent::Delete => panic!("Expected Create event, got Delete"),
        }
    }

    #[tokio::test]
    async fn test_socket_delete_event() {
        let (_temp_dir, socket_path) = setup_test_dir();

        fs::write(&socket_path, b"").expect("Failed to create socket file");

        let mut watcher = SocketWatcher::try_new(&socket_path).expect("Failed to create watcher");

        tokio::time::sleep(Duration::from_millis(100)).await;

        fs::remove_file(&socket_path).expect("Failed to delete socket file");

        let result = tokio::time::timeout(Duration::from_secs(2), watcher.next()).await;

        assert!(result.is_ok(), "Should receive event within timeout");
        let event = result.unwrap();
        assert!(event.is_some(), "Stream should not be closed");

        let event = event.unwrap();
        assert!(event.is_ok(), "Event should not be an error");

        match event.unwrap() {
            SocketWatcherEvent::Delete => {}
            SocketWatcherEvent::Create(_) => panic!("Expected Delete event, got Create"),
        }
    }

    #[tokio::test]
    async fn test_socket_moved_from_terminates_stream() {
        let (_temp_dir, socket_path) = setup_test_dir();

        fs::write(&socket_path, b"").expect("Failed to create socket file");

        let mut watcher = SocketWatcher::try_new(&socket_path).expect("Failed to create watcher");

        tokio::time::sleep(Duration::from_millis(100)).await;

        let moved_path = socket_path.with_extension("moved");
        fs::rename(&socket_path, &moved_path).expect("Failed to move socket file");

        let result = tokio::time::timeout(Duration::from_secs(2), watcher.next()).await;

        assert!(result.is_ok(), "Should receive event within timeout");
        let event = result.unwrap();
        assert!(
            event.is_none(),
            "Stream should terminate when socket is moved"
        );
    }

    #[tokio::test]
    async fn test_parent_directory_deletion_terminates_stream() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let nested_dir = temp_dir.path().join("nested");
        fs::create_dir(&nested_dir).expect("Failed to create nested dir");

        let socket_path = nested_dir.join("test.sock");
        let mut watcher = SocketWatcher::try_new(&socket_path).expect("Failed to create watcher");

        tokio::time::sleep(Duration::from_millis(100)).await;

        fs::remove_dir(&nested_dir).expect("Failed to remove nested dir");

        let result = tokio::time::timeout(Duration::from_secs(2), watcher.next()).await;

        assert!(result.is_ok(), "Should receive event within timeout");
        let event = result.unwrap();
        assert!(
            event.is_none(),
            "Stream should terminate when parent directory is deleted"
        );
    }

    #[tokio::test]
    async fn test_unrelated_file_events_ignored() {
        let (_temp_dir, socket_path) = setup_test_dir();

        let mut watcher = SocketWatcher::try_new(&socket_path).expect("Failed to create watcher");

        let other_file = socket_path.with_file_name("other.txt");
        fs::write(&other_file, b"test").expect("Failed to create other file");

        let result = tokio::time::timeout(Duration::from_millis(500), watcher.next()).await;

        assert!(
            result.is_err(),
            "Should timeout because unrelated file events are ignored"
        );
    }

    #[tokio::test]
    async fn test_similar_filename_ignored() {
        let (_temp_dir, socket_path) = setup_test_dir();

        let mut watcher = SocketWatcher::try_new(&socket_path).expect("Failed to create watcher");

        let similar_file = socket_path.with_extension("sock.bak");
        fs::write(&similar_file, b"test").expect("Failed to create similar file");

        let result = tokio::time::timeout(Duration::from_millis(500), watcher.next()).await;

        assert!(
            result.is_err(),
            "Should timeout because similar filename is ignored"
        );
    }

    #[tokio::test]
    async fn test_multiple_events_sequence() {
        let (_temp_dir, socket_path) = setup_test_dir();

        let mut watcher = SocketWatcher::try_new(&socket_path).expect("Failed to create watcher");

        fs::write(&socket_path, b"").expect("Failed to create socket file");

        let result = tokio::time::timeout(Duration::from_secs(2), watcher.next()).await;
        assert!(result.is_ok(), "Should receive create event");
        let event = result.unwrap().unwrap().unwrap();
        assert!(
            matches!(event, SocketWatcherEvent::Create(_)),
            "First event should be Create"
        );

        fs::remove_file(&socket_path).expect("Failed to delete socket file");

        let result = tokio::time::timeout(Duration::from_secs(2), watcher.next()).await;
        assert!(result.is_ok(), "Should receive delete event");
        let event = result.unwrap().unwrap().unwrap();
        assert!(
            matches!(event, SocketWatcherEvent::Delete),
            "Second event should be Delete"
        );

        fs::write(&socket_path, b"").expect("Failed to create socket file again");

        let result = tokio::time::timeout(Duration::from_secs(2), watcher.next()).await;
        assert!(result.is_ok(), "Should receive second create event");
        let event = result.unwrap().unwrap().unwrap();
        assert!(
            matches!(event, SocketWatcherEvent::Create(_)),
            "Third event should be Create"
        );
    }
}
