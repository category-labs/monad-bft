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
    cell::Cell,
    collections::BTreeMap,
    io::{Error, ErrorKind},
    mem::size_of_val,
    net::SocketAddr,
    num::NonZeroU64,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use futures::{
    channel::oneshot,
    future::{AbortHandle, Abortable},
};
use monoio::{
    io::AsyncWriteRentExt,
    net::TcpStream,
    spawn,
    time::{timeout, Instant},
};
use tokio::sync::mpsc;
use tracing::{trace, warn};
use zerocopy::IntoBytes;

use super::{message_timeout, TcpMsgHdr, TCP_MESSAGE_LENGTH_LIMIT};
use crate::{
    addrlist::{Addrlist, Status},
    metrics::{ActiveConnectionGuard, DataplaneMetrics},
    pacing::PacingItem,
    TcpMsg,
};

pub const QUEUED_CHUNK_LIMIT: usize = 128;
pub const QUEUED_MESSAGE_BYTE_LIMIT: usize = 4 * 1024 * 1024;
pub const MSG_WAIT_TIMEOUT: Duration = Duration::from_secs(1);
pub(crate) const TCP_CHUNK_BYTES: usize = 128 * 1024;
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Shared by the pacing remainder and every dispatched chunk. Failure is counted
/// once, after all buffers belonging to the message have been discarded.
struct Completion {
    len: usize,
    completion: Cell<Option<oneshot::Sender<()>>>,
    completed: Cell<bool>,
    metrics: DataplaneMetrics,
}

impl Completion {
    fn new(
        len: usize,
        completion: Option<oneshot::Sender<()>>,
        metrics: DataplaneMetrics,
    ) -> Rc<Self> {
        Rc::new(Self {
            len,
            completion: Cell::new(completion),
            completed: Cell::new(false),
            metrics,
        })
    }

    fn complete(&self) {
        assert!(!self.completed.replace(true), "TCP message completed twice");
        self.metrics.tcp_messages_sent.inc();
        self.metrics.tcp_bytes_sent.add(self.len as u64);
        if let Some(completion) = self.completion.take() {
            let _ = completion.send(());
        }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        if !self.completed.get() {
            self.metrics.tcp_egress_messages_dropped.inc();
        }
    }
}

/// A whole admitted message and its unscheduled remainder. Framing and completion
/// ownership stay here; the scheduler only asks for byte costs and takes chunks.
pub(crate) struct Message {
    payload: Bytes,
    completion: Rc<Completion>,
    first: bool,
}

impl Message {
    pub(crate) fn new(message: TcpMsg, metrics: DataplaneMetrics) -> Option<Self> {
        if message.msg.len() > TCP_MESSAGE_LENGTH_LIMIT {
            metrics.tcp_egress_messages_dropped.inc();
            return None;
        }
        Some(Self {
            completion: Completion::new(message.msg.len(), message.completion, metrics),
            payload: message.msg,
            first: true,
        })
    }

    pub(crate) fn take_chunk(&mut self, bytes: usize) -> Chunk {
        assert!(bytes <= self.next_payload_bytes());
        let payload = self.payload.split_to(bytes);
        let first = std::mem::replace(&mut self.first, false);
        let metrics = &self.completion.metrics;
        metrics.egress_pacing_background_grants.inc();
        metrics
            .egress_pacing_background_granted_bytes
            .add((bytes + chunk_header_bytes(first)) as u64);
        Chunk {
            payload,
            message: Rc::clone(&self.completion),
            first,
            last: self.payload.is_empty(),
        }
    }
}

impl PacingItem for Message {
    fn queued_bytes(&self) -> usize {
        self.payload.len()
    }

    fn next_payload_bytes(&self) -> usize {
        self.payload
            .len()
            .min(TCP_CHUNK_BYTES - chunk_header_bytes(self.first))
    }

    fn next_pacing_bytes(&self) -> usize {
        self.next_payload_bytes() + chunk_header_bytes(self.first)
    }

    fn peer_bytes_per_second(&self, _: NonZeroU64) -> Option<NonZeroU64> {
        None
    }
}

pub(crate) struct Chunk {
    payload: Bytes,
    message: Rc<Completion>,
    first: bool,
    last: bool,
}

const fn chunk_header_bytes(first: bool) -> usize {
    if first {
        std::mem::size_of::<TcpMsgHdr>()
    } else {
        0
    }
}

struct BufferedChunk {
    chunk: Chunk,
    queued_bytes: Rc<Cell<usize>>,
    bytes: usize,
}

impl Drop for BufferedChunk {
    fn drop(&mut self) {
        self.queued_bytes.set(self.queued_bytes.get() - self.bytes);
    }
}

struct PeerWriter {
    id: u64,
    sender: mpsc::Sender<BufferedChunk>,
    queued_bytes: Rc<Cell<usize>>,
    abort: AbortHandle,
}

struct WriterEvent {
    addr: SocketAddr,
    id: u64,
    failed: bool,
}

/// Owned by the pacing task, not a separate TCP routing task. Peer sends are
/// always nonblocking; overflowing one peer cancels only that writer.
pub(crate) struct TxState {
    peers: BTreeMap<SocketAddr, PeerWriter>,
    addrlist: Arc<Addrlist>,
    connections_limit: usize,
    next_id: u64,
    event_tx: mpsc::UnboundedSender<WriterEvent>,
    events: mpsc::UnboundedReceiver<WriterEvent>,
    metrics: DataplaneMetrics,
}

impl TxState {
    pub(crate) fn new(
        addrlist: Arc<Addrlist>,
        connections_limit: usize,
        metrics: DataplaneMetrics,
    ) -> Self {
        let (event_tx, events) = mpsc::unbounded_channel();
        Self {
            peers: BTreeMap::new(),
            addrlist,
            connections_limit,
            next_id: 0,
            event_tx,
            events,
            metrics,
        }
    }

    /// False means the peer's pacing buffers must also be discarded.
    pub(crate) fn try_send(&mut self, addr: SocketAddr, chunk: Chunk) -> bool {
        if !self.peers.contains_key(&addr) {
            // A new stream can only start at a message boundary.
            if !chunk.first {
                return false;
            }
            if self.addrlist.status(&addr.ip()) != Status::Trusted
                && self.peers.len() >= self.connections_limit
            {
                warn!(?addr, "outgoing TCP connection limit reached");
                return false;
            }
            let (sender, receiver) = mpsc::channel(QUEUED_CHUNK_LIMIT);
            let (abort, registration) = AbortHandle::new_pair();
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            let metrics = self.metrics.clone();
            let events = self.event_tx.clone();
            spawn(async move {
                // Aborting drops the receiver and socket, including a pending
                // write. Dropping the sender alone would drain partial frames.
                let result =
                    Abortable::new(write_connection(addr, receiver, &metrics), registration).await;
                let failed = match result {
                    Ok(Ok(())) => false,
                    Ok(Err(err)) => {
                        metrics.tcp_send_errors.inc();
                        warn!(?addr, ?err, "error transmitting TCP message");
                        true
                    }
                    Err(_) => true,
                };
                let _ = events.send(WriterEvent { addr, id, failed });
            });
            self.peers.insert(
                addr,
                PeerWriter {
                    id,
                    sender,
                    queued_bytes: Rc::new(Cell::new(0)),
                    abort,
                },
            );
        }
        let peer = self.peers.get(&addr).expect("created TCP writer");
        let bytes = chunk.payload.len() + chunk_header_bytes(chunk.first);
        if bytes <= QUEUED_MESSAGE_BYTE_LIMIT.saturating_sub(peer.queued_bytes.get()) {
            peer.queued_bytes.set(peer.queued_bytes.get() + bytes);
            let buffered = BufferedChunk {
                chunk,
                queued_bytes: Rc::clone(&peer.queued_bytes),
                bytes,
            };
            if peer.sender.try_send(buffered).is_ok() {
                return true;
            }
        }
        warn!(
            ?addr,
            "TCP writer queue full or closed; dropping peer buffers and disconnecting"
        );
        self.abort(addr);
        false
    }

    fn abort(&mut self, addr: SocketAddr) {
        if let Some(peer) = self.peers.remove(&addr) {
            peer.abort.abort();
        }
    }

    pub(crate) fn try_failed_peer(&mut self) -> Option<SocketAddr> {
        while let Ok(event) = self.events.try_recv() {
            if let Some(addr) = self.handle_event(event) {
                return Some(addr);
            }
        }
        None
    }

    pub(crate) async fn failed_peer(&mut self) -> SocketAddr {
        loop {
            let event = self
                .events
                .recv()
                .await
                .expect("writer event sender is owned by this state");
            if let Some(addr) = self.handle_event(event) {
                return addr;
            }
        }
    }

    fn handle_event(&mut self, event: WriterEvent) -> Option<SocketAddr> {
        // A cancelled writer may finish after its replacement was created.
        if self
            .peers
            .get(&event.addr)
            .is_some_and(|peer| peer.id == event.id)
        {
            self.peers.remove(&event.addr);
            return event.failed.then_some(event.addr);
        }
        None
    }
}

impl Drop for TxState {
    fn drop(&mut self) {
        for peer in self.peers.values() {
            peer.abort.abort();
        }
    }
}

struct ActiveMessage {
    message: Rc<Completion>,
    deadline: Instant,
}

async fn write_connection(
    addr: SocketAddr,
    mut receiver: mpsc::Receiver<BufferedChunk>,
    metrics: &DataplaneMetrics,
) -> Result<(), Error> {
    let mut stream = timeout(TCP_CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .unwrap_or_else(|_| Err(Error::from(ErrorKind::TimedOut)))
        .inspect_err(|_| metrics.tcp_outbound_connection_errors.inc())?;
    let _active = ActiveConnectionGuard::new(
        &metrics.tcp_outbound_connections_established,
        &metrics.tcp_current_outbound_connections,
    );
    set_socket_option(
        stream.as_raw_fd(),
        libc::TCP_NOTSENT_LOWAT,
        TCP_CHUNK_BYTES as u32,
    );
    let mut active: Option<ActiveMessage> = None;
    loop {
        let wait = active.as_ref().map_or(MSG_WAIT_TIMEOUT, |active| {
            active.deadline.saturating_duration_since(Instant::now())
        });
        let buffered = match timeout(wait, receiver.recv()).await {
            Ok(Some(chunk)) => chunk,
            Ok(None) if active.is_none() => return Ok(()),
            Ok(None) => return Err(Error::from(ErrorKind::UnexpectedEof)),
            Err(_) if active.is_none() => return Ok(()),
            Err(_) => return Err(Error::from(ErrorKind::TimedOut)),
        };
        let chunk = &buffered.chunk;
        if chunk.first {
            if active.is_some() {
                return Err(Error::other(
                    "TCP message started before previous message ended",
                ));
            }
            active = Some(ActiveMessage {
                message: Rc::clone(&chunk.message),
                deadline: Instant::now() + message_timeout(chunk.message.len),
            });
        }
        let Some(current) = active
            .as_ref()
            .filter(|active| Rc::ptr_eq(&active.message, &chunk.message))
        else {
            return Err(Error::other(
                "TCP continuation does not match active message",
            ));
        };
        let remaining = current.deadline.saturating_duration_since(Instant::now());
        set_socket_option(stream.as_raw_fd(), libc::TCP_CORK, 1);
        timeout(remaining, async {
            if chunk.first {
                let header = TcpMsgHdr::new(chunk.message.len as u64);
                let (result, _) = stream
                    .write_all(Bytes::copy_from_slice(header.as_bytes()))
                    .await;
                result?;
            }
            let (result, _) = stream.write_all(chunk.payload.clone()).await;
            result
        })
        .await
        .map_err(|_| Error::from(ErrorKind::TimedOut))??;
        set_socket_option(stream.as_raw_fd(), libc::TCP_CORK, 0);
        if chunk.last {
            chunk.message.complete();
            active = None;
            trace!(?addr, "completed TCP message");
        }
        // Releasing the buffer also releases its byte budget. The channel
        // capacity may already be available while this write is in progress.
        drop(buffered);
    }
}

fn set_socket_option(fd: RawFd, option: libc::c_int, value: u32) {
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            option,
            &value as *const _ as _,
            size_of_val(&value) as _,
        )
    };
    if result != 0 {
        warn!(option, error = ?Error::last_os_error(), "TCP setsockopt failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunking_keeps_framing_and_completion_inside_tcp() {
        for len in [
            0,
            1,
            TCP_CHUNK_BYTES - chunk_header_bytes(true),
            TCP_CHUNK_BYTES,
            2 * TCP_CHUNK_BYTES + 9,
        ] {
            let metrics = DataplaneMetrics::new();
            let payload = Bytes::from(vec![7; len]);
            let (completion, mut completed) = oneshot::channel();
            let mut message = Message::new(
                TcpMsg {
                    msg: payload.clone(),
                    completion: Some(completion),
                },
                metrics.clone(),
            )
            .unwrap();
            let mut chunks = Vec::new();
            let mut offset = 0;
            loop {
                let bytes = message.next_payload_bytes();
                assert!(message.next_pacing_bytes() <= TCP_CHUNK_BYTES);
                let chunk = message.take_chunk(bytes);
                assert_eq!(chunk.payload, payload.slice(offset..offset + bytes));
                assert_eq!(chunk.first, chunks.is_empty());
                offset += bytes;
                assert_eq!(chunk.last, offset == len);
                chunks.push(chunk);
                if offset == len {
                    break;
                }
            }
            assert_eq!(message.queued_bytes(), 0);
            assert_eq!(completed.try_recv().unwrap(), None);
            assert_eq!(metrics.tcp_messages_sent.get(), 0);
            assert_eq!(
                metrics.egress_pacing_background_granted_bytes.get(),
                (len + chunk_header_bytes(true)) as u64
            );
            drop(message);
            assert_eq!(completed.try_recv().unwrap(), None);
            // Only the writer's final successful write completes the message.
            chunks.last().unwrap().message.complete();
            assert_eq!(completed.try_recv().unwrap(), Some(()));
            drop(chunks);
            assert_eq!(metrics.tcp_messages_sent.get(), 1);
            assert_eq!(metrics.tcp_bytes_sent.get(), len as u64);
            assert_eq!(metrics.tcp_egress_messages_dropped.get(), 0);
        }
    }

    #[test]
    fn failed_remainder_is_counted_once_after_all_chunks_are_dropped() {
        let metrics = DataplaneMetrics::new();
        let (completion, mut completed) = oneshot::channel();
        let mut message = Message::new(
            TcpMsg {
                msg: vec![7; TCP_CHUNK_BYTES * 2].into(),
                completion: Some(completion),
            },
            metrics.clone(),
        )
        .unwrap();
        let chunk = message.take_chunk(message.next_payload_bytes());
        drop(message);
        assert_eq!(metrics.tcp_egress_messages_dropped.get(), 0);
        assert_eq!(completed.try_recv().unwrap(), None);
        drop(chunk);
        assert_eq!(metrics.tcp_egress_messages_dropped.get(), 1);
        assert!(completed.try_recv().is_err());
    }

    #[test]
    fn continuation_cannot_create_a_new_connection() {
        let mut state = state();
        let mut message = Message::new(
            TcpMsg {
                msg: vec![7; TCP_CHUNK_BYTES].into(),
                completion: None,
            },
            state.metrics.clone(),
        )
        .unwrap();
        drop(message.take_chunk(message.next_payload_bytes()));
        let continuation = message.take_chunk(message.next_payload_bytes());
        assert!(!state.try_send("127.0.0.1:1".parse().unwrap(), continuation));
        assert!(state.peers.is_empty());
        drop(message);
        assert_eq!(state.metrics.tcp_egress_messages_dropped.get(), 1);
    }

    fn state() -> TxState {
        TxState::new(
            Arc::new(Addrlist::new_with_trusted(std::iter::empty())),
            64,
            DataplaneMetrics::new(),
        )
    }

    fn install_writer(
        state: &mut TxState,
        addr: SocketAddr,
        capacity: usize,
        id: u64,
    ) -> (mpsc::Receiver<BufferedChunk>, AbortHandle) {
        let (sender, receiver) = mpsc::channel(capacity);
        let (abort, _registration) = AbortHandle::new_pair();
        state.peers.insert(
            addr,
            PeerWriter {
                id,
                sender,
                queued_bytes: Rc::new(Cell::new(0)),
                abort: abort.clone(),
            },
        );
        (receiver, abort)
    }

    #[test]
    fn queue_overflow_aborts_only_affected_writer() {
        let mut state = state();
        let slow = "127.0.0.1:1".parse().unwrap();
        let healthy = "127.0.0.1:2".parse().unwrap();
        let (slow_rx, abort) = install_writer(&mut state, slow, 1, 0);
        let (mut healthy_rx, healthy_abort) = install_writer(&mut state, healthy, 1, 1);
        let (completion, mut completed) = oneshot::channel();
        let message = Completion::new(2, Some(completion), state.metrics.clone());
        assert!(state.try_send(
            slow,
            Chunk {
                payload: Bytes::from_static(&[1]),
                message: message.clone(),
                first: true,
                last: false
            }
        ));
        assert!(!state.try_send(
            slow,
            Chunk {
                payload: Bytes::from_static(&[2]),
                message: message.clone(),
                first: false,
                last: true
            }
        ));
        assert!(abort.is_aborted());
        assert!(!healthy_abort.is_aborted());
        assert!(!state.peers.contains_key(&slow));
        drop(slow_rx); // What cancelling the real writer does.
        drop(message);
        assert!(completed.try_recv().is_err());
        assert_eq!(state.metrics.tcp_egress_messages_dropped.get(), 1);
        let message = Completion::new(1, None, state.metrics.clone());
        assert!(state.try_send(
            healthy,
            Chunk {
                payload: Bytes::from_static(&[3]),
                message,
                first: true,
                last: true
            }
        ));
        assert_eq!(
            healthy_rx.try_recv().unwrap().chunk.payload,
            Bytes::from_static(&[3])
        );
    }

    #[test]
    fn byte_limit_includes_chunk_currently_being_written() {
        let mut state = state();
        let addr = "127.0.0.1:1".parse().unwrap();
        let (mut receiver, abort) = install_writer(&mut state, addr, QUEUED_CHUNK_LIMIT, 0);
        let message = Completion::new(QUEUED_MESSAGE_BYTE_LIMIT, None, state.metrics.clone());
        let chunk = || Chunk {
            payload: Bytes::from(vec![0; TCP_CHUNK_BYTES]),
            message: message.clone(),
            first: false,
            last: false,
        };
        assert!(state.try_send(addr, chunk()));
        let in_progress = receiver.try_recv().unwrap();
        for _ in 1..QUEUED_MESSAGE_BYTE_LIMIT / TCP_CHUNK_BYTES {
            assert!(state.try_send(addr, chunk()));
        }
        assert_eq!(
            state.peers[&addr].queued_bytes.get(),
            QUEUED_MESSAGE_BYTE_LIMIT
        );
        assert!(!state.try_send(addr, chunk()));
        assert!(abort.is_aborted());
        drop(in_progress);
        drop(receiver);
    }

    #[test]
    fn old_writer_exit_cannot_remove_replacement() {
        let mut state = state();
        let addr = "127.0.0.1:1".parse().unwrap();
        let (_receiver, abort) = install_writer(&mut state, addr, 1, 2);
        assert!(state
            .handle_event(WriterEvent {
                addr,
                id: 1,
                failed: true
            })
            .is_none());
        assert_eq!(state.peers[&addr].id, 2);
        assert!(!abort.is_aborted());
        assert_eq!(
            state.handle_event(WriterEvent {
                addr,
                id: 2,
                failed: true
            }),
            Some(addr)
        );
        assert!(!state.peers.contains_key(&addr));
    }
}
