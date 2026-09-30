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
    cell::RefCell,
    collections::BTreeMap,
    io::{Error, ErrorKind},
    net::SocketAddr,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use monoio::{
    net::TcpStream,
    select, spawn,
    time::{sleep, timeout},
};
use tokio::sync::mpsc::{
    self,
    error::{TryRecvError, TrySendError},
};
use tracing::{debug, enabled, trace, warn, Level};
use zerocopy::IntoBytes;

use super::{
    message_timeout, rx::RxContext, task_connection, ConnectionIdle, ConnectionOrigin,
    TcpConnectionGuard, TcpMsg, TcpMsgHdr, TcpWriteHalf, TCP_MESSAGE_LENGTH_LIMIT,
};
use crate::{
    addrlist::{Addrlist, Status},
    metrics::{ActiveConnectionGuard, DataplaneMetrics},
    TcpSocketId,
};

// These are per-peer limits.
pub const QUEUED_MESSAGE_WARN_LIMIT: usize = 100;
// should be higher than MAX_UNACKNOWLEDGED_RESPONSES
pub const QUEUED_MESSAGE_LIMIT: usize = 150;
pub const QUEUED_MESSAGE_BYTE_LIMIT: usize = 4 * 1024 * 1024;

const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_FAILURE_LINGER_WAIT: Duration = Duration::from_secs(1);

enum BoundedQueueError {
    ByteLimitExceeded,
    Full,
    Closed,
}

struct BoundedQueueSender {
    tx: mpsc::Sender<TcpMsg>,
    queued_bytes: Arc<AtomicUsize>,
}

pub(crate) struct BoundedQueueReceiver {
    rx: mpsc::Receiver<TcpMsg>,
    queued_bytes: Arc<AtomicUsize>,
}

fn bounded_queue() -> (BoundedQueueSender, BoundedQueueReceiver) {
    let (sender, receiver) = mpsc::channel(QUEUED_MESSAGE_LIMIT);
    let queued_bytes = Arc::new(AtomicUsize::new(0));
    (
        BoundedQueueSender {
            tx: sender,
            queued_bytes: queued_bytes.clone(),
        },
        BoundedQueueReceiver {
            rx: receiver,
            queued_bytes,
        },
    )
}

impl BoundedQueueSender {
    fn try_send(&self, msg: TcpMsg) -> Result<(), BoundedQueueError> {
        let msg_len = msg.msg.len();
        let mut current = self.queued_bytes.load(Ordering::Relaxed);
        loop {
            if msg_len > QUEUED_MESSAGE_BYTE_LIMIT.saturating_sub(current) {
                return Err(BoundedQueueError::ByteLimitExceeded);
            }
            let new_value = current + msg_len;
            match self.queued_bytes.compare_exchange_weak(
                current,
                new_value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }

        match self.tx.try_send(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.queued_bytes.fetch_sub(msg_len, Ordering::Relaxed);
                Err(BoundedQueueError::Full)
            }
            Err(TrySendError::Closed(_)) => {
                self.queued_bytes.fetch_sub(msg_len, Ordering::Relaxed);
                Err(BoundedQueueError::Closed)
            }
        }
    }

    fn queued_bytes(&self) -> usize {
        self.queued_bytes.load(Ordering::Relaxed)
    }

    fn message_count(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }
}

impl BoundedQueueReceiver {
    fn try_recv(&mut self) -> Result<TcpMsg, TryRecvError> {
        let msg = self.rx.try_recv()?;
        self.queued_bytes
            .fetch_sub(msg.msg.len(), Ordering::Relaxed);
        Ok(msg)
    }

    async fn recv(&mut self) -> Option<TcpMsg> {
        let msg = self.rx.recv().await?;
        self.queued_bytes
            .fetch_sub(msg.msg.len(), Ordering::Relaxed);
        Some(msg)
    }
}

#[derive(Clone)]
pub(crate) struct TxState {
    inner: Rc<RefCell<TxStateInner>>,
    addrlist: Arc<Addrlist>,
    connections_limit: usize,
    metrics: DataplaneMetrics,
}

pub(crate) enum RegisterResult {
    Rejected,
    Existing,
    New(BoundedQueueReceiver, TxStatePeerHandle),
}

impl TxState {
    pub(crate) fn new(
        addrlist: Arc<Addrlist>,
        connections_limit: usize,
        metrics: DataplaneMetrics,
    ) -> TxState {
        let inner = Rc::new(RefCell::new(TxStateInner {
            peer_channels: BTreeMap::new(),
            outgoing_connections: 0,
            next_connection_id: 0,
        }));

        TxState {
            inner,
            addrlist,
            connections_limit,
            metrics,
        }
    }

    fn try_send(&self, key: &(TcpSocketId, SocketAddr), msg: TcpMsg) -> Option<TcpMsg> {
        let inner_ref = self.inner.borrow();

        let Some(sender) = inner_ref.peer_channels.get(key) else {
            return Some(msg);
        };

        let addr = key.1;
        match sender.try_send(msg) {
            Ok(()) => {
                let message_count = sender.message_count();
                if message_count >= QUEUED_MESSAGE_WARN_LIMIT {
                    warn!(
                        ?addr,
                        message_count, "excessive number of messages queued for peer"
                    );
                }
            }
            Err(BoundedQueueError::ByteLimitExceeded) => {
                self.metrics.tcp_egress_messages_dropped.inc();
                warn!(
                    ?addr,
                    queued_bytes = sender.queued_bytes(),
                    byte_limit = QUEUED_MESSAGE_BYTE_LIMIT,
                    "peer byte limit reached, dropping message"
                );
            }
            Err(BoundedQueueError::Full) => {
                self.metrics.tcp_egress_messages_dropped.inc();
                warn!(
                    ?addr,
                    message_count = sender.message_count(),
                    message_limit = QUEUED_MESSAGE_LIMIT,
                    "peer message limit reached, dropping message"
                );
            }
            Err(BoundedQueueError::Closed) => {
                self.metrics.tcp_egress_messages_dropped.inc();
                warn!(?addr, "channel unexpectedly closed");
            }
        }

        None
    }

    pub(crate) fn register(
        &self,
        key: (TcpSocketId, SocketAddr),
        origin: ConnectionOrigin,
    ) -> RegisterResult {
        let mut inner_ref = self.inner.borrow_mut();

        if inner_ref.peer_channels.contains_key(&key) {
            return RegisterResult::Existing;
        }

        let addr = key.1;
        let outgoing = matches!(origin, ConnectionOrigin::Outgoing);
        if outgoing {
            let is_trusted = self.addrlist.status(&addr.ip()) == Status::Trusted;
            if !is_trusted && inner_ref.outgoing_connections >= self.connections_limit {
                self.metrics.tcp_egress_messages_dropped.inc();
                warn!(
                    ?addr,
                    total_connections = inner_ref.outgoing_connections,
                    connections_limit = self.connections_limit,
                    "outgoing connection limit reached, dropping message"
                );
                return RegisterResult::Rejected;
            }
        }

        let (sender, receiver) = bounded_queue();
        inner_ref.peer_channels.insert(key, sender);
        inner_ref.outgoing_connections += usize::from(outgoing);
        let conn_id = inner_ref.next_connection_id;
        inner_ref.next_connection_id += 1;
        RegisterResult::New(
            receiver,
            TxStatePeerHandle {
                tx_state: self.clone(),
                key,
                origin,
                conn_id,
            },
        )
    }
}

pub(crate) struct TxStatePeerHandle {
    tx_state: TxState,
    key: (TcpSocketId, SocketAddr),
    origin: ConnectionOrigin,
    pub(super) conn_id: u64,
}

impl Drop for TxStatePeerHandle {
    fn drop(&mut self) {
        let mut inner = self.tx_state.inner.borrow_mut();
        inner.peer_channels.remove(&self.key);
        inner.outgoing_connections -=
            usize::from(matches!(self.origin, ConnectionOrigin::Outgoing));
        let addr = self.key.1;
        trace!(?addr, "removed peer from tx channels map");
    }
}

struct TxStateInner {
    // There is a connection task running for a given (socket_id, peer) iff
    // there is an entry in this map. Exiting the connection task drops a
    // TxStatePeerHandle which removes the entry from this map.
    peer_channels: BTreeMap<(TcpSocketId, SocketAddr), BoundedQueueSender>,
    outgoing_connections: usize,
    next_connection_id: u64,
}

pub(crate) async fn task(
    tx_state: TxState,
    mut tcp_egress_rx: mpsc::Receiver<(TcpSocketId, SocketAddr, TcpMsg)>,
    contexts: BTreeMap<TcpSocketId, RxContext>,
) {
    while let Some((socket_id, addr, msg)) = tcp_egress_rx.recv().await {
        debug!(
            ?socket_id,
            ?addr,
            len = msg.msg.len(),
            "queueing up TCP message"
        );
        let key = (socket_id, addr);
        match tx_state.register(key, ConnectionOrigin::Outgoing) {
            RegisterResult::Rejected => continue,
            RegisterResult::Existing => {}
            RegisterResult::New(msg_receiver, peer_handle) => {
                let context = contexts
                    .get(&socket_id)
                    .cloned()
                    .expect("socket_id must have a TCP context");
                spawn(task_connect(context, addr, msg_receiver, peer_handle));
            }
        }
        if tx_state.try_send(&key, msg).is_some() {
            warn!(?socket_id, ?addr, "failed to send message after register");
        }
    }
}

async fn task_connect(
    context: RxContext,
    addr: SocketAddr,
    mut msg_receiver: BoundedQueueReceiver,
    peer_handle: TxStatePeerHandle,
) {
    let conn_id = peer_handle.conn_id;
    let connection = TcpConnectionGuard::new(context.tcp_control_map.clone(), addr, conn_id);
    let metrics = &context.metrics;
    let result = select! {
        biased;
        _ = connection.connection.disconnected() => Ok(()),
        result = async {
            let stream = timeout(TCP_CONNECT_TIMEOUT, TcpStream::connect(addr))
                .await
                .unwrap_or_else(|_| Err(Error::from(ErrorKind::TimedOut)))
                .map_err(|err| {
                    metrics.tcp_outbound_connection_errors.inc();
                    Error::other(format!("error connecting to remote host: {err}"))
                })?;
            let _active_connection = ActiveConnectionGuard::new(
                &metrics.tcp_outbound_connections_established,
                &metrics.tcp_current_outbound_connections,
            );
            task_connection(&context, addr, stream, &mut msg_receiver, &connection).await
        } => result,
    };

    if let Err(err) = result {
        drop_queued_messages(&mut msg_receiver, metrics);
        warn!(conn_id, ?addr, ?err, "error in tcp connection task");
        // Avoid repeatedly reconnecting to a failing peer on every message.
        select! {
            _ = connection.connection.disconnected() => {},
            _ = sleep(TCP_FAILURE_LINGER_WAIT) => {},
        }
    }
    drop_queued_messages(&mut msg_receiver, metrics);
}

pub(super) fn drop_queued_messages(
    receiver: &mut BoundedQueueReceiver,
    metrics: &DataplaneMetrics,
) {
    let mut dropped = 0;
    while receiver.try_recv().is_ok() {
        dropped += 1;
    }
    metrics.tcp_egress_messages_dropped.add(dropped);
}

// Count a message as dropped even if the connection task cancels its write
// because reading failed or an explicit disconnect arrived.
struct PendingMessage<'a> {
    metrics: &'a DataplaneMetrics,
    sent: bool,
}

impl Drop for PendingMessage<'_> {
    fn drop(&mut self) {
        if !self.sent {
            self.metrics.tcp_egress_messages_dropped.inc();
        }
    }
}

pub(super) async fn send_messages(
    conn_id: u64,
    addr: &SocketAddr,
    write_half: &mut TcpWriteHalf,
    msg_receiver: &mut BoundedQueueReceiver,
    metrics: &DataplaneMetrics,
    idle: &ConnectionIdle,
) -> Result<(), Error> {
    write_half.set_cork(true);

    let mut message_id: u64 = 0;

    loop {
        idle.set_tx_idle(true);
        let msg = match msg_receiver.try_recv() {
            Ok(msg) => msg,
            Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {
                write_half.set_cork(false);

                match msg_receiver.recv().await {
                    None => break,
                    Some(msg) => {
                        write_half.set_cork(true);
                        msg
                    }
                }
            }
        };

        idle.set_tx_idle(false);
        let len = msg.msg.len();

        if len > TCP_MESSAGE_LENGTH_LIMIT {
            metrics.tcp_egress_messages_dropped.inc();
            warn!(
                conn_id,
                ?addr,
                message_id,
                message_len = len,
                limit = TCP_MESSAGE_LENGTH_LIMIT,
                "message exceeds size limit, skipping"
            );
            message_id += 1;
            continue;
        }

        let mut pending = PendingMessage {
            metrics,
            sent: false,
        };
        timeout(
            message_timeout(len),
            send_message(conn_id, addr, write_half, message_id, msg, metrics),
        )
        .await
        .unwrap_or_else(|_| Err(Error::from(ErrorKind::TimedOut)))
        .map_err(|err| {
            metrics.tcp_send_errors.inc();
            Error::other(format!(
                "error writing message {message_id} on TCP connection: {err}"
            ))
        })?;

        pending.sent = true;
        message_id += 1;
    }

    Ok(())
}

async fn send_message(
    conn_id: u64,
    addr: &SocketAddr,
    write_half: &mut TcpWriteHalf,
    message_id: u64,
    message: TcpMsg,
    metrics: &DataplaneMetrics,
) -> Result<(), Error> {
    trace!(
        conn_id,
        ?addr,
        message_id,
        len = message.msg.len(),
        "start transmission of TCP message"
    );

    let start = if enabled!(Level::DEBUG) {
        Some((Instant::now(), write_half.unacked_bytes()))
    } else {
        None
    };

    let message_len = message.msg.len();

    let header = TcpMsgHdr::new(message_len as u64);

    let (ret, _header) = write_half
        .write_all(Box::<[u8]>::from(header.as_bytes()))
        .await;
    ret?;

    let (ret, _message) = write_half.write_all(message.msg).await;
    ret?;

    metrics.tcp_messages_sent.inc();
    metrics.tcp_bytes_sent.add(message_len as u64);

    if let Some((start_time, start_unacked_bytes)) = start {
        let end_unacked_bytes = write_half.unacked_bytes();

        let duration = Instant::now() - start_time;

        let duration_ms = duration.as_millis();

        let bytes_per_second = {
            let bytes_acked = start_unacked_bytes + std::mem::size_of::<TcpMsgHdr>() + message_len
                - end_unacked_bytes;
            let duration_f64 = duration.as_secs_f64();

            if duration_f64 >= 0.01 {
                (bytes_acked as f64) / duration_f64
            } else {
                f64::NAN
            }
        };

        debug!(
            conn_id,
            ?addr,
            message_id,
            ?header,
            start_unacked_bytes,
            end_unacked_bytes,
            duration_ms,
            bytes_per_second,
            "successfully transmitted TCP message"
        );
    }

    if message
        .completion
        .is_some_and(|completion| completion.send(()).is_err())
    {
        warn!(
            conn_id,
            ?addr,
            ?header,
            "error sending completion for transmitted TCP message"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn make_msg(size: usize) -> TcpMsg {
        TcpMsg {
            msg: Bytes::from(vec![0u8; size]),
            completion: None,
        }
    }

    #[test]
    fn bounded_queue_limits() {
        let (sender, mut receiver) = bounded_queue();

        assert!(sender.try_send(make_msg(1024)).is_ok());
        assert_eq!(sender.queued_bytes(), 1024);
        assert_eq!(sender.message_count(), 1);

        let msg = receiver.try_recv().unwrap();
        assert_eq!(msg.msg.len(), 1024);
        assert_eq!(sender.queued_bytes(), 0);

        let large_msg_size = QUEUED_MESSAGE_BYTE_LIMIT + 1;
        assert!(matches!(
            sender.try_send(make_msg(large_msg_size)),
            Err(BoundedQueueError::ByteLimitExceeded)
        ));
        assert_eq!(sender.queued_bytes(), 0);

        for _ in 0..QUEUED_MESSAGE_LIMIT {
            assert!(sender.try_send(make_msg(1)).is_ok());
        }
        assert_eq!(sender.message_count(), QUEUED_MESSAGE_LIMIT);
        assert!(matches!(
            sender.try_send(make_msg(1)),
            Err(BoundedQueueError::Full)
        ));

        drop(receiver);
        assert!(matches!(
            sender.try_send(make_msg(1)),
            Err(BoundedQueueError::Closed)
        ));
    }

    #[test]
    fn incoming_connections_do_not_use_outgoing_limit() {
        let state = TxState::new(Arc::new(Addrlist::new()), 1, DataplaneMetrics::new());
        let register = |addr: &str, origin| {
            state.register((TcpSocketId::Raptorcast, addr.parse().unwrap()), origin)
        };

        let RegisterResult::New(_incoming_rx, _incoming_handle) =
            register("127.0.0.1:1000", ConnectionOrigin::Accepted)
        else {
            panic!("incoming connection should be registered");
        };
        let RegisterResult::New(_outgoing_rx, _outgoing_handle) =
            register("127.0.0.1:2000", ConnectionOrigin::Outgoing)
        else {
            panic!("incoming connection should not consume outgoing capacity");
        };
        assert!(matches!(
            register("127.0.0.1:3000", ConnectionOrigin::Outgoing),
            RegisterResult::Rejected
        ));
    }
    #[monoio::test(enable_timer = true)]
    async fn disconnect_after_connect_failure_releases_connection_slot() {
        use std::num::NonZeroU32;

        use super::super::{TcpControl, TcpRateLimit};

        // Use a synchronous close so the connect cannot race an io_uring close.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let metrics = DataplaneMetrics::new();
        let state = TxState::new(Arc::new(Addrlist::new()), 1, metrics.clone());
        let key = (TcpSocketId::Raptorcast, addr);
        let RegisterResult::New(receiver, handle) = state.register(key, ConnectionOrigin::Outgoing)
        else {
            panic!("connection should be registered");
        };
        let (complete, completion) = futures::channel::oneshot::channel();
        assert!(state
            .try_send(
                &key,
                TcpMsg {
                    msg: Bytes::from_static(b"queued while connecting"),
                    completion: Some(complete),
                }
            )
            .is_none());
        let (ingress, _messages) = mpsc::channel(1);
        let control = TcpControl::new();
        let context = RxContext {
            socket_id: key.0,
            rate_limit: TcpRateLimit {
                rps: NonZeroU32::new(1).unwrap(),
                rps_burst: NonZeroU32::new(1).unwrap(),
            },
            tcp_control_map: control.clone(),
            tcp_ingress_tx: ingress,
            metrics: metrics.clone(),
        };
        spawn(task_connect(context, addr, receiver, handle));
        timeout(Duration::from_secs(1), async {
            assert!(completion.await.is_err());
        })
        .await
        .unwrap();
        assert_eq!(metrics.tcp_outbound_connection_errors.get(), 1);
        assert_eq!(metrics.tcp_egress_messages_dropped.get(), 1);
        assert_eq!(metrics.tcp_current_outbound_connections.get(), 0);
        assert_eq!(control.0.lock().unwrap().len(), 1);
        assert_eq!(state.inner.borrow().outgoing_connections, 1);

        // Explicit disconnect must end the failure cooldown immediately.
        control.disconnect_socket(addr.ip(), addr.port());
        timeout(Duration::from_millis(250), async {
            while state.inner.borrow().peer_channels.contains_key(&key) {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(control.0.lock().unwrap().is_empty());
        assert_eq!(state.inner.borrow().outgoing_connections, 0);
        assert!(matches!(
            state.register(key, ConnectionOrigin::Outgoing),
            RegisterResult::New(..)
        ));
    }

    #[monoio::test(enable_timer = true)]
    async fn write_timeout_cancels_reader_and_removes_connection() {
        use std::{num::NonZeroU32, os::fd::AsRawFd};

        use monoio::io::AsyncWriteRentExt;

        use super::super::{TcpControl, TcpRateLimit};

        let listener = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        // A peer that never reads must block this write even on loopback.
        let buffer_size: libc::c_int = 4096;
        for (socket, option) in [
            (stream.as_raw_fd(), libc::SO_SNDBUF),
            (peer.as_raw_fd(), libc::SO_RCVBUF),
        ] {
            assert_eq!(
                unsafe {
                    libc::setsockopt(
                        socket,
                        libc::SOL_SOCKET,
                        option,
                        &buffer_size as *const _ as _,
                        std::mem::size_of_val(&buffer_size) as _,
                    )
                },
                0
            );
        }
        let metrics = DataplaneMetrics::new();
        let state = TxState::new(Arc::new(Addrlist::new()), 1, metrics.clone());
        let key = (TcpSocketId::Raptorcast, addr);
        let RegisterResult::New(receiver, handle) = state.register(key, ConnectionOrigin::Accepted)
        else {
            panic!("connection should be registered");
        };
        let (complete, completion) = futures::channel::oneshot::channel();
        assert!(state
            .try_send(
                &key,
                TcpMsg {
                    msg: Bytes::from(vec![0; TCP_MESSAGE_LENGTH_LIMIT]),
                    completion: Some(complete),
                }
            )
            .is_none());
        let (ingress, _messages) = mpsc::channel(1);
        let control = TcpControl::new();
        let context = RxContext {
            socket_id: key.0,
            rate_limit: TcpRateLimit {
                rps: NonZeroU32::new(1).unwrap(),
                rps_burst: NonZeroU32::new(1).unwrap(),
            },
            tcp_control_map: control.clone(),
            tcp_ingress_tx: ingress,
            metrics: metrics.clone(),
        };
        spawn(async move {
            let _peer_handle = handle;
            let connection = TcpConnectionGuard::new(
                context.tcp_control_map.clone(),
                addr,
                _peer_handle.conn_id,
            );
            let mut receiver = receiver;
            assert!(
                task_connection(&context, addr, stream, &mut receiver, &connection)
                    .await
                    .is_err()
            );
            drop_queued_messages(&mut receiver, &context.metrics);
        });
        // Deliver a full inbound frame halfway through the blocked write so
        // the preserved reader deadline cannot mask a missing write timeout.
        monoio::time::sleep(Duration::from_secs(5)).await;
        let frame = [TcpMsgHdr::new(1).as_bytes(), &[1]].concat();
        let (result, _) = peer.write_all(frame).await;
        result.unwrap();
        monoio::time::timeout(Duration::from_secs(7), async {
            assert!(completion.await.is_err());
            while state.inner.borrow().peer_channels.contains_key(&key) {
                monoio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(control.0.lock().unwrap().is_empty());
        assert_eq!(state.inner.borrow().outgoing_connections, 0);
        assert_eq!(metrics.tcp_send_errors.get(), 1);
        assert_eq!(metrics.tcp_receive_errors.get(), 0);
        drop(peer);
    }
}
