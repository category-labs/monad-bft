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
    net::{IpAddr, SocketAddr},
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
    conn::{ConnectionRegistration, ConnectionRegistry},
    message_timeout, task_connection, ConnectionContext, ConnectionIdle, IoState,
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

pub(super) struct BoundedQueueSender {
    tx: mpsc::Sender<TcpMsg>,
    queued_bytes: Arc<AtomicUsize>,
}

pub(crate) struct BoundedQueueReceiver {
    rx: mpsc::Receiver<TcpMsg>,
    queued_bytes: Arc<AtomicUsize>,
}

pub(super) fn bounded_queue() -> (BoundedQueueSender, BoundedQueueReceiver) {
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

    pub(super) fn enqueue(&self, addr: SocketAddr, msg: TcpMsg, metrics: &DataplaneMetrics) {
        match self.try_send(msg) {
            Ok(()) => {
                let message_count = self.message_count();
                if message_count >= QUEUED_MESSAGE_WARN_LIMIT {
                    warn!(
                        ?addr,
                        message_count, "excessive number of messages queued for peer"
                    );
                }
            }
            Err(BoundedQueueError::ByteLimitExceeded) => {
                metrics.tcp_egress_messages_dropped.inc();
                warn!(
                    ?addr,
                    queued_bytes = self.queued_bytes(),
                    byte_limit = QUEUED_MESSAGE_BYTE_LIMIT,
                    "peer byte limit reached, dropping message"
                );
            }
            Err(BoundedQueueError::Full) => {
                metrics.tcp_egress_messages_dropped.inc();
                warn!(
                    ?addr,
                    message_count = self.message_count(),
                    message_limit = QUEUED_MESSAGE_LIMIT,
                    "peer message limit reached, dropping message"
                );
            }
            Err(BoundedQueueError::Closed) => {
                metrics.tcp_egress_messages_dropped.inc();
                warn!(?addr, "channel unexpectedly closed");
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
pub(crate) struct OutgoingLimit {
    num_connections: Rc<Cell<usize>>,
    addrlist: Arc<Addrlist>,
    connections_limit: usize,
    metrics: DataplaneMetrics,
}

impl OutgoingLimit {
    pub(crate) fn new(
        addrlist: Arc<Addrlist>,
        connections_limit: usize,
        metrics: DataplaneMetrics,
    ) -> Self {
        Self {
            num_connections: Rc::new(Cell::new(0)),
            addrlist,
            connections_limit,
            metrics,
        }
    }

    fn reject_banned(&self, addr: SocketAddr) -> bool {
        if self.addrlist.status(&addr.ip()) != Status::Banned {
            return false;
        }
        self.metrics.tcp_egress_messages_dropped.inc();
        self.metrics.tcp_egress_messages_dropped_banned.inc();
        debug!(?addr, "banned address, dropping tcp message");
        true
    }

    fn try_acquire(&self, addr: SocketAddr) -> Option<OutgoingPermit> {
        if self.reject_banned(addr) {
            return None;
        }
        let is_trusted = self.addrlist.status(&addr.ip()) == Status::Trusted;
        let count = self.num_connections.get();
        if !is_trusted && count >= self.connections_limit {
            self.metrics.tcp_egress_messages_dropped.inc();
            warn!(
                ?addr,
                total_connections = count,
                connections_limit = self.connections_limit,
                "outgoing connection limit reached, dropping message"
            );
            return None;
        }
        self.num_connections.set(count + 1);
        Some(OutgoingPermit {
            limit: self.clone(),
            ip: addr.ip(),
        })
    }
}

// Reserve outgoing capacity through dialing, I/O, and the failure cooldown.
struct OutgoingPermit {
    limit: OutgoingLimit,
    ip: IpAddr,
}

impl OutgoingPermit {
    fn is_banned(&self) -> bool {
        self.limit.addrlist.status(&self.ip) == Status::Banned
    }
}

impl Drop for OutgoingPermit {
    fn drop(&mut self) {
        self.limit
            .num_connections
            .set(self.limit.num_connections.get() - 1);
    }
}

pub(crate) async fn task(
    outgoing_limit: OutgoingLimit,
    registry: ConnectionRegistry,
    mut tcp_egress_rx: mpsc::Receiver<(TcpSocketId, SocketAddr, TcpMsg)>,
    contexts: BTreeMap<TcpSocketId, ConnectionContext>,
) {
    while let Some((socket_id, addr, msg)) = tcp_egress_rx.recv().await {
        debug!(
            ?socket_id,
            ?addr,
            len = msg.msg.len(),
            "queueing up TCP message"
        );
        if outgoing_limit.reject_banned(addr) {
            continue;
        }
        let key = (socket_id, addr);
        // Replies and subsequent sends reuse a registered connection without
        // acquiring another outgoing slot, even when the outgoing budget is full.
        let Some(msg) = registry.try_send(&key, msg, &outgoing_limit.metrics) else {
            continue;
        };
        let Some(permit) = outgoing_limit.try_acquire(addr) else {
            continue;
        };
        let (receiver, registration) = registry
            .register(key)
            .expect("connection cannot be registered between lookup and insertion on this thread");
        if registry
            .try_send(&key, msg, &outgoing_limit.metrics)
            .is_some()
        {
            warn!(?socket_id, ?addr, "failed to send message after register");
        }
        let context = contexts
            .get(&socket_id)
            .cloned()
            .expect("socket_id must have a TCP context");
        spawn(task_connect(context, addr, receiver, registration, permit));
    }
}

async fn task_connect(
    context: ConnectionContext,
    addr: SocketAddr,
    mut msg_receiver: BoundedQueueReceiver,
    registration: ConnectionRegistration,
    permit: OutgoingPermit,
) {
    let conn_id = registration.conn_id;
    let connection = TcpConnectionGuard::new(context.tcp_control_map.clone(), addr, conn_id);
    let metrics = &context.metrics;
    if permit.is_banned() {
        debug!(
            conn_id,
            ?addr,
            "banned address, dropping outgoing tcp connection"
        );
        let dropped = drop_queued_messages(&mut msg_receiver, metrics);
        metrics.tcp_egress_messages_dropped_banned.add(dropped);
        return;
    }
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
        // Keep the registration guard alive during this delay to prevent reconnects.
        // When the task exits, dropping the guard removes the queue from the registry.
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
) -> u64 {
    let mut dropped = 0;
    while receiver.try_recv().is_ok() {
        dropped += 1;
    }
    metrics.tcp_egress_messages_dropped.add(dropped);
    dropped
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
        idle.set_tx_state(IoState::Idle);
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

        idle.set_tx_state(IoState::Busy);
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
}
