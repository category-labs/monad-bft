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
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use monoio::{
    io::AsyncWriteRentExt,
    net::TcpStream,
    spawn,
    time::{sleep, timeout},
};
use tokio::sync::mpsc::{
    self,
    error::{TryRecvError, TrySendError},
};
use tracing::{debug, enabled, trace, warn, Level};
use zerocopy::IntoBytes;

use super::{message_timeout, TcpConfig, TcpMsg, TcpMsgHdr, TCP_MESSAGE_LENGTH_LIMIT};
use crate::addrlist::{Addrlist, Status};

// These are per-peer limits.
pub const QUEUED_MESSAGE_WARN_LIMIT: usize = 100;
// should be higher than MAX_UNACKNOWLEDGED_RESPONSES
pub const QUEUED_MESSAGE_LIMIT: usize = 150;
pub const QUEUED_MESSAGE_BYTE_LIMIT: usize = 4 * 1024 * 1024;

pub const MSG_WAIT_TIMEOUT: Duration = Duration::from_secs(1);

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

struct BoundedQueueReceiver {
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
struct TxState {
    inner: Rc<RefCell<TxStateInner>>,
    addrlist: Arc<Addrlist>,
    connections_limit: usize,
}

impl TxState {
    fn new(addrlist: Arc<Addrlist>, connections_limit: usize) -> TxState {
        let inner = Rc::new(RefCell::new(TxStateInner {
            peer_channels: BTreeMap::new(),
        }));

        TxState {
            inner,
            addrlist,
            connections_limit,
        }
    }

    fn push(
        &self,
        addr: &SocketAddr,
        msg: TcpMsg,
    ) -> Option<(BoundedQueueReceiver, TxStatePeerHandle)> {
        let mut ret = None;

        let mut inner_ref = self.inner.borrow_mut();

        let is_new_peer = !inner_ref.peer_channels.contains_key(addr);
        if is_new_peer {
            let is_trusted = self.addrlist.status(&addr.ip()) == Status::Trusted;
            if !is_trusted && inner_ref.peer_channels.len() >= self.connections_limit {
                warn!(
                    ?addr,
                    total_connections = inner_ref.peer_channels.len(),
                    connections_limit = self.connections_limit,
                    "outgoing connection limit reached, dropping message"
                );
                return None;
            }
        }

        let msg_sender = inner_ref.peer_channels.entry(*addr).or_insert_with(|| {
            let (sender, receiver) = bounded_queue();

            ret = Some((
                receiver,
                TxStatePeerHandle {
                    tx_state: self.clone(),
                    addr: *addr,
                },
            ));

            sender
        });

        match msg_sender.try_send(msg) {
            Ok(()) => {
                let message_count = msg_sender.message_count();
                if message_count >= QUEUED_MESSAGE_WARN_LIMIT {
                    warn!(
                        ?addr,
                        message_count, "excessive number of messages queued for peer"
                    );
                }
            }
            Err(BoundedQueueError::ByteLimitExceeded) => {
                warn!(
                    ?addr,
                    queued_bytes = msg_sender.queued_bytes(),
                    byte_limit = QUEUED_MESSAGE_BYTE_LIMIT,
                    "peer byte limit reached, dropping message"
                );
            }
            Err(BoundedQueueError::Full) => {
                warn!(
                    ?addr,
                    message_count = msg_sender.message_count(),
                    message_limit = QUEUED_MESSAGE_LIMIT,
                    "peer message limit reached, dropping message"
                );
            }
            Err(BoundedQueueError::Closed) => {
                warn!(?addr, "channel unexpectedly closed");
            }
        }

        ret
    }
}

struct TxStatePeerHandle {
    tx_state: TxState,
    addr: SocketAddr,
}

impl Drop for TxStatePeerHandle {
    fn drop(&mut self) {
        self.tx_state
            .inner
            .borrow_mut()
            .peer_channels
            .remove(&self.addr);
        trace!(?self.addr, "removed peer from tx channels map");
    }
}

struct TxStateInner {
    // There is a transmit connection task running for a given peer iff there is an
    // entry for the peer address in this map.  Exiting the transmit connection task
    // drops a TxStatePeerHandle which removes the entry from this map.
    peer_channels: BTreeMap<SocketAddr, BoundedQueueSender>,
}

pub(crate) async fn task(
    cfg: TcpConfig,
    addrlist: Arc<Addrlist>,
    mut tcp_egress_rx: mpsc::Receiver<(SocketAddr, TcpMsg)>,
) {
    let tx_state = TxState::new(addrlist, cfg.connections_limit);

    let mut conn_id: u64 = 0;

    while let Some((addr, msg)) = tcp_egress_rx.recv().await {
        debug!(?addr, len = msg.msg.len(), "queueing up TCP message");

        if let Some((msg_receiver, tx_state_peer_handle)) = tx_state.push(&addr, msg) {
            let peer_count = tx_state.inner.borrow().peer_channels.len();
            trace!(
                conn_id,
                ?addr,
                total_tx_connections = peer_count,
                "spawning tcp transmit connection task for peer"
            );

            spawn(task_connection(
                conn_id,
                addr,
                msg_receiver,
                tx_state_peer_handle,
            ));

            conn_id += 1;
        }
    }
}

async fn task_connection(
    conn_id: u64,
    addr: SocketAddr,
    mut msg_receiver: BoundedQueueReceiver,
    _tx_state_peer_handle: TxStatePeerHandle,
) {
    trace!(
        conn_id,
        ?addr,
        "starting tcp transmit connection task for peer"
    );

    if let Err(err) = connect_and_send_messages(conn_id, &addr, &mut msg_receiver).await {
        let mut additional_messages_dropped = 0;

        // Throw away (and fail) all remaining queued messages immediately.
        while msg_receiver.try_recv().is_ok() {
            additional_messages_dropped += 1;
        }

        warn!(
            conn_id,
            ?addr,
            ?err,
            additional_messages_dropped,
            "error transmitting tcp message"
        );

        // Sleep to avoid reconnecting too soon.
        sleep(TCP_FAILURE_LINGER_WAIT).await;
    }

    trace!(
        conn_id,
        ?addr,
        "exiting tcp transmit connection task for peer"
    );
}

async fn connect_and_send_messages(
    conn_id: u64,
    addr: &SocketAddr,
    msg_receiver: &mut BoundedQueueReceiver,
) -> Result<(), Error> {
    let mut stream = timeout(TCP_CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .unwrap_or_else(|_| Err(Error::from(ErrorKind::TimedOut)))
        .map_err(|err| Error::other(format!("error connecting to remote host: {}", err)))?;

    trace!(conn_id, ?addr, "outbound tcp connection established");

    conn_cork(stream.as_raw_fd(), true);

    let mut message_id: u64 = 0;

    loop {
        let msg = match msg_receiver.try_recv() {
            Ok(msg) => msg,
            Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {
                conn_cork(stream.as_raw_fd(), false);

                match timeout(MSG_WAIT_TIMEOUT, msg_receiver.recv()).await {
                    Ok(None) | Err(_) => break,
                    Ok(Some(msg)) => {
                        conn_cork(stream.as_raw_fd(), true);
                        msg
                    }
                }
            }
        };

        let len = msg.msg.len();

        if len > TCP_MESSAGE_LENGTH_LIMIT {
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

        timeout(
            message_timeout(len),
            send_message(conn_id, addr, &mut stream, message_id, msg),
        )
        .await
        .unwrap_or_else(|_| Err(Error::from(ErrorKind::TimedOut)))
        .map_err(|err| {
            Error::other(format!(
                "error writing message {} on TCP connection: {}",
                message_id, err
            ))
        })?;

        message_id += 1;
    }

    Ok(())
}

fn conn_cork(raw_fd: RawFd, cork_flag: bool) {
    let r = unsafe {
        let cork_flag: libc::c_int = if cork_flag { 1 } else { 0 };

        libc::setsockopt(
            raw_fd,
            libc::SOL_TCP,
            libc::TCP_CORK,
            &cork_flag as *const _ as _,
            std::mem::size_of_val(&cork_flag) as _,
        )
    };

    if r != 0 {
        warn!(
            "setsockopt(TCP_CORK) failed with: {}",
            Error::last_os_error()
        );
    }
}

async fn send_message(
    conn_id: u64,
    addr: &SocketAddr,
    stream: &mut TcpStream,
    message_id: u64,
    message: TcpMsg,
) -> Result<(), Error> {
    trace!(
        conn_id,
        ?addr,
        message_id,
        len = message.msg.len(),
        "start transmission of TCP message"
    );

    let start = if enabled!(Level::DEBUG) {
        Some((Instant::now(), num_unacked_bytes(stream.as_raw_fd())))
    } else {
        None
    };

    let message_len = message.msg.len();

    let header = TcpMsgHdr::new(message_len as u64);

    let (ret, _header) = stream.write_all(Box::<[u8]>::from(header.as_bytes())).await;
    ret?;

    let (ret, _message) = stream.write_all(message.msg).await;
    ret?;

    if let Some((start_time, start_unacked_bytes)) = start {
        let end_unacked_bytes = num_unacked_bytes(stream.as_raw_fd());

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

fn num_unacked_bytes(raw_fd: RawFd) -> usize {
    let mut outq: libc::c_int = 0;

    let r = unsafe { libc::ioctl(raw_fd, libc::TIOCOUTQ, &mut outq as *mut libc::c_int) };

    if r == 0 {
        outq as _
    } else {
        warn!("ioctl(TIOCOUTQ) failed with: {}", Error::last_os_error());
        0
    }
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
