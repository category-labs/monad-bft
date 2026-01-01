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
    time::{Duration, Instant},
};

use monoio::{
    io::{AsyncWriteRentExt, Splitable},
    net::{
        tcp::{TcpOwnedReadHalf, TcpOwnedWriteHalf},
        TcpStream,
    },
    select, spawn,
    time::{sleep, timeout},
};
use tokio::sync::mpsc::{
    self,
    error::{TryRecvError, TrySendError},
};
use tracing::{debug, enabled, trace, warn, Level};
use zerocopy::IntoBytes;

use super::{message_timeout, TcpMsg, TcpMsgHdr, TCP_MESSAGE_LENGTH_LIMIT};
use crate::TcpSocketId;

pub(crate) type WriteHalfReceiver = mpsc::Receiver<(SocketAddr, RawFd, TcpOwnedWriteHalf)>;
pub(crate) type ReadHalfSenders =
    BTreeMap<TcpSocketId, mpsc::Sender<(SocketAddr, TcpOwnedReadHalf)>>;

// These are per-peer limits.
pub const QUEUED_MESSAGE_WARN_LIMIT: usize = 100;
// should be higher than MAX_UNACKNOWLEDGED_RESPONSES
pub const QUEUED_MESSAGE_LIMIT: usize = 150;
// TODO add QUEUED_MESSAGE_BYTE_LIMIT

pub const MSG_WAIT_TIMEOUT: Duration = Duration::from_secs(4);

const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_FAILURE_LINGER_WAIT: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct TxState {
    inner: Rc<RefCell<TxStateInner>>,
}

enum RegisterResult {
    Existing,
    New(mpsc::Receiver<TcpMsg>, TxStatePeerHandle),
}

impl TxState {
    fn new() -> TxState {
        let inner = Rc::new(RefCell::new(TxStateInner {
            peer_channels: BTreeMap::new(),
        }));

        TxState { inner }
    }

    fn try_send(&self, addr: &SocketAddr, msg: TcpMsg) -> Option<TcpMsg> {
        let inner_ref = self.inner.borrow();

        if let Some(sender) = inner_ref.peer_channels.get(addr) {
            match sender.try_send(msg) {
                Ok(()) => {
                    let message_count = sender.max_capacity() - sender.capacity();

                    if message_count >= QUEUED_MESSAGE_WARN_LIMIT {
                        warn!(
                            ?addr,
                            message_count, "excessive number of messages queued for peer"
                        );
                    }
                    None
                }
                Err(TrySendError::Full(msg)) => {
                    warn!(
                        ?addr,
                        message_count = sender.max_capacity(),
                        "peer message limit reached, dropping message"
                    );
                    Some(msg)
                }
                Err(TrySendError::Closed(msg)) => {
                    warn!(?addr, "channel unexpectedly closed");
                    Some(msg)
                }
            }
        } else {
            Some(msg)
        }
    }

    fn register(&self, addr: &SocketAddr) -> RegisterResult {
        let mut inner_ref = self.inner.borrow_mut();

        let mut ret = RegisterResult::Existing;

        inner_ref.peer_channels.entry(*addr).or_insert_with(|| {
            let (sender, receiver) = mpsc::channel(QUEUED_MESSAGE_LIMIT);

            ret = RegisterResult::New(
                receiver,
                TxStatePeerHandle {
                    tx_state: self.clone(),
                    addr: *addr,
                },
            );

            sender
        });

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
    peer_channels: BTreeMap<SocketAddr, mpsc::Sender<TcpMsg>>,
}

pub async fn task(
    mut tcp_egress_rx: mpsc::Receiver<(TcpSocketId, SocketAddr, TcpMsg)>,
    mut write_half_rx: WriteHalfReceiver,
    read_half_txs: ReadHalfSenders,
) {
    let tx_state = TxState::new();

    let mut conn_id: u64 = 0;

    loop {
        select! {
            egress = tcp_egress_rx.recv() => {
                let Some((socket_id, addr, msg)) = egress else {
                    break;
                };
                debug!(?socket_id, ?addr, len = msg.msg.len(), "queueing up TCP message");

                if let RegisterResult::New(msg_receiver, tx_state_peer_handle) = tx_state.register(&addr) {
                    let peer_count = tx_state.inner.borrow().peer_channels.len();
                    trace!(
                        conn_id,
                        ?socket_id,
                        ?addr,
                        total_tx_connections = peer_count,
                        "spawning tcp connect and send task for peer"
                    );

                    let read_half_tx = read_half_txs.get(&socket_id).cloned();

                    spawn(task_connect_and_send(
                        conn_id,
                        addr,
                        msg_receiver,
                        tx_state_peer_handle,
                        read_half_tx,
                    ));

                    conn_id += 1;
                }

                if tx_state.try_send(&addr, msg).is_some() {
                    warn!(?addr, "failed to send message after register");
                }
            }
            write_half = write_half_rx.recv() => {
                let Some((addr, raw_fd, write_half)) = write_half else {
                    break;
                };
                trace!(conn_id, ?addr, "received write half from rx");

                if let RegisterResult::New(msg_receiver, tx_state_peer_handle) = tx_state.register(&addr) {
                    let peer_count = tx_state.inner.borrow().peer_channels.len();
                    trace!(
                        conn_id,
                        ?addr,
                        total_tx_connections = peer_count,
                        "spawning tcp send task for peer"
                    );

                    spawn(task_send(
                        conn_id,
                        addr,
                        raw_fd,
                        write_half,
                        msg_receiver,
                        tx_state_peer_handle,
                    ));

                    conn_id += 1;
                } else {
                    warn!(?addr, "received write half for already registered peer");
                }
            }
        }
    }
}

async fn task_send(
    conn_id: u64,
    addr: SocketAddr,
    raw_fd: RawFd,
    write_half: TcpOwnedWriteHalf,
    mut msg_receiver: mpsc::Receiver<TcpMsg>,
    _tx_state_peer_handle: TxStatePeerHandle,
) {
    trace!(conn_id, ?addr, "starting tcp send task");

    if let Err(err) = send_messages(conn_id, &addr, raw_fd, write_half, &mut msg_receiver).await {
        let mut additional_messages_dropped = 0;
        while let Ok(_msg) = msg_receiver.try_recv() {
            additional_messages_dropped += 1;
        }

        warn!(
            conn_id,
            ?addr,
            ?err,
            additional_messages_dropped,
            "error transmitting tcp message"
        );
    }

    trace!(conn_id, ?addr, "exiting tcp send task");
}

async fn task_connect_and_send(
    conn_id: u64,
    addr: SocketAddr,
    mut msg_receiver: mpsc::Receiver<TcpMsg>,
    _tx_state_peer_handle: TxStatePeerHandle,
    read_half_tx: Option<mpsc::Sender<(SocketAddr, TcpOwnedReadHalf)>>,
) {
    trace!(conn_id, ?addr, "starting tcp connect and send task");

    if let Err(err) =
        connect_and_send_messages(conn_id, &addr, &mut msg_receiver, read_half_tx).await
    {
        let mut additional_messages_dropped = 0;
        while let Ok(_msg) = msg_receiver.try_recv() {
            additional_messages_dropped += 1;
        }

        warn!(
            conn_id,
            ?addr,
            ?err,
            additional_messages_dropped,
            "error transmitting tcp message"
        );

        sleep(TCP_FAILURE_LINGER_WAIT).await;
    }

    trace!(conn_id, ?addr, "exiting tcp connect and send task");
}

async fn connect_and_send_messages(
    conn_id: u64,
    addr: &SocketAddr,
    msg_receiver: &mut mpsc::Receiver<TcpMsg>,
    read_half_tx: Option<mpsc::Sender<(SocketAddr, TcpOwnedReadHalf)>>,
) -> Result<(), Error> {
    let stream = timeout(TCP_CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .unwrap_or_else(|_| Err(Error::from(ErrorKind::TimedOut)))
        .map_err(|err| Error::other(format!("error connecting to remote host: {}", err)))?;

    trace!(conn_id, ?addr, "outbound tcp connection established");

    let raw_fd = stream.as_raw_fd();
    let (read_half, write_half) = stream.into_split();

    if let Some(read_half_tx) = read_half_tx {
        if let Err(err) = read_half_tx.send((*addr, read_half)).await {
            warn!(conn_id, ?addr, ?err, "failed to send read half to rx task");
        }
    }

    send_messages(conn_id, addr, raw_fd, write_half, msg_receiver).await
}

async fn send_messages(
    conn_id: u64,
    addr: &SocketAddr,
    raw_fd: RawFd,
    mut write_half: TcpOwnedWriteHalf,
    msg_receiver: &mut mpsc::Receiver<TcpMsg>,
) -> Result<(), Error> {
    conn_cork(raw_fd, true);

    let mut message_id: u64 = 0;

    loop {
        let msg = match msg_receiver.try_recv() {
            Ok(msg) => msg,
            Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {
                conn_cork(raw_fd, false);

                match timeout(MSG_WAIT_TIMEOUT, msg_receiver.recv()).await {
                    Ok(None) => break,
                    Ok(Some(msg)) => {
                        conn_cork(raw_fd, true);
                        msg
                    }
                    Err(_) => break,
                }
            }
        };

        let message_count = msg_receiver.max_capacity() - msg_receiver.capacity();

        if message_count >= QUEUED_MESSAGE_WARN_LIMIT {
            warn!(
                ?addr,
                message_count, "excessive number of messages queued for peer"
            );
        }

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
            send_message(conn_id, addr, raw_fd, &mut write_half, message_id, msg),
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
    raw_fd: RawFd,
    write_half: &mut TcpOwnedWriteHalf,
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
        Some((Instant::now(), num_unacked_bytes(raw_fd)))
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

    if let Some((start_time, start_unacked_bytes)) = start {
        let end_unacked_bytes = num_unacked_bytes(raw_fd);

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
