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
    sync::Arc,
    time::Duration,
};

use monoio::{
    io::AsyncWriteRentExt,
    net::TcpStream,
    spawn,
    time::{sleep, timeout},
};
use tokio::sync::mpsc;
use tracing::{trace, warn};

use super::{message_timeout, TcpConfig};
use crate::{
    addrlist::{Addrlist, Status},
    metrics::{ActiveConnectionGuard, DataplaneMetrics},
    pacing::{TcpDispatch, TCP_DISPATCH_PAYLOAD_BYTES},
};

pub const QUEUED_MESSAGE_WARN_LIMIT: usize = 100;
pub const QUEUED_MESSAGE_LIMIT: usize = 150;
pub const QUEUED_MESSAGE_BYTE_LIMIT: usize = 4 * 1024 * 1024;

pub const MSG_WAIT_TIMEOUT: Duration = Duration::from_secs(1);

const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_FAILURE_LINGER_WAIT: Duration = Duration::from_secs(1);
const PEER_DISPATCH_CHANNEL_SIZE: usize = 8;

#[derive(Clone)]
struct TxState {
    inner: Rc<RefCell<TxStateInner>>,
    addrlist: Arc<Addrlist>,
    connections_limit: usize,
}

impl TxState {
    fn new(addrlist: Arc<Addrlist>, connections_limit: usize) -> Self {
        Self {
            inner: Rc::new(RefCell::new(TxStateInner {
                peer_channels: BTreeMap::new(),
            })),
            addrlist,
            connections_limit,
        }
    }

    fn sender(
        &self,
        addr: SocketAddr,
        create: bool,
    ) -> Option<(
        mpsc::Sender<TcpDispatch>,
        Option<(mpsc::Receiver<TcpDispatch>, TxStatePeerHandle)>,
    )> {
        let mut inner = self.inner.borrow_mut();
        if let Some(sender) = inner.peer_channels.get(&addr) {
            return Some((sender.clone(), None));
        }
        if !create {
            return None;
        }

        let is_trusted = self.addrlist.status(&addr.ip()) == Status::Trusted;
        if !is_trusted && inner.peer_channels.len() >= self.connections_limit {
            warn!(
                ?addr,
                total_connections = inner.peer_channels.len(),
                connections_limit = self.connections_limit,
                "outgoing connection limit reached, dropping paced chunk"
            );
            return None;
        }

        let (sender, receiver) = mpsc::channel(PEER_DISPATCH_CHANNEL_SIZE);
        inner.peer_channels.insert(addr, sender.clone());
        Some((
            sender,
            Some((
                receiver,
                TxStatePeerHandle {
                    tx_state: self.clone(),
                    addr,
                },
            )),
        ))
    }
}

struct TxStateInner {
    peer_channels: BTreeMap<SocketAddr, mpsc::Sender<TcpDispatch>>,
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
        trace!(?self.addr, "removed peer from TCP transmit map");
    }
}

pub(crate) async fn task(
    cfg: TcpConfig,
    addrlist: Arc<Addrlist>,
    mut tcp_egress_rx: mpsc::UnboundedReceiver<(SocketAddr, TcpDispatch)>,
    metrics: DataplaneMetrics,
) {
    let tx_state = TxState::new(addrlist, cfg.connections_limit);
    let mut conn_id = 0_u64;

    while let Some((addr, dispatch)) = tcp_egress_rx.recv().await {
        let starts_message = dispatch.header.is_some();
        let completes_message = dispatch.end.is_some();
        let Some((sender, new_connection)) = tx_state.sender(addr, starts_message) else {
            if completes_message {
                metrics.tcp_egress_messages_dropped.inc();
            }
            continue;
        };
        if let Some((dispatch_rx, peer_handle)) = new_connection {
            trace!(
                conn_id,
                ?addr,
                total_tx_connections = tx_state.inner.borrow().peer_channels.len(),
                "spawning TCP transmit connection task for peer"
            );
            spawn(task_connection(
                conn_id,
                addr,
                dispatch_rx,
                peer_handle,
                metrics.clone(),
            ));
            conn_id = conn_id.wrapping_add(1);
        }
        if let Err(err) = sender.send(dispatch).await {
            if err.0.end.is_some() {
                metrics.tcp_egress_messages_dropped.inc();
            }
            warn!(?addr, "TCP peer writer unexpectedly closed");
        }
    }
}

async fn task_connection(
    conn_id: u64,
    addr: SocketAddr,
    mut dispatch_rx: mpsc::Receiver<TcpDispatch>,
    _peer_handle: TxStatePeerHandle,
    metrics: DataplaneMetrics,
) {
    trace!(
        conn_id,
        ?addr,
        "starting TCP transmit connection task for peer"
    );
    if let Err(err) = connect_and_send(conn_id, &addr, &mut dispatch_rx, &metrics).await {
        dispatch_rx.close();
        while dispatch_rx.try_recv().is_ok() {}
        metrics.tcp_send_errors.inc();
        metrics.tcp_egress_messages_dropped.inc();
        warn!(conn_id, ?addr, ?err, "error transmitting TCP message");
        sleep(TCP_FAILURE_LINGER_WAIT).await;
    }
    trace!(
        conn_id,
        ?addr,
        "exiting TCP transmit connection task for peer"
    );
}

async fn connect_and_send(
    conn_id: u64,
    addr: &SocketAddr,
    dispatch_rx: &mut mpsc::Receiver<TcpDispatch>,
    metrics: &DataplaneMetrics,
) -> Result<(), Error> {
    let mut stream = timeout(TCP_CONNECT_TIMEOUT, TcpStream::connect(addr))
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
    trace!(conn_id, ?addr, "outbound TCP connection established");

    conn_notsent_lowat(stream.as_raw_fd(), TCP_DISPATCH_PAYLOAD_BYTES);
    let mut message_id = 0_u64;
    let mut in_message = false;
    loop {
        conn_cork(stream.as_raw_fd(), false);
        let dispatch = match timeout(MSG_WAIT_TIMEOUT, dispatch_rx.recv()).await {
            Ok(Some(dispatch)) => dispatch,
            Ok(None) => break,
            Err(_) if in_message => continue,
            Err(_) => break,
        };
        conn_cork(stream.as_raw_fd(), true);

        if let Some(header) = dispatch.header {
            in_message = true;
            trace!(
                conn_id,
                ?addr,
                message_id,
                "start transmission of TCP message"
            );
            write_all(&mut stream, header).await?;
        }
        if !dispatch.payload.is_empty() {
            write_all(&mut stream, dispatch.payload).await?;
        }
        if let Some(end) = dispatch.end {
            in_message = false;
            metrics.tcp_messages_sent.inc();
            metrics
                .tcp_bytes_sent
                .add(u64::try_from(end.message_len).unwrap_or(u64::MAX));
            if end
                .completion
                .is_some_and(|completion| completion.send(()).is_err())
            {
                warn!(conn_id, ?addr, message_id, "error sending TCP completion");
            }
            trace!(
                conn_id,
                ?addr,
                message_id,
                "completed transmission of TCP message"
            );
            message_id = message_id.wrapping_add(1);
        }
    }
    Ok(())
}

async fn write_all(stream: &mut TcpStream, bytes: bytes::Bytes) -> Result<(), Error> {
    let len = bytes.len();
    let (result, _bytes) = timeout(message_timeout(len), stream.write_all(bytes))
        .await
        .map_err(|_| Error::from(ErrorKind::TimedOut))?;
    result.map(|_| ())
}

fn conn_cork(raw_fd: RawFd, cork_flag: bool) {
    let result = unsafe {
        let cork_flag: libc::c_int = if cork_flag { 1 } else { 0 };
        libc::setsockopt(
            raw_fd,
            libc::SOL_TCP,
            libc::TCP_CORK,
            &cork_flag as *const _ as _,
            std::mem::size_of_val(&cork_flag) as _,
        )
    };
    if result != 0 {
        warn!(
            "setsockopt(TCP_CORK) failed with: {}",
            Error::last_os_error()
        );
    }
}

fn conn_notsent_lowat(raw_fd: RawFd, bytes: usize) {
    let bytes = u32::try_from(bytes).unwrap_or(u32::MAX);
    let result = unsafe {
        libc::setsockopt(
            raw_fd,
            libc::IPPROTO_TCP,
            libc::TCP_NOTSENT_LOWAT,
            &bytes as *const _ as _,
            std::mem::size_of_val(&bytes) as _,
        )
    };
    if result != 0 {
        warn!(
            "setsockopt(TCP_NOTSENT_LOWAT) failed with: {}",
            Error::last_os_error()
        );
    }
}
