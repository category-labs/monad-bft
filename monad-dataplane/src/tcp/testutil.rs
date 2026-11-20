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

use std::{io::ErrorKind, net::SocketAddr, num::NonZeroU32, os::fd::AsRawFd, time::Duration};

use bytes::BytesMut;
use monoio::{
    io::{AsyncReadRentExt, AsyncWriteRentExt, Splitable},
    net::{TcpListener, TcpStream},
};
use tokio::sync::mpsc;
use zerocopy::{FromBytes, IntoBytes};

use super::{
    ConnectionContext, TcpControl, TcpMsgHdr, TcpRateLimit, TcpReadHalf, HEADER_MAGIC,
    HEADER_VERSION,
};
use crate::{egress::PacedMessage, metrics::DataplaneMetrics, RecvTcpMsg, TcpMsg, TcpSocketId};

pub(super) fn background_message(message: TcpMsg) -> super::tx::TcpEgressMsg {
    super::tx::TcpEgressMsg {
        message,
        priority: monad_types::EgressPriority::Background,
    }
}

pub(super) fn frame(payload: &[u8]) -> Vec<u8> {
    [TcpMsgHdr::new(payload.len() as u64).as_bytes(), payload].concat()
}

pub(super) async fn tcp_pair() -> (SocketAddr, TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let stream = within(TcpStream::connect(addr)).await.unwrap();
    let (peer, _) = within(listener.accept()).await.unwrap();
    (addr, stream, peer)
}

pub(super) fn set_buffer_sizes(sender: &impl AsRawFd, receiver: &impl AsRawFd, bytes: libc::c_int) {
    for (fd, option) in [
        (sender.as_raw_fd(), libc::SO_SNDBUF),
        (receiver.as_raw_fd(), libc::SO_RCVBUF),
    ] {
        assert_eq!(
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    option,
                    &bytes as *const _ as _,
                    std::mem::size_of_val(&bytes) as _,
                )
            },
            0
        );
    }
}

pub(super) fn context(
    socket_id: TcpSocketId,
    metrics: DataplaneMetrics,
    pacing: mpsc::Sender<PacedMessage>,
) -> (ConnectionContext, mpsc::Receiver<RecvTcpMsg>) {
    let (ingress, messages) = mpsc::channel(1);
    (
        ConnectionContext {
            socket_id,
            rate_limit: TcpRateLimit {
                rps: NonZeroU32::new(1).unwrap(),
                rps_burst: NonZeroU32::new(1).unwrap(),
            },
            tcp_control_map: TcpControl::new(),
            tcp_ingress_tx: ingress,
            metrics,
            pacing,
        },
        messages,
    )
}

pub(super) const PEER_TIMEOUT: Duration = Duration::from_secs(2);

pub(super) async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    monoio::time::timeout(PEER_TIMEOUT, future).await.unwrap()
}

pub(super) struct Peer {
    pub(super) addr: SocketAddr,
    pub(super) read: TcpReadHalf,
    pub(super) write: monoio::net::tcp::TcpOwnedWriteHalf,
}

impl Peer {
    pub(super) fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        let (read, write) = stream.into_split();
        Self { addr, read, write }
    }

    pub(super) async fn connect(addr: SocketAddr) -> Self {
        let stream = within(TcpStream::connect(addr)).await.unwrap();
        let local_addr = stream.local_addr().unwrap();
        Self::new(stream, local_addr)
    }

    pub(super) async fn send(&mut self, payload: &[u8]) {
        let frame = frame(payload);
        let (result, _) = within(self.write.write_all(frame)).await;
        result.unwrap();
    }

    pub(super) async fn received(&mut self, payload: &[u8]) {
        within(async {
            let (result, header) = self
                .read
                .read_exact(BytesMut::with_capacity(std::mem::size_of::<TcpMsgHdr>()))
                .await;
            result.unwrap();
            let header = TcpMsgHdr::read_from_bytes(&header).unwrap();
            assert_eq!(header.magic.get(), HEADER_MAGIC);
            assert_eq!(header.version.get(), HEADER_VERSION);
            assert_eq!(header.length.get(), payload.len() as u64);
            let (result, body) = self
                .read
                .read_exact(BytesMut::with_capacity(payload.len()))
                .await;
            result.unwrap();
            assert_eq!(&body[..], payload);
        })
        .await;
    }

    pub(super) async fn closed(&mut self) {
        let (result, _) = self.read.read_exact(BytesMut::with_capacity(1)).await;
        assert_eq!(result.unwrap_err().kind(), ErrorKind::UnexpectedEof);
    }
}
