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

//! `Packet::Tx` frames straight to a validator's udp address, under the
//! colocated validator's id. No reply: the ledger tells what landed.

use std::{
    collections::HashMap,
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket},
};

use bytes::Bytes;
use monad_mcp_chorus::ledger::Tx;
use monad_mcp_node::{
    chorus::types::NodeId,
    network::{Packet, encode_frame},
};

// what a validator's receive loop takes as a tx from `sender`
pub fn frame(sender: NodeId, tx: &Tx) -> Bytes {
    encode_frame(sender, &Packet::Tx(tx.to_rlp()))
}

// an ephemeral local port of the family the destinations use
pub fn bind_ephemeral(ipv6: bool) -> io::Result<UdpSocket> {
    if ipv6 {
        UdpSocket::bind((Ipv6Addr::UNSPECIFIED, 0))
    } else {
        UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))
    }
}

pub struct UdpSender {
    socket: UdpSocket,
    sender_id: NodeId,
    peers: HashMap<NodeId, SocketAddr>,
}

impl UdpSender {
    // peers of one address family, as `RpcConfig::validate` checks
    pub fn bind(sender_id: NodeId, peers: HashMap<NodeId, SocketAddr>) -> io::Result<Self> {
        let ipv6 = peers.values().any(SocketAddr::is_ipv6);
        let socket = bind_ephemeral(ipv6)?;
        // a full send buffer fails the send, to be resent, rather than stalling a worker
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            sender_id,
            peers,
        })
    }

    pub fn sender_id(&self) -> NodeId {
        self.sender_id
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    // one datagram to `to`; an id outside the peers is an error
    pub fn send(&self, to: NodeId, tx: &Tx) -> io::Result<()> {
        let addr = self.peers.get(&to).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no address for validator {}", u64::from(to)),
            )
        })?;
        self.socket.send_to(&frame(self.sender_id, tx), addr)?;
        Ok(())
    }
}
