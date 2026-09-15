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

//! UDP between configured peers. One datagram carries one frame:
//! sender node id (8 bytes, little endian), packet type (1 byte),
//! payload. A frame that does not fit the MTU is dropped at the
//! sender, a frame from a sender outside the validator set at the
//! receiver.

use std::{collections::HashMap, io, net::SocketAddr};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::{net::UdpSocket, task::JoinHandle};
use tracing::{Instrument as _, Span};

use super::{Inbound, Link, Outbound, Packet};
use crate::chorus::types::NodeId;

const CADENCE: u8 = 1;
const CHUNK: u8 = 2;
const CHUNK_REQUEST: u8 = 3;

const SENDER_LEN: usize = 8;
const HEADER_LEN: usize = SENDER_LEN + 1;

// MTU 1500 less the IPv4 and UDP headers
const MAX_FRAME_LEN: usize = 1500 - 20 - 8;

pub struct UdpNetwork {
    socket: UdpSocket,
    self_id: NodeId,
    peers: HashMap<NodeId, SocketAddr>,
}

impl UdpNetwork {
    pub async fn bind(
        self_id: NodeId,
        port: u16,
        peers: HashMap<NodeId, SocketAddr>,
    ) -> io::Result<Self> {
        let socket = UdpSocket::bind(("0.0.0.0", port)).await?;
        tracing::info!(address = %socket.local_addr()?, "udp bound");
        Ok(Self {
            socket,
            self_id,
            peers,
        })
    }

    pub fn spawn(self, link: Link<Inbound, Outbound>) -> JoinHandle<()> {
        tokio::spawn(self.run(link).instrument(Span::current()))
    }

    async fn run(self, mut link: Link<Inbound, Outbound>) {
        let mut buffer = vec![0u8; MAX_FRAME_LEN];
        loop {
            tokio::select! {
                received = self.socket.recv_from(&mut buffer) => match received {
                    Ok((len, from)) => self.receive(&buffer[..len], from, &link),
                    Err(error) => tracing::warn!(%error, "udp receive failed"),
                },
                outbound = link.recv() => {
                    let Some(outbound) = outbound else {
                        return;
                    };
                    self.send(outbound).await;
                }
            }
        }
    }

    fn receive(&self, frame: &[u8], from: SocketAddr, link: &Link<Inbound, Outbound>) {
        let Some((sender, packet)) = decode_frame(frame) else {
            tracing::debug!(%from, "malformed frame");
            return;
        };
        if !self.peers.contains_key(&sender) {
            tracing::debug!(?sender, %from, "unknown sender");
            return;
        }
        link.send(Inbound {
            from: sender,
            packet,
        });
    }

    async fn send(&self, outbound: Outbound) {
        match outbound {
            Outbound::Broadcast(packet) => {
                let Some(frame) = self.frame(&packet) else {
                    return;
                };
                for (peer, address) in &self.peers {
                    if *peer != self.self_id {
                        self.send_to(&frame, *address).await;
                    }
                }
            }
            Outbound::Unicast { to, packet } => {
                let Some(address) = self.peers.get(&to) else {
                    tracing::debug!(?to, "unknown recipient");
                    return;
                };
                let Some(frame) = self.frame(&packet) else {
                    return;
                };
                self.send_to(&frame, *address).await;
            }
        }
    }

    // None, logged, if the frame does not fit one datagram
    fn frame(&self, packet: &Packet) -> Option<Bytes> {
        let frame = encode_frame(self.self_id, packet);
        if frame.len() > MAX_FRAME_LEN {
            let (kind, _) = parts(packet);
            tracing::warn!(kind, len = frame.len(), "frame exceeds the mtu, dropped");
            return None;
        }
        Some(frame)
    }

    async fn send_to(&self, frame: &[u8], address: SocketAddr) {
        if let Err(error) = self.socket.send_to(frame, address).await {
            tracing::warn!(%error, %address, "udp send failed");
        }
    }
}

fn parts(packet: &Packet) -> (u8, &Bytes) {
    match packet {
        Packet::Cadence(payload) => (CADENCE, payload),
        Packet::Chunk(payload) => (CHUNK, payload),
        Packet::ChunkRequest(payload) => (CHUNK_REQUEST, payload),
    }
}

fn packet(kind: u8, payload: Bytes) -> Option<Packet> {
    match kind {
        CADENCE => Some(Packet::Cadence(payload)),
        CHUNK => Some(Packet::Chunk(payload)),
        CHUNK_REQUEST => Some(Packet::ChunkRequest(payload)),
        _ => None,
    }
}

fn encode_frame(sender: NodeId, packet: &Packet) -> Bytes {
    let (kind, payload) = parts(packet);
    let mut frame = BytesMut::with_capacity(HEADER_LEN + payload.len());
    frame.put_u64_le(u64::from(sender));
    frame.put_u8(kind);
    frame.put_slice(payload);
    frame.freeze()
}

fn decode_frame(frame: &[u8]) -> Option<(NodeId, Packet)> {
    let (sender, rest) = frame.split_first_chunk::<SENDER_LEN>()?;
    let (kind, payload) = rest.split_first()?;
    let sender = NodeId::dummy(u64::from_le_bytes(*sender));
    let packet = packet(*kind, Bytes::copy_from_slice(payload))?;
    Some((sender, packet))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_roundtrip_and_reject_unknown_types() {
        let sender = NodeId::dummy(7);
        let payload = Bytes::from_static(b"hello");
        let packets = [
            Packet::Cadence(payload.clone()),
            Packet::Chunk(payload.clone()),
            Packet::ChunkRequest(payload.clone()),
        ];
        for original in packets {
            let frame = encode_frame(sender, &original);
            assert_eq!(frame.len(), HEADER_LEN + payload.len());
            let (decoded_sender, decoded) = decode_frame(&frame).unwrap();
            assert_eq!(decoded_sender, sender);
            assert_eq!(parts(&decoded), parts(&original));
        }

        let mut bad_kind = encode_frame(sender, &Packet::Cadence(payload)).to_vec();
        bad_kind[SENDER_LEN] = 9;
        assert!(decode_frame(&bad_kind).is_none());
        assert!(decode_frame(&bad_kind[..HEADER_LEN - 1]).is_none());
    }
}
