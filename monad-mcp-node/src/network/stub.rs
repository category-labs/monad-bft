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

use std::{io, net::SocketAddr};

use bytes::{BufMut, Bytes, BytesMut};
use monad_mcp_common::containers::FastHashMap;
use tokio::{net::UdpSocket, task::JoinHandle};
use tracing::Span;

use super::{NodeInbound, NodeMessage, NodeOutbound};
use crate::{common::NodeId, component::Link};

const CADENCE: u8 = 1;
const CHUNK: u8 = 2;
const CHUNK_REQUEST: u8 = 3;
const TX: u8 = 4;

const SENDER_LEN: usize = 8;
const HEADER_LEN: usize = SENDER_LEN + 1;

// MTU 1500 less the IPv4 and UDP headers
const MAX_FRAME_LEN: usize = 1500 - 20 - 8;

pub struct UdpNetwork {
    socket: UdpSocket,
    self_id: NodeId,
    peers: FastHashMap<NodeId, SocketAddr>,
}

impl UdpNetwork {
    pub async fn bind(
        self_id: NodeId,
        port: u16,
        peers: FastHashMap<NodeId, SocketAddr>,
    ) -> io::Result<Self> {
        let socket = UdpSocket::bind(("0.0.0.0", port)).await?;
        tracing::info!(address = %socket.local_addr()?, "udp bound");
        Ok(Self {
            socket,
            self_id,
            peers,
        })
    }

    pub fn spawn(self, link: Link<NodeInbound, NodeOutbound>) -> JoinHandle<()> {
        use tracing::Instrument as _;

        tokio::spawn(self.run(link).instrument(Span::current()))
    }

    pub async fn run(self, mut link: Link<NodeInbound, NodeOutbound>) {
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

    fn receive(&self, frame: &[u8], from: SocketAddr, link: &Link<NodeInbound, NodeOutbound>) {
        let Some((sender, message)) = decode_frame(frame) else {
            tracing::debug!(%from, "malformed frame");
            return;
        };
        if !self.peers.contains_key(&sender) {
            tracing::debug!(?sender, %from, "unknown sender");
            return;
        }
        link.send(NodeInbound {
            from: sender,
            message,
        });
    }

    async fn send(&self, outbound: NodeOutbound) {
        match outbound {
            NodeOutbound::Broadcast(message) => {
                let Some(frame) = self.frame(&message) else {
                    return;
                };
                // the real udp transport, never simulated: send order is
                // left to the os and the network anyway
                #[allow(clippy::iter_over_hash_type)]
                for (peer, address) in self.peers.iter_unordered() {
                    if *peer != self.self_id {
                        self.send_to(&frame, *address).await;
                    }
                }
            }
            NodeOutbound::Unicast(to, message) => {
                let Some(address) = self.peers.get(&to) else {
                    tracing::debug!(?to, "unknown recipient");
                    return;
                };
                let Some(frame) = self.frame(&message) else {
                    return;
                };
                self.send_to(&frame, *address).await;
            }
        }
    }

    // None, logged, if the frame does not fit one datagram
    fn frame(&self, message: &NodeMessage) -> Option<Bytes> {
        let frame = encode_frame(self.self_id, message);
        if frame.len() > MAX_FRAME_LEN {
            let (kind, _) = parts(message);
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

fn parts(message: &NodeMessage) -> (u8, &Bytes) {
    match message {
        NodeMessage::Cadence(payload) => (CADENCE, payload),
        NodeMessage::Chunk(payload) => (CHUNK, payload),
        NodeMessage::ChunkRequest(payload) => (CHUNK_REQUEST, payload),
        NodeMessage::Tx(payload) => (TX, payload),
    }
}

fn from_parts(kind: u8, payload: Bytes) -> Option<NodeMessage> {
    match kind {
        CADENCE => Some(NodeMessage::Cadence(payload)),
        CHUNK => Some(NodeMessage::Chunk(payload)),
        CHUNK_REQUEST => Some(NodeMessage::ChunkRequest(payload)),
        TX => Some(NodeMessage::Tx(payload)),
        _ => None,
    }
}

pub fn encode_frame(sender: NodeId, message: &NodeMessage) -> Bytes {
    let (kind, payload) = parts(message);
    let mut frame = BytesMut::with_capacity(HEADER_LEN + payload.len());
    frame.put_u64_le(u64::from(sender));
    frame.put_u8(kind);
    frame.put_slice(payload);
    frame.freeze()
}

pub fn decode_frame(frame: &[u8]) -> Option<(NodeId, NodeMessage)> {
    let (sender, rest) = frame.split_first_chunk::<SENDER_LEN>()?;
    let (kind, payload) = rest.split_first()?;
    let sender = NodeId::dummy(u64::from_le_bytes(*sender));
    let message = from_parts(*kind, Bytes::copy_from_slice(payload))?;
    Some((sender, message))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::network::NetworkHandle;

    #[test]
    fn frames_roundtrip_and_reject_unknown_types() {
        let sender = NodeId::dummy(7);
        let payload = Bytes::from_static(b"hello");
        let messages = [
            NodeMessage::Cadence(payload.clone()),
            NodeMessage::Chunk(payload.clone()),
            NodeMessage::ChunkRequest(payload.clone()),
            NodeMessage::Tx(payload.clone()),
        ];
        for original in messages {
            let frame = encode_frame(sender, &original);
            assert_eq!(frame.len(), HEADER_LEN + payload.len());
            let (decoded_sender, decoded) = decode_frame(&frame).unwrap();
            assert_eq!(decoded_sender, sender);
            assert_eq!(parts(&decoded), parts(&original));
        }

        let mut bad_kind = encode_frame(sender, &NodeMessage::Cadence(payload)).to_vec();
        bad_kind[SENDER_LEN] = 9;
        assert!(decode_frame(&bad_kind).is_none());
        assert!(decode_frame(&bad_kind[..HEADER_LEN - 1]).is_none());
    }

    // the largest tx the node accepts fits one datagram from the rpc
    #[test]
    fn the_largest_tx_fits_the_mtu() {
        let tx = crate::component::mempool::largest_tx();
        let message = NodeMessage::Tx(tx.to_rlp());
        let frame = encode_frame(NodeId::dummy(u64::MAX), &message);
        assert!(frame.len() <= MAX_FRAME_LEN);
        let (_, decoded) = decode_frame(&frame).unwrap();
        let NodeMessage::Tx(bytes) = decoded else {
            panic!("a tx message");
        };
        assert_eq!(monad_mcp_chorus::ledger::Tx::decode_exact(&bytes), Ok(tx));
    }

    // the rpc signs frames with its colocated validator's id: only the sender id
    // is checked, against the validator set, whatever address the datagram came from
    #[tokio::test]
    async fn only_frames_from_a_validator_id_reach_the_node() {
        let peers = FastHashMap::from([
            (NodeId::dummy(0), SocketAddr::from(([127, 0, 0, 1], 1))),
            (NodeId::dummy(1), SocketAddr::from(([127, 0, 0, 1], 2))),
        ]);
        let udp = UdpNetwork::bind(NodeId::dummy(0), 0, peers).await.unwrap();
        let port = udp.socket.local_addr().unwrap().port();
        let (mut node, transport) = NetworkHandle::pair();
        let _task = udp.spawn(transport);

        let client = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let tx = |sender: u64, body: &'static [u8]| {
            let message = NodeMessage::Tx(Bytes::from_static(body));
            encode_frame(NodeId::dummy(sender), &message)
        };
        for frame in [
            tx(9, b"stranger"),
            Bytes::from_static(b"junk"),
            tx(1, b"peer"),
        ] {
            client.send_to(&frame, ("127.0.0.1", port)).unwrap();
        }
        let inbound = tokio::time::timeout(Duration::from_secs(5), node.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inbound.from, NodeId::dummy(1));
        let NodeMessage::Tx(body) = inbound.message else {
            panic!("a tx message");
        };
        assert_eq!(&body[..], b"peer");
        let next = tokio::time::timeout(Duration::from_millis(200), node.recv()).await;
        assert!(
            next.is_err(),
            "a frame outside the validator set got through"
        );
    }
}
