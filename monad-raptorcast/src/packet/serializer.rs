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
    collections::{HashMap, VecDeque},
    net::SocketAddr,
};

use bytes::BytesMut;
use monad_crypto::certificate_signature::PubKey;

use super::{Chunk, Collector, PacketLayout, UdpMessage};

pub(crate) enum OperationMode {
    // `serialize_into` should be called multiple times on portions of
    // all chunks.
    Stream,

    // `serialize_into` should be called once with all chunks.
    Full,
}

pub(crate) trait ChunkSerializer<PT: PubKey> {
    // Specify the preferred operation mode for the serializer. Full
    fn operation_mode(&self) -> OperationMode;

    fn serialize_into(
        &self,
        collector: &mut impl Collector<UdpMessage>,
        chunks: Vec<Chunk<PT>>,
        layout: PacketLayout,
    );
}

// The list of available chunk serializers.

// each UDP message is a GSO batch of merged adjacent chunks
// GSO-batching is done at best effort.
pub struct GsoConcat;

// each UDP message is a GSO batch of merged chunks, slightly slower
// than GsoConcat. Each recipient gets only one GSO-batched UDP
// message.
pub struct GsoGroup;

// each UDP message contains a single chunk
pub struct Standalone;

// each recipient get their chunks in round-robin order
pub struct RoundRobin;

impl<PT: PubKey> ChunkSerializer<PT> for GsoConcat {
    fn operation_mode(&self) -> OperationMode {
        // GsoConcat does not guarantee each recipient to get only one
        // message so it can work in stream mode.
        OperationMode::Stream
    }

    fn serialize_into(
        &self,
        collector: &mut impl Collector<UdpMessage>,
        chunks: Vec<Chunk<PT>>,
        layout: PacketLayout,
    ) {
        struct AggregatedChunk {
            dest: SocketAddr,
            priority: Option<u8>,
            payload: BytesMut,
        }

        impl AggregatedChunk {
            fn from_chunk<PT: PubKey>(chunk: Chunk<PT>, dest: SocketAddr) -> Self {
                Self {
                    dest,
                    priority: chunk.priority,
                    payload: chunk.payload,
                }
            }

            fn into_udp_message(self, stride: usize) -> UdpMessage {
                UdpMessage {
                    dest: self.dest,
                    priority: self.priority,
                    payload: self.payload.freeze(),
                    stride,
                }
            }
        }

        let stride = layout.segment_len();
        let mut agg_chunk = None;

        for chunk in chunks {
            let Some(dest) = chunk.recipient.get_addr() else {
                // skip chunks with unknown recipient
                continue;
            };

            let Some(agg) = &mut agg_chunk else {
                // first chunk, start a new aggregation
                agg_chunk = Some(AggregatedChunk::from_chunk(chunk, dest));
                continue;
            };

            if agg.dest == dest && agg.priority == chunk.priority {
                // same recipient and priority, merge the payload
                // BytesMut::unsplit is O(1) when the chunk payload
                // are consecutive.
                agg.payload.unsplit(chunk.payload);
                continue;
            }

            // different recipient or priority, flush the previous message
            let next_agg = AggregatedChunk::from_chunk(chunk, dest);
            let udp_msg = std::mem::replace(agg, next_agg).into_udp_message(stride);
            collector.push(udp_msg);
        }

        if let Some(agg) = agg_chunk.take() {
            collector.push(agg.into_udp_message(stride));
        }
    }
}

impl<PT: PubKey> ChunkSerializer<PT> for GsoGroup {
    fn operation_mode(&self) -> OperationMode {
        // Requires having all chunks in order to accurately group all
        // messages by the same recipients.
        OperationMode::Full
    }

    fn serialize_into(
        &self,
        collector: &mut impl Collector<UdpMessage>,
        chunks: Vec<Chunk<PT>>,
        layout: PacketLayout,
    ) {
        #[derive(Hash, PartialEq, Eq, Clone, Copy)]
        struct GroupKey {
            dest: SocketAddr,
            priority: Option<u8>,
        }

        let mut messages: HashMap<GroupKey, BytesMut> = HashMap::new();

        for chunk in chunks {
            let Some(dest) = chunk.recipient.get_addr() else {
                continue;
            };

            let key = GroupKey {
                dest,
                priority: chunk.priority,
            };

            // BytesMut::unsplit is O(1) when the chunk payload are consecutive.
            messages.entry(key).or_default().unsplit(chunk.payload);
        }

        collector.reserve(messages.len());

        for (key, payload) in messages {
            collector.push(UdpMessage {
                dest: key.dest,
                priority: key.priority,
                payload: payload.freeze(),
                stride: layout.segment_len(),
            });
        }
    }
}

impl<PT: PubKey> ChunkSerializer<PT> for Standalone {
    fn operation_mode(&self) -> OperationMode {
        // Each chunk gets its own UDP message, so we can stream the
        // processing without waiting for all chunks.
        OperationMode::Stream
    }

    fn serialize_into(
        &self,
        collector: &mut impl Collector<UdpMessage>,
        chunks: Vec<Chunk<PT>>,
        layout: PacketLayout,
    ) {
        collector.reserve(chunks.len());

        for chunk in chunks {
            let Some(dest) = chunk.recipient.get_addr() else {
                continue;
            };
            collector.push(UdpMessage {
                dest,
                priority: chunk.priority,
                payload: chunk.payload.freeze(),
                stride: layout.segment_len(),
            });
        }
    }
}

impl<PT: PubKey> ChunkSerializer<PT> for RoundRobin {
    fn operation_mode(&self) -> OperationMode {
        // Each recipient get their chunks in round-robin order, so we
        // need to have all recipients' chunks available.
        OperationMode::Full
    }

    fn serialize_into(
        &self,
        collector: &mut impl Collector<UdpMessage>,
        chunks: Vec<Chunk<PT>>,
        layout: PacketLayout,
    ) {
        collector.reserve(chunks.len());

        let mut buckets: HashMap<SocketAddr, VecDeque<Chunk<PT>>> = HashMap::new();
        for chunk in chunks {
            let Some(dest) = chunk.recipient.get_addr() else {
                continue;
            };

            buckets.entry(dest).or_default().push_back(chunk);
        }

        // Each recipient get their own queue of chunks.
        let mut queues: Vec<VecDeque<Chunk<PT>>> = buckets.into_values().collect();

        // Optimized algorithm:
        //
        // 1. go through each recipient's queue in order
        // 2. if the queue is not empty, add the front chunk into the output
        // 3. otherwise, delete the empty queue from the Vec of queues
        // 4. repeat until there is no queue left
        while !queues.is_empty() {
            queues.retain_mut(|queue| {
                let Some(chunk) = queue.pop_front() else {
                    return false; // remove the empty queue
                };
                let dest = chunk
                    .recipient
                    .get_addr()
                    .expect("unknown addr already filtered out");
                let message = UdpMessage {
                    dest,
                    priority: chunk.priority,
                    payload: chunk.payload.freeze(),
                    stride: layout.segment_len(),
                };

                collector.push(message);
                true // keep the non-empty queue
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, net::SocketAddr, ops::Range};

    use bytes::BytesMut;
    use monad_crypto::certificate_signature::CertificateSignaturePubKey;
    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;
    use monad_types::NodeId;

    use super::{ChunkSerializer as _, RoundRobin};
    use crate::packet::{assembler::merkle_batches, Chunk, PacketLayout, Recipient};

    const DEFAULT_SEGMENT_LEN: usize = 1400;
    const DEFAULT_MERKLE_TREE_DEPTH: u8 = 6;
    const DEFAULT_LAYOUT: PacketLayout =
        PacketLayout::new(DEFAULT_SEGMENT_LEN, DEFAULT_MERKLE_TREE_DEPTH);

    type ST = SecpSignature;
    type PT = CertificateSignaturePubKey<ST>;

    fn node_id(seed: u64) -> NodeId<PT> {
        let key_pair = get_key::<ST>(seed);
        NodeId::new(key_pair.pubkey())
    }

    #[test]
    fn test_round_robin() {
        let layout = DEFAULT_LAYOUT;
        let mut chunks = gen_chunks(
            &[
                (1, 0..4),    // 4 chunks for node 1
                (2, 4..5),    // 1 chunk for node 2
                (3, 5..6),    // 1 chunk for node 3
                (4, 10..100), // 90 chunks for node 4 (discontinuity intended)
            ],
            layout,
        );
        fill_chunk_headers(&mut chunks, layout);

        let num_chunks = chunks.len();
        assert_eq!(num_chunks, 96);

        let mut udp_messages = vec![];
        RoundRobin.serialize_into(&mut udp_messages, chunks, layout);

        assert_eq!(udp_messages.len(), num_chunks);

        // each round is a set of (node_idx, chunk_id range)
        let expected_rounds: &[&[(u8, Range<usize>)]] = &[
            // first round
            &[(1, 0..1), (2, 4..5), (3, 5..6), (4, 10..11)],
            // second round
            &[(1, 1..2), (4, 11..12)],
            // third round
            &[(1, 2..3), (4, 12..13)],
            // fourth round
            &[(1, 3..4), (4, 13..14)],
            // remaining rounds
            &[(4, 14..100)],
        ];

        for round in expected_rounds {
            let mut num_msgs = 0;
            let mut expected_msgs = HashSet::new();

            for (node, chunk_range) in round.iter() {
                for chunk_id in chunk_range.clone() {
                    expected_msgs.insert((*node, chunk_id));
                }
                num_msgs += chunk_range.len();
            }

            for msg in udp_messages.drain(0..num_msgs) {
                assert_eq!(msg.payload.len(), layout.segment_len());
                let node = addr_to_node(msg.dest);
                let chunk_id = get_chunk_id(&msg.payload, layout);

                assert!(expected_msgs.contains(&(node, chunk_id as usize)));
                expected_msgs.remove(&(node, chunk_id as usize));
            }

            assert!(expected_msgs.is_empty());
        }
    }

    // Generate chunks with a given specification. Each entry in
    // `spec` is a pair of node index and the range of chunk_ids to
    // generate for that node.
    //
    // Note: chunk generated will already have the recipient's socket address
    // assigned.
    fn gen_chunks(spec: &[(u8, Range<usize>)], layout: PacketLayout) -> Vec<Chunk<PT>> {
        let mut chunks = Vec::new();

        for (recipient_idx, num_chunks) in spec {
            let recipient = Recipient::new(node_id(*recipient_idx as u64));
            recipient.set_addr(gen_addr(*recipient_idx));
            for chunk_id in num_chunks.clone() {
                let payload = BytesMut::zeroed(layout.segment_len());
                chunks.push(Chunk::new(chunk_id, recipient.clone(), payload));
            }
        }

        chunks
    }

    fn fill_chunk_headers(chunks: &mut [Chunk<PT>], layout: PacketLayout) {
        for mut merkle_batch in merkle_batches(chunks, layout) {
            merkle_batch.write_chunk_headers(layout).unwrap();
        }
    }

    fn gen_addr(node: u8) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, node], 1000 + node as u16))
    }

    fn addr_to_node(addr: SocketAddr) -> u8 {
        let SocketAddr::V4(addr) = addr else {
            panic!("invalid adr");
        };

        let [127, 0, 0, octet] = addr.ip().octets() else {
            panic!("invalid addr");
        };

        let port = addr.port();
        assert_eq!(octet as u16 + 1000, port);

        octet
    }

    fn get_chunk_id(segment: &[u8], layout: PacketLayout) -> u16 {
        let header = &segment[layout.chunk_header_range()];
        u16::from_le_bytes(header[22..24].try_into().expect("invalid chunk header"))
    }
}
