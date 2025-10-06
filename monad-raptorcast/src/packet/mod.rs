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

pub(crate) mod assembler;
pub(crate) mod assigner;
mod builder;

use std::{collections::HashMap, net::SocketAddr};

use bytes::Bytes;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_types::NodeId;

pub(crate) use self::{
    assembler::{Chunk, PacketLayout, Recipient},
    assigner::ChunkAssigner,
    builder::MessageBuilder,
};
use crate::{
    udp::GroupId,
    util::{BuildTarget, Redundancy},
};

#[derive(Debug, Clone)]
pub struct UdpMessage {
    pub dest: SocketAddr,
    pub payload: Bytes,
    pub stride: usize,
}

#[derive(Debug)]
pub enum BuildError {
    // merkle tree depth is 0
    MerkleTreeTooShallow,
    // merkle tree depth is larger than the allowed maximum
    MerkleTreeTooDeep,
    // chunk id does not fit in u16
    ChunkIdOverflow,
    // failed to create encoder
    EncoderCreationFailed,
    // chunk length smaller than the allowed minimum
    ChunkLengthTooSmall,
    // too many chunks
    TooManyChunks,
    // app message is too large
    AppMessageTooLarge,
    // total stake is zero
    ZeroTotalStake,
    // redundancy is too high
    RedundancyTooHigh,
}

pub(crate) trait PeerAddrLookup<PT: PubKey> {
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr>;
}

// Similar to std::iter::Extend trait but implemented for FnMut as
// well.
pub(crate) trait Collector<T> {
    fn push(&mut self, item: T);
    fn reserve(&mut self, _additional: usize) {}
}

type Result<A, E = BuildError> = std::result::Result<A, E>;

#[allow(clippy::too_many_arguments)]
pub fn build_messages<ST>(
    key: &ST::KeyPairType,
    segment_size: u16,
    app_message: Bytes,
    redundancy: Redundancy,
    group_id: GroupId,
    unix_ts_ms: u64,
    build_target: BuildTarget<ST>,
    known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
) -> Vec<(SocketAddr, Bytes)>
where
    ST: CertificateSignatureRecoverable,
{
    let builder = MessageBuilder::new(key, known_addresses)
        .segment_size(segment_size)
        .group_id(group_id)
        .unix_ts_ms(unix_ts_ms)
        .redundancy(redundancy);

    let packets = builder
        .build_vec(&app_message, &build_target)
        .unwrap_log_on_error(&app_message, &build_target);

    packets
        .into_iter()
        .map(|msg| (msg.dest, msg.payload))
        .collect()
}

// retrofit original error handling
pub trait RetrofitResult<T> {
    fn unwrap_log_on_error<ST>(self, ctx_app_msg: &[u8], ctx_build_target: &BuildTarget<ST>) -> T
    where
        ST: CertificateSignatureRecoverable;
}

impl<T> RetrofitResult<T> for Result<T>
where
    T: Default,
{
    fn unwrap_log_on_error<ST>(self, ctx_app_msg: &[u8], ctx_build_target: &BuildTarget<ST>) -> T
    where
        ST: CertificateSignatureRecoverable,
    {
        let app_message_len = ctx_app_msg.len();
        let build_target = ctx_build_target;

        match self {
            Ok(packets) => return packets,

            // retrofit original error handling
            Err(BuildError::TooManyChunks) => {
                tracing::error!(
                    ?app_message_len,
                    ?build_target,
                    "Too many chunks generated."
                );
            }
            Err(BuildError::AppMessageTooLarge) => {
                tracing::error!(?app_message_len, "App message too large");
            }
            Err(BuildError::ZeroTotalStake) => {
                tracing::error!(?build_target, "Total stake is zero");
            }
            Err(BuildError::RedundancyTooHigh) => {
                tracing::error!(?build_target, "Redundancy too high");
            }
            Err(e) => {
                tracing::error!("Failed to build packets: {:?}", e);
            }
        }

        Default::default()
    }
}

impl<PT: PubKey> PeerAddrLookup<PT> for HashMap<NodeId<PT>, SocketAddr> {
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr> {
        self.get(node_id).copied()
    }
}

/// Used in RaptorCast instance to lookup peer addresses with the peer discovery driver.
impl<ST: CertificateSignatureRecoverable, PD> PeerAddrLookup<CertificateSignaturePubKey<ST>>
    for std::sync::Mutex<monad_peer_discovery::driver::PeerDiscoveryDriver<PD>>
where
    PD: monad_peer_discovery::PeerDiscoveryAlgo<SignatureType = ST>,
{
    fn lookup(&self, node_id: &NodeId<CertificateSignaturePubKey<ST>>) -> Option<SocketAddr> {
        let guard = self.lock().ok()?;
        guard.get_addr(node_id)
    }
}

impl<PT, T> PeerAddrLookup<PT> for &T
where
    PT: PubKey,
    T: PeerAddrLookup<PT>,
{
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr> {
        (*self).lookup(node_id)
    }
}
impl<PT, T> PeerAddrLookup<PT> for std::sync::Arc<T>
where
    PT: PubKey,
    T: PeerAddrLookup<PT>,
{
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr> {
        self.as_ref().lookup(node_id)
    }
}

impl<T> Collector<T> for Vec<T> {
    fn push(&mut self, item: T) {
        Vec::push(self, item)
    }

    fn reserve(&mut self, additional: usize) {
        Vec::reserve(self, additional)
    }
}

impl<F, T> Collector<T> for F
where
    F: FnMut(T),
{
    fn push(&mut self, item: T) {
        self(item)
    }
}

// Batch assembled UdpMessages into UnicastMsgs for consumption in
// dataplane, flush on buffer full and on drop.
pub struct UdpMessageBatcher<F>
where
    F: FnMut(monad_dataplane::UnicastMsg),
{
    buffer_size: usize,
    buffer: monad_dataplane::UnicastMsg,
    sink: F,
}

impl<F> UdpMessageBatcher<F>
where
    F: FnMut(monad_dataplane::UnicastMsg),
{
    pub fn new(buffer_size: usize, sink: F) -> Self {
        Self {
            buffer_size,
            buffer: monad_dataplane::UnicastMsg {
                msgs: Vec::with_capacity(buffer_size),
                stride: 0,
            },
            sink,
        }
    }

    fn flush(&mut self) {
        if self.buffer.msgs.is_empty() {
            return;
        }
        debug_assert!(self.buffer.stride != 0);

        let fresh_buffer = monad_dataplane::UnicastMsg {
            msgs: Vec::with_capacity(self.buffer_size),
            stride: 0,
        };

        let unicast_msg = std::mem::replace(&mut self.buffer, fresh_buffer);
        (self.sink)(unicast_msg);
    }
}

impl<F> Drop for UdpMessageBatcher<F>
where
    F: FnMut(monad_dataplane::UnicastMsg),
{
    fn drop(&mut self) {
        self.flush();
    }
}

impl<F> Collector<UdpMessage> for UdpMessageBatcher<F>
where
    F: FnMut(monad_dataplane::UnicastMsg),
{
    fn push(&mut self, item: UdpMessage) {
        let stride = item.stride as u16;

        // uninitialized, set the stride
        if self.buffer.stride == 0 {
            self.buffer.stride = stride;
        }

        // stride changes, flush the buffer and update the stride
        if self.buffer.stride != stride {
            tracing::debug!(
                "UdpMessageBatcher: stride changed from {} to {}",
                self.buffer.stride,
                stride
            );

            self.flush();
            self.buffer.stride = stride;
        }

        self.buffer.msgs.push((item.dest, item.payload));

        if self.buffer.msgs.len() >= self.buffer_size {
            self.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    #[test]
    fn test_udp_message_batcher() {
        let collected_batches: RefCell<Vec<monad_dataplane::UnicastMsg>> = Default::default();
        let dest = "127.0.0.1:3000".parse().unwrap();
        let msg_batch_1 = vec![
            // 4 messages, each with stride 4
            UdpMessage { dest, payload: Bytes::from(vec![42; 4]), stride: 4 }; 4
        ];

        let msg_batch_2 = vec![
            // 4 messages, each with stride 5
            UdpMessage { dest, payload: Bytes::from(vec![43; 5]), stride: 5 }; 4
        ];

        let mut batcher = UdpMessageBatcher::new(3, |batch| {
            collected_batches.borrow_mut().push(batch);
        });
        for msg in msg_batch_1 {
            batcher.push(msg);
        }
        for msg in msg_batch_2 {
            batcher.push(msg);
        }
        assert_eq!(collected_batches.borrow().len(), 3);
        // first UnicastMsg: 3 messages with stride 4
        assert_eq!(collected_batches.borrow()[0].msgs.len(), 3);
        assert_eq!(collected_batches.borrow()[0].stride, 4);
        // second UnicastMsg: 1 message with stride 4
        assert_eq!(collected_batches.borrow()[1].msgs.len(), 1);
        assert_eq!(collected_batches.borrow()[1].stride, 4);
        // third UnicastMsg: 3 messages with stride 5
        assert_eq!(collected_batches.borrow()[2].msgs.len(), 3);
        assert_eq!(collected_batches.borrow()[2].stride, 5);

        drop(batcher);

        // on drop, the last message with stride 5 should be flushed
        assert_eq!(collected_batches.borrow().len(), 4);
        assert_eq!(collected_batches.borrow()[3].msgs.len(), 1);
        assert_eq!(collected_batches.borrow()[3].stride, 5);
    }
}
