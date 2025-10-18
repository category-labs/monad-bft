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
use crate::util::{BuildTarget, Redundancy};

#[derive(Debug)]
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
    epoch_no: u64,
    unix_ts_ms: u64,
    build_target: BuildTarget<ST>,
    known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
) -> Vec<(SocketAddr, Bytes)>
where
    ST: CertificateSignatureRecoverable,
{
    let builder = MessageBuilder::new(key, known_addresses)
        .segment_size(segment_size)
        .epoch_no(epoch_no)
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

impl<T> RetrofitResult<Vec<T>> for Result<Vec<T>> {
    fn unwrap_log_on_error<ST>(
        self,
        ctx_app_msg: &[u8],
        ctx_build_target: &BuildTarget<ST>,
    ) -> Vec<T>
    where
        ST: CertificateSignatureRecoverable,
    {
        let app_message_len = ctx_app_msg.len();
        let build_target = ctx_build_target;

        match self {
            Ok(packets) => packets,

            // retrofit original error handling
            Err(BuildError::TooManyChunks) => {
                tracing::error!(
                    ?app_message_len,
                    ?build_target,
                    "Too many chunks generated."
                );
                vec![]
            }
            Err(BuildError::AppMessageTooLarge) => {
                tracing::error!(?app_message_len, "App message too large");
                vec![]
            }
            Err(BuildError::ZeroTotalStake) => {
                tracing::error!(?build_target, "Total stake is zero");
                vec![]
            }
            Err(BuildError::RedundancyTooHigh) => {
                tracing::error!(?build_target, "Redundancy too high");
                vec![]
            }
            Err(e) => {
                tracing::error!("Failed to build packets: {:?}", e);
                vec![]
            }
        }
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
