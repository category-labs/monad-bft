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
    assembler::{Chunk, PacketLayout},
    assigner::ChunkAssigner,
    builder::MessageBuilder,
};
use crate::{
    udp::GroupId,
    util::{BuildTarget, Redundancy},
};

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

type Result<A, E = BuildError> = std::result::Result<A, E>;

#[allow(clippy::too_many_arguments)]
pub fn build_messages<ST>(
    key: &ST::KeyPairType,
    segment_size: u16,
    app_message: Bytes,
    redundancy: Redundancy,
    group_id: GroupId,
    unix_ts_ms: u64,
    build_target: BuildTarget<CertificateSignaturePubKey<ST>>,
    known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
) -> Vec<(SocketAddr, Bytes)>
where
    ST: CertificateSignatureRecoverable,
{
    let builder = MessageBuilder::<ST>::new(key)
        .segment_size(segment_size)
        .group_id(group_id)
        .unix_ts_ms(unix_ts_ms)
        .redundancy(redundancy);

    let packets = builder
        .prepare()
        .build_vec(&app_message, &build_target)
        .unwrap_log_on_error(&app_message, &build_target);

    packets
        .into_iter()
        .filter_map(|msg| {
            msg.recipient
                .lookup(known_addresses)
                .map(|dest| (dest, msg.payload))
        })
        .collect()
}

// retrofit original error handling
pub trait RetrofitResult<T> {
    fn unwrap_log_on_error<PT>(self, ctx_app_msg: &[u8], ctx_build_target: &BuildTarget<PT>) -> T
    where
        PT: PubKey;
}

impl<T> RetrofitResult<T> for Result<T>
where
    T: Default,
{
    fn unwrap_log_on_error<PT>(self, ctx_app_msg: &[u8], ctx_build_target: &BuildTarget<PT>) -> T
    where
        PT: PubKey,
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
