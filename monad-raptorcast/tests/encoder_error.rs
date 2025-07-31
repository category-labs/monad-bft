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

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bytes::Bytes;
use itertools::Itertools;
use monad_crypto::hasher::{Hasher, HasherType};
use monad_dataplane::udp::DEFAULT_SEGMENT_SIZE;
use monad_raptor::SOURCE_SYMBOLS_MAX;
use monad_raptorcast::{
    udp::build_messages,
    util::{BuildTarget, EpochValidators, FullNodes, Redundancy, Validator},
};
use monad_secp::{KeyPair, SecpSignature};
use monad_types::{NodeId, Stake};
use tracing_subscriber::fmt::format::FmtSpan;

// Try to encode a message that is too large to be encoded, to verify that the encoder
// errors out instead of panic!()ing.
#[test]
pub fn encoder_error() {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let message_size = SOURCE_SYMBOLS_MAX * usize::from(DEFAULT_SEGMENT_SIZE) + 1;

    let message: Bytes = vec![123_u8; message_size].into();

    let keys = (0_u8..100_u8)
        .map(|n| {
            let mut hasher = HasherType::new();
            hasher.update(n.to_le_bytes());
            let mut hash = hasher.hash();
            KeyPair::from_bytes(&mut hash.0).unwrap()
        })
        .collect_vec();

    let mut validators = EpochValidators {
        validators: keys
            .iter()
            .map(|key| (NodeId::new(key.pubkey()), Validator { stake: Stake(1) }))
            .collect(),
    };

    let known_addresses = keys
        .iter()
        .map(|key| {
            (
                NodeId::new(key.pubkey()),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            )
        })
        .collect();

    let epoch_validators = validators.view_without(vec![&NodeId::new(keys[0].pubkey())]);
    let full_nodes = FullNodes::new(Vec::new());

    let _ = build_messages::<SecpSignature>(
        &keys[0],
        DEFAULT_SEGMENT_SIZE,
        message,
        Redundancy::from_u8(1),
        0, // epoch_no
        0, // unix_ts_ms
        BuildTarget::Raptorcast((epoch_validators, full_nodes.view())),
        &known_addresses,
    );
}
