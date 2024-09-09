use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bytes::Bytes;
use itertools::Itertools;
use monad_crypto::hasher::{Hasher, HasherType};
use monad_dataplane::network::MONAD_GSO_SIZE;
use monad_raptor::SOURCE_SYMBOLS_MAX;
use monad_raptorcast::{
    udp::build_messages,
    util::{BuildTarget, EpochValidators, Validator},
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

    let message_size = SOURCE_SYMBOLS_MAX * MONAD_GSO_SIZE + 1;

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

    let _ = build_messages::<SecpSignature>(
        &keys[0],
        message,
        1, // redundancy,
        0, // epoch_no
        0, // unix_ts_ms
        BuildTarget::Raptorcast(epoch_validators),
        &known_addresses,
    );
}
