use std::net::Ipv4Addr;

use monad_crypto::{bls12_381::BlsPubKey, secp256k1::PubKey};
use monad_types::Stake;
use serde::Deserialize;

use super::util::{deserialize_bls12_381_pubkey, deserialize_secp256k1_pubkey};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapConfig {
    pub peers: Vec<NodeBootstrapPeerConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapPeerConfig {
    pub ip: Ipv4Addr,

    pub port: u16,

    #[serde(deserialize_with = "deserialize_secp256k1_pubkey")]
    pub secp256k1_pubkey: PubKey,

    #[serde(deserialize_with = "deserialize_bls12_381_pubkey")]
    pub bls12_381_pubkey: BlsPubKey,

    pub stake: Stake,
}
