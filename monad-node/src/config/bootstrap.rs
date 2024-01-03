use monad_crypto::secp256k1::PubKey;
use serde::Deserialize;

use super::util::deserialize_secp256k1_pubkey;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapConfig {
    pub peers: Vec<NodeBootstrapPeerConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapPeerConfig {
    pub address: String,
    pub mempool_address: String,

    #[serde(deserialize_with = "deserialize_secp256k1_pubkey")]
    pub secp256k1_pubkey: PubKey,
}
