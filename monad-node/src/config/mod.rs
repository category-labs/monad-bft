use monad_crypto::{bls12_381::BlsPubKey, secp256k1::PubKey};
use monad_eth_types::{serde::deserialize_eth_address_from_str, EthAddress};
use monad_types::Stake;
use serde::Deserialize;

mod bootstrap;
pub use bootstrap::{NodeBootstrapConfig, NodeBootstrapPeerConfig};

mod network;
pub use network::NodeNetworkConfig;

pub mod util;
use util::{deserialize_bls12_381_pubkey, deserialize_secp256k1_pubkey};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    #[serde(deserialize_with = "deserialize_eth_address_from_str")]
    pub beneficiary: EthAddress,

    pub bootstrap: NodeBootstrapConfig,
    pub network: NodeNetworkConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenesisConfig {
    pub validators: Vec<ValidatorConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidatorConfig {
    #[serde(deserialize_with = "deserialize_secp256k1_pubkey")]
    pub secp256k1_pubkey: PubKey,

    #[serde(deserialize_with = "deserialize_bls12_381_pubkey")]
    pub bls12_381_pubkey: BlsPubKey,

    pub stake: Stake,
}
