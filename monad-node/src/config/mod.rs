use monad_eth_types::{serde::deserialize_eth_address_from_str, EthAddress};
use serde::Deserialize;

mod bootstrap;
pub use bootstrap::{NodeBootstrapConfig, NodeBootstrapPeerConfig};

mod network;
pub use network::NodeNetworkConfig;

pub mod util;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    #[serde(deserialize_with = "deserialize_eth_address_from_str")]
    pub beneficiary: EthAddress,

    pub bootstrap: NodeBootstrapConfig,
    pub network: NodeNetworkConfig,
}
