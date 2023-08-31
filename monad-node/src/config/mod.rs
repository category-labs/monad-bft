use serde::Deserialize;

mod bootstrap;
pub use bootstrap::{NodeBootstrapConfig, NodeBootstrapPeerConfig};

mod network;
pub use network::NodeNetworkConfig;

pub mod util;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub bootstrap: NodeBootstrapConfig,
    pub network: NodeNetworkConfig,
}
