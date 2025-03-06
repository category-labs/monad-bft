use alloy_primitives::Address;
use monad_crypto::certificate_signature::PubKey;
use monad_eth_types::serde::deserialize_eth_address_from_str;
use serde::Deserialize;

mod bootstrap;
pub use bootstrap::{NodeBootstrapConfig, NodeBootstrapPeerConfig};

mod consensus;
pub use consensus::NodeConsensusConfig;

mod fullnode;
pub use fullnode::{FullNodeConfig, FullNodeIdentityConfig};

mod network;
pub use network::NodeNetworkConfig;

mod sync_peers;
#[allow(unused_imports)]
pub use sync_peers::{BlockSyncPeersConfig, StateSyncPeersConfig, SyncPeerIdentityConfig};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig<P: PubKey> {
    ////////////////////////////////
    // NODE-SPECIFIC CONFIGURATION //
    ////////////////////////////////
    pub node_name: String,
    pub network_name: String,

    #[serde(deserialize_with = "deserialize_eth_address_from_str")]
    pub beneficiary: Address,

    pub ipc_tx_batch_size: u32,
    pub ipc_max_queued_batches: u8,
    // must be <= ipc_max_queued_batches
    pub ipc_queued_batches_watermark: u8,

    pub statesync_threshold: u16,
    pub statesync_max_concurrent_requests: u8,

    #[serde(bound = "P:PubKey")]
    pub bootstrap: NodeBootstrapConfig<P>,
    #[serde(bound = "P:PubKey")]
    pub fullnode: FullNodeConfig<P>,
    #[serde(bound = "P:PubKey")]
    pub blocksync_override: BlockSyncPeersConfig<P>,
    #[serde(bound = "P:PubKey")]
    pub statesync: StateSyncPeersConfig<P>,
    pub network: NodeNetworkConfig,

    // TODO split network-wide configuration into separate file
    ////////////////////////////////
    // NETWORK-WIDE CONFIGURATION //
    ////////////////////////////////
    pub chain_id: u64,
    pub consensus: NodeConsensusConfig,
}
