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

use alloy_primitives::Address;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{serde::deserialize_eth_address_from_str, EthExecutionProtocol};
use serde::Deserialize;

pub use self::{
    bootstrap::{NodeBootstrapConfig, NodeBootstrapPeerConfig},
    fullnode::{FullNodeConfig, FullNodeIdentityConfig},
    network::NodeNetworkConfig,
    peers::PeerDiscoveryConfig,
    sync_peers::{BlockSyncPeersConfig, StateSyncPeersConfig, SyncPeerIdentityConfig},
};

mod bootstrap;
mod fullnode;
mod network;
mod peers;

pub mod fullnode_raptorcast;
pub use fullnode_raptorcast::FullNodeRaptorCastConfig;

mod sync_peers;

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[serde(bound = "")]
pub struct NodeConfig<ST: CertificateSignatureRecoverable> {
    /////////////////////////////////
    // NODE-SPECIFIC CONFIGURATION //
    /////////////////////////////////
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

    pub bootstrap: NodeBootstrapConfig<ST>,
    pub fullnode_dedicated: FullNodeConfig<CertificateSignaturePubKey<ST>>,
    pub blocksync_override: BlockSyncPeersConfig<CertificateSignaturePubKey<ST>>,
    pub statesync: StateSyncPeersConfig<CertificateSignaturePubKey<ST>>,
    pub network: NodeNetworkConfig,

    pub peer_discovery: PeerDiscoveryConfig<ST>,

    pub raptor10_validator_redundancy_factor: u8, // validator -> validator
    pub fullnode_raptorcast: Option<FullNodeRaptorCastConfig<CertificateSignaturePubKey<ST>>>,

    // TODO split network-wide configuration into separate file
    ////////////////////////////////
    // NETWORK-WIDE CONFIGURATION //
    ////////////////////////////////
    pub chain_id: u64,

    #[serde(default = "default_soft_tx_expiry_secs")]
    pub soft_tx_expiry_secs: u64,

    #[serde(default = "default_hard_tx_expiry_secs")]
    pub hard_tx_expiry_secs: u64,
}

#[cfg(feature = "crypto")]
pub type SignatureType = monad_secp::SecpSignature;
#[cfg(feature = "crypto")]
pub type SignatureCollectionType =
    monad_bls::BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
pub type ExecutionProtocolType = EthExecutionProtocol;
#[cfg(feature = "crypto")]
pub type ForkpointConfig = monad_consensus_types::checkpoint::Checkpoint<
    SignatureType,
    SignatureCollectionType,
    ExecutionProtocolType,
>;
#[cfg(feature = "crypto")]
pub type MonadNodeConfig = NodeConfig<SignatureType>;

fn default_soft_tx_expiry_secs() -> u64 {
    15
}

fn default_hard_tx_expiry_secs() -> u64 {
    5 * 60
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tx_expiry_fields_are_now_supported_in_toml() {
        // Using the existing devnet node.toml structure and add tx_expiry fields to validate they're supported
        let existing_config_with_tx_expiry = r#"
beneficiary = "0x0000000000000000000000000000000000000000"
node_name = "dev-node"
network_name = "devnet"
ipc_tx_batch_size = 500
ipc_max_queued_batches = 6
ipc_queued_batches_watermark = 3
statesync_threshold = 600
statesync_max_concurrent_requests = 5
chain_id = 20143
raptor10_validator_redundancy_factor = 3
soft_tx_expiry_secs = 30
hard_tx_expiry_secs = 600

[bootstrap]
peers = []

[peer_discovery]
self_address = "0.0.0.0:8000"
self_record_seq_num = 0
self_name_record_sig = "85663053b3d719985357b56270ba320abadd1c51e10ea06430f3990d81393dc7770ff661c331bd631afca705029b0fd77cc0f48d46b37c8d8523c73c6af1242801"
ping_period = 30
refresh_period = 120
request_timeout = 5
unresponsive_prune_threshold = 5
last_participation_prune_threshold = 5000
min_num_peers = 0
max_num_peers = 200

[fullnode_dedicated]
identities = []

[blocksync_override]
peers = []

[statesync]
peers = []

[network]
bind_address_host = "0.0.0.0"
bind_address_port = 8000
max_rtt_ms = 300
max_mbps = 1000
"#;

        // This should now parse the config with tx_expiry fields successfully
        let result: Result<NodeConfig<SignatureType>, _> =
            toml::from_str(existing_config_with_tx_expiry);
        assert!(
            result.is_ok(),
            "Config with tx_expiry fields should parse successfully: {:?}",
            result.err()
        );

        let config = result.unwrap();
        assert_eq!(config.soft_tx_expiry_secs, 30);
        assert_eq!(config.hard_tx_expiry_secs, 600);
    }
}
