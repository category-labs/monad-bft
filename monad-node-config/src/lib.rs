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

use std::collections::BTreeMap;

use alloy_primitives::Address;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{serde::deserialize_eth_address_from_str, EthExecutionProtocol};
use monad_peer_score::ema;
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

pub mod raptorcast;
pub use raptorcast::DeterministicProtocolRolloutStage;

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

    #[serde(default)]
    pub metrics: MetricsConfig,

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

    #[serde(deserialize_with = "peers::deserialize_peer_discovery_config")]
    pub peer_discovery: PeerDiscoveryConfig<ST>,
    #[serde(default)]
    pub txpool_peer_score: ema::ScoreConfig,

    pub fullnode_raptorcast: FullNodeRaptorCastConfig<CertificateSignaturePubKey<ST>>,

    #[serde(default)]
    pub deterministic_raptorcast_rollout: DeterministicProtocolRolloutStage,

    // TODO split network-wide configuration into separate file
    ////////////////////////////////
    // NETWORK-WIDE CONFIGURATION //
    ////////////////////////////////
    pub chain_id: u64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub listen_addr: String,
    pub labels: BTreeMap<String, String>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            // listening on 0.0.0.0 is potentially unsafe because we do not monitor precise
            // resource usage of the prometheus service; operators must secure it appropriately.
            listen_addr: "0.0.0.0:9143".to_owned(),
            labels: BTreeMap::new(),
        }
    }
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
pub type ValidatorsConfigType =
    monad_consensus_types::validator_data::ValidatorsConfig<SignatureCollectionType>;
#[cfg(feature = "crypto")]
pub type MonadNodeConfig = NodeConfig<SignatureType>;

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::MetricsConfig;

    #[derive(Deserialize)]
    struct TestNodeConfig {
        #[serde(default)]
        metrics: MetricsConfig,
    }

    #[test]
    fn metrics_config_defaults_and_override() {
        let config: MetricsConfig = toml::from_str("").unwrap();

        assert!(config.enabled);
        assert_eq!(config.listen_addr, "0.0.0.0:9143");
        assert!(config.labels.is_empty());

        let disabled: MetricsConfig = toml::from_str("enabled = false").unwrap();
        assert!(!disabled.enabled);
    }

    #[test]
    fn metrics_config_accepts_labels_only() {
        let config: TestNodeConfig = toml::from_str(
            r#"
[metrics.labels]
environment = "testnet"
"#,
        )
        .unwrap();

        assert!(config.metrics.enabled);
        assert_eq!(config.metrics.listen_addr, "0.0.0.0:9143");
        assert_eq!(config.metrics.labels["environment"], "testnet");
    }
}
