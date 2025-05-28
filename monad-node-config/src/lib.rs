use alloy_primitives::Address;
#[cfg(feature = "crypto")]
use monad_bls::BlsSignatureCollection;
#[cfg(feature = "crypto")]
use monad_consensus_types::checkpoint::Checkpoint;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{serde::deserialize_eth_address_from_str, EthExecutionProtocol};
use monad_secp::SecpSignature;
use serde::Deserialize;

pub use self::{
    bootstrap::{NodeBootstrapConfig, NodeBootstrapPeerConfig},
    consensus::NodeConsensusConfig,
    fullnode::{FullNodeConfig, FullNodeIdentityConfig},
    network::NodeNetworkConfig,
    peers::PeerDiscoveryConfig,
    sync_peers::{BlockSyncPeersConfig, StateSyncPeersConfig, SyncPeerIdentityConfig},
};

mod bootstrap;
mod consensus;
mod fullnode;
mod network;
mod peers;

pub mod fullnode_raptorcast;
pub use fullnode_raptorcast::FullNodeRaptorCastConfig;

mod sync_peers;

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
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

    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub bootstrap: NodeBootstrapConfig<ST>,
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub fullnode_dedicated: FullNodeConfig<CertificateSignaturePubKey<ST>>,
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub blocksync_override: BlockSyncPeersConfig<CertificateSignaturePubKey<ST>>,
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub statesync: StateSyncPeersConfig<CertificateSignaturePubKey<ST>>,
    pub network: NodeNetworkConfig,

    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub peer_discovery: PeerDiscoveryConfig<ST>,

    pub raptor10_validator_redundancy_factor: u8, // validator -> validator
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub fullnode_raptorcast: Option<FullNodeRaptorCastConfig<CertificateSignaturePubKey<ST>>>,

    // TODO split network-wide configuration into separate file
    ////////////////////////////////
    // NETWORK-WIDE CONFIGURATION //
    ////////////////////////////////
    pub chain_id: u64,
    pub consensus: NodeConsensusConfig,
}

pub type SignatureType = SecpSignature;
#[cfg(feature = "crypto")]
pub type SignatureCollectionType =
    BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
pub type ExecutionProtocolType = EthExecutionProtocol;
#[cfg(feature = "crypto")]
pub type ForkpointConfig = Checkpoint<SignatureCollectionType>;
pub type MonadNodeConfig = NodeConfig<SignatureType>;
