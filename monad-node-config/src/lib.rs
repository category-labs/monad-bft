use alloy_primitives::Address;
#[cfg(feature = "crypto")]
use monad_bls::BlsSignatureCollection;
#[cfg(feature = "crypto")]
use monad_consensus_types::checkpoint::Checkpoint;
use monad_crypto::certificate_signature::{CertificateSignaturePubKey, PubKey};
use monad_eth_types::{serde::deserialize_eth_address_from_str, EthExecutionProtocol};
use monad_secp::SecpSignature;
use serde::Deserialize;

pub use self::{
    bootstrap::{NodeBootstrapConfig, NodeBootstrapPeerConfig},
    consensus::NodeConsensusConfig,
    fullnode::{FullNodeConfig, FullNodeIdentityConfig},
    network::NodeNetworkConfig,
    sync_peers::{BlockSyncPeersConfig, StateSyncPeersConfig, SyncPeerIdentityConfig},
};

mod bootstrap;
mod consensus;
mod fullnode;
mod network;
mod sync_peers;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig<P: PubKey> {
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

pub type SignatureType = SecpSignature;
#[cfg(feature = "crypto")]
pub type SignatureCollectionType =
    BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
pub type ExecutionProtocolType = EthExecutionProtocol;
#[cfg(feature = "crypto")]
pub type ForkpointConfig = Checkpoint<SignatureCollectionType>;
pub type MonadNodeConfig = NodeConfig<CertificateSignaturePubKey<SignatureType>>;
