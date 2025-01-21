use monad_bls::BlsSignatureCollection;
use monad_consensus_types::checkpoint::Checkpoint;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::{serde::deserialize_eth_address_from_str, EthAddress};
use monad_secp::SecpSignature;
use serde::Deserialize;
use thiserror::Error;

mod bootstrap;
pub use bootstrap::{NodeBootstrapConfig, NodeBootstrapPeerConfig};

pub mod consensus;
pub use consensus::NodeConsensusConfig;

mod fullnode;
pub use fullnode::{FullNodeConfig, FullNodeIdentityConfig};

mod network;
pub use network::NodeNetworkConfig;

pub mod util;

pub(crate) type SignatureType = SecpSignature;
pub type SignatureCollectionType =
    BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

#[derive(Error, Debug)]
pub enum NodeConfigError {
    #[error(transparent)]
    ParseError(#[from] toml::de::Error),

    #[error("Node configuration error: {0}")]
    ValidationError(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    ////////////////////////////////
    // NODE-SPECIFIC CONFIGURATION //
    ////////////////////////////////
    pub name: Option<String>,
    pub network_name: Option<String>,

    #[serde(deserialize_with = "deserialize_eth_address_from_str")]
    pub beneficiary: EthAddress,

    pub ipc_tx_batch_size: u32,
    pub ipc_max_queued_batches: u8,
    // must be <= ipc_max_queued_batches
    pub ipc_queued_batches_watermark: u8,

    pub statesync_threshold: u64,
    pub statesync_max_concurrent_requests: u64,
    pub statesync_request_timeout_ms: u64,

    pub bootstrap: NodeBootstrapConfig,
    pub fullnode: FullNodeConfig,
    pub network: NodeNetworkConfig,

    // TODO split network-wide configuration into separate file
    ////////////////////////////////
    // NETWORK-WIDE CONFIGURATION //
    ////////////////////////////////
    pub chain_id: u64,
    pub consensus: NodeConsensusConfig,
}

pub type ForkpointConfig = Checkpoint<SignatureCollectionType>;

impl NodeConfig {
    pub fn parse(toml_str: &str) -> Result<Self, NodeConfigError> {
        let config: NodeConfig = toml::from_str(toml_str)?;
        config.verify()?;
        Ok(config)
    }

    fn verify(&self) -> Result<(), NodeConfigError> {
        if self.statesync_threshold <= self.consensus.execution_delay * 2 {
            return Err(NodeConfigError::ValidationError(
                "statesync_threshold must be greater than 2x the execution_delay".to_string(),
            ));
        }
        Ok(())
    }
}
