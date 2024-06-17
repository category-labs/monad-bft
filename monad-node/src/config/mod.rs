use monad_bls::{BlsPubKey, BlsSignatureCollection};
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::{serde::deserialize_eth_address_from_str, EthAddress};
use monad_secp::{PubKey, SecpSignature};
use monad_state::Forkpoint;
use monad_types::Stake;
use serde::{Deserialize, Serialize};

mod bootstrap;
pub use bootstrap::{NodeBootstrapConfig, NodeBootstrapPeerConfig};

pub mod consensus;
pub use consensus::NodeConsensusConfig;

mod network;
pub use network::NodeNetworkConfig;

pub mod util;
use util::{deserialize_bls12_381_pubkey, deserialize_secp256k1_pubkey};

pub(crate) type SignatureType = SecpSignature;
pub type SignatureCollectionType =
    BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    #[serde(deserialize_with = "deserialize_eth_address_from_str")]
    pub beneficiary: EthAddress,

    pub bootstrap: NodeBootstrapConfig,
    pub network: NodeNetworkConfig,
    pub consensus: NodeConsensusConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenesisConfig {
    pub validators: Vec<ValidatorConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkpointConfig {
    #[serde(flatten)]
    pub forkpoint: Forkpoint<SignatureCollectionType>,
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
