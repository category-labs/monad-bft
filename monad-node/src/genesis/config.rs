use monad_crypto::secp256k1::PubKey;
use serde::Deserialize;

use crate::{
    config::util::{deserialize_secp256k1_pubkey, deserialize_secp256k1_signature},
    SignatureType,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenesisConfig {
    #[serde(deserialize_with = "deserialize_secp256k1_pubkey")]
    pub author: PubKey,

    pub signatures: Vec<GenesisSignatureConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenesisSignatureConfig {
    #[serde(deserialize_with = "deserialize_secp256k1_pubkey")]
    pub id: PubKey,

    #[serde(deserialize_with = "deserialize_secp256k1_signature")]
    pub signature: SignatureType,
}
