use super::util::deserialize_secp256k1_pubkey;
use monad_secp::PubKey;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullNodeConfig {
    pub identities: Vec<FullNodeIdentityConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullNodeIdentityConfig {
    #[serde(deserialize_with = "deserialize_secp256k1_pubkey")]
    pub secp256k1_pubkey: PubKey,
}
