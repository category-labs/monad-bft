use monad_secp::PubKey;
use serde::{Deserialize, Serialize};

use super::util::{deserialize_secp256k1_pubkey, serialize_secp256k1_pubkey};

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FullNodeConfig {
    pub identities: Vec<FullNodeIdentityConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FullNodeIdentityConfig {
    #[serde(deserialize_with = "deserialize_secp256k1_pubkey")]
    #[serde(serialize_with = "serialize_secp256k1_pubkey")]
    pub secp256k1_pubkey: PubKey,
}
