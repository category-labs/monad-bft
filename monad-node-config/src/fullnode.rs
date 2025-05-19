use monad_crypto::certificate_signature::PubKey;
use monad_types::{deserialize_pubkey, serialize_pubkey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FullNodeConfig<P: PubKey> {
    #[serde(bound = "P:PubKey")]
    pub identities: Vec<FullNodeIdentityConfig<P>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FullNodeIdentityConfig<P: PubKey> {
    // #[serde(deserialize_with = "deserialize_secp256k1_pubkey")]
    // #[serde(serialize_with = "serialize_secp256k1_pubkey")]
    #[serde(serialize_with = "serialize_pubkey::<_, P>")]
    #[serde(deserialize_with = "deserialize_pubkey::<_, P>")]
    #[serde(bound = "P:PubKey")]
    pub secp256k1_pubkey: P,
}
