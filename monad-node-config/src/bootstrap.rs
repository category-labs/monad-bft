use monad_crypto::certificate_signature::PubKey;
use monad_types::{deserialize_pubkey, serialize_pubkey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapConfig<P: PubKey> {
    #[serde(bound = "P:PubKey")]
    pub peers: Vec<NodeBootstrapPeerConfig<P>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapPeerConfig<P: PubKey> {
    pub address: String,

    #[serde(serialize_with = "serialize_pubkey::<_, P>")]
    #[serde(deserialize_with = "deserialize_pubkey::<_, P>")]
    #[serde(bound = "P:PubKey")]
    pub secp256k1_pubkey: P,
}
