use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{
    deserialize_certificate_signature, deserialize_pubkey, serialize_certificate_signature,
    serialize_pubkey,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapConfig<ST: CertificateSignatureRecoverable> {
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub peers: Vec<NodeBootstrapPeerConfig<ST>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapPeerConfig<ST: CertificateSignatureRecoverable> {
    pub address: String,

    pub record_seq_num: u64,

    #[serde(serialize_with = "serialize_pubkey::<_, CertificateSignaturePubKey<ST>>")]
    #[serde(deserialize_with = "deserialize_pubkey::<_, CertificateSignaturePubKey<ST>>")]
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub secp256k1_pubkey: CertificateSignaturePubKey<ST>,

    #[serde(serialize_with = "serialize_certificate_signature::<_, ST>")]
    #[serde(deserialize_with = "deserialize_certificate_signature::<_, ST>")]
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub name_record_sig: ST,
}
