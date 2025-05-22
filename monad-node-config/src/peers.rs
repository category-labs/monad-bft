use std::net::SocketAddrV4;

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{
    deserialize_certificate_signature, deserialize_pubkey, serialize_certificate_signature,
    serialize_pubkey,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig<ST: CertificateSignatureRecoverable> {
    pub self_address: SocketAddrV4,
    pub self_record_seq_num: u64,

    #[serde(serialize_with = "serialize_certificate_signature::<_, ST>")]
    #[serde(deserialize_with = "deserialize_certificate_signature::<_, ST>")]
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub self_name_record_sig: ST,

    pub ping_period: u64,
    pub refresh_period: u64,
    pub request_timeout: u64,
    pub prune_threshold: u32,
    pub min_active_connections: usize,

    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub peers: Vec<PeerDiscoveryConfig<ST>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PeerDiscoveryConfig<ST: CertificateSignatureRecoverable> {
    pub address: SocketAddrV4,

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
