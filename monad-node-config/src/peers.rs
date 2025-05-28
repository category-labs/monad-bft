use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_types::{deserialize_certificate_signature, serialize_certificate_signature};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct PeerDiscoveryConfig<ST: CertificateSignatureRecoverable> {
    pub self_address: String,
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
    pub max_active_connections: usize,
}
