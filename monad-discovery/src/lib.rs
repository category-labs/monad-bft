pub mod convert;
pub mod message;

mod nop_discovery;

use std::net::{IpAddr, SocketAddr};

use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey},
    hasher::{Hashable, Hasher},
};
use monad_types::NodeId;
pub use nop_discovery::NopDiscovery;
use serde::{Deserialize, Serialize};
use zerocopy::AsBytes;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct NetworkEndpoint {
    pub socket_addr: SocketAddr,
}

impl Hashable for NetworkEndpoint {
    fn hash(&self, state: &mut impl Hasher) {
        match self.socket_addr.ip() {
            IpAddr::V4(ip) => state.update(ip.octets()),
            IpAddr::V6(ip) => state.update(ip.octets()),
        }
        state.update(self.socket_addr.port().as_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapPeer<PT: PubKey> {
    pub node_id: NodeId<PT>,
    pub endpoint: NetworkEndpoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct MonadNameRecord<PT: PubKey> {
    pub endpoint: NetworkEndpoint,
    #[serde(bound = "PT: PubKey")]
    pub node_id: NodeId<PT>,
    pub seq_num: u64,
}

impl<PT: PubKey> Hashable for MonadNameRecord<PT> {
    fn hash(&self, state: &mut impl Hasher) {
        self.endpoint.hash(state);
        self.node_id.hash(state);
        state.update(self.seq_num.as_bytes());
    }
}

#[derive(Debug)]
pub enum DiscoveryError<ST: CertificateSignatureRecoverable> {
    InvalidPeer {
        existing_peer: MonadNameRecord<CertificateSignaturePubKey<ST>>,
        requesting_peer: MonadNameRecord<CertificateSignaturePubKey<ST>>,
    },
    DuplicatePeer(VerifiedMonadNameRecord<ST>),
    SenderRecovery,
    IncorrectSender,
}

#[derive(Clone, Debug)]
pub struct SignedMonadNameRecord<ST: CertificateSignatureRecoverable> {
    monad_name_record: MonadNameRecord<CertificateSignaturePubKey<ST>>,
    signature: ST,
}

impl<ST: CertificateSignatureRecoverable> SignedMonadNameRecord<ST> {
    pub fn new(
        monad_name_record: MonadNameRecord<CertificateSignaturePubKey<ST>>,
        key: &ST::KeyPairType,
    ) -> Self {
        let hash = monad_crypto::hasher::HasherType::hash_object(&monad_name_record);
        let signature = ST::sign(hash.as_ref(), key);
        Self {
            monad_name_record,
            signature,
        }
    }

    pub fn with_signature(
        monad_name_record: MonadNameRecord<CertificateSignaturePubKey<ST>>,
        signature: ST,
    ) -> Self {
        Self {
            monad_name_record,
            signature,
        }
    }

    pub fn record(&self) -> &MonadNameRecord<CertificateSignaturePubKey<ST>> {
        &self.monad_name_record
    }

    pub fn signature(&self) -> ST {
        self.signature
    }
}

#[derive(Clone, Debug)]
pub struct VerifiedMonadNameRecord<ST: CertificateSignatureRecoverable>(SignedMonadNameRecord<ST>);

impl<ST: CertificateSignatureRecoverable> TryFrom<SignedMonadNameRecord<ST>>
    for VerifiedMonadNameRecord<ST>
{
    type Error = DiscoveryError<ST>;

    fn try_from(value: SignedMonadNameRecord<ST>) -> Result<Self, Self::Error> {
        let hash = monad_crypto::hasher::HasherType::hash_object(&value.monad_name_record);
        let recovered_sender = value.signature.recover_pubkey(hash.as_ref()).map_err(|_| {
            // give more descriptive error
            DiscoveryError::SenderRecovery
        })?;
        if value.monad_name_record.node_id.pubkey() != recovered_sender {
            return Err(DiscoveryError::IncorrectSender);
        }
        Ok(VerifiedMonadNameRecord::new(value))
    }
}

impl<ST: CertificateSignatureRecoverable> From<VerifiedMonadNameRecord<ST>>
    for SignedMonadNameRecord<ST>
{
    fn from(value: VerifiedMonadNameRecord<ST>) -> Self {
        let (monad_name_record, signature) = value.into_parts();
        Self {
            monad_name_record,
            signature,
        }
    }
}

impl<ST: CertificateSignatureRecoverable> VerifiedMonadNameRecord<ST> {
    /// Private constructor. Use TryFrom<SignedMonadNameRecord>
    fn new(signed_monad_name_record: SignedMonadNameRecord<ST>) -> Self {
        Self(signed_monad_name_record)
    }

    fn into_parts(self) -> (MonadNameRecord<CertificateSignaturePubKey<ST>>, ST) {
        (self.0.monad_name_record, self.0.signature)
    }

    pub fn node_id(&self) -> &NodeId<CertificateSignaturePubKey<ST>> {
        &self.0.monad_name_record.node_id
    }

    pub fn seq_num(&self) -> u64 {
        self.0.monad_name_record.seq_num
    }

    pub fn record(&self) -> &MonadNameRecord<CertificateSignaturePubKey<ST>> {
        self.0.record()
    }
}

pub trait Discovery<PT>
where
    PT: PubKey,
{
    fn bootstrap_peers(&self) -> Vec<BootstrapPeer<PT>>;
}

#[cfg(test)]
mod test {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use monad_crypto::{certificate_signature::CertificateSignature, hasher::Hasher};
    use monad_secp::SecpSignature;
    use monad_types::NodeId;

    use crate::{
        DiscoveryError, MonadNameRecord, NetworkEndpoint, SignedMonadNameRecord,
        VerifiedMonadNameRecord,
    };

    type SignatureType = SecpSignature;
    type KeyPairType = <SignatureType as CertificateSignature>::KeyPairType;

    #[test]
    fn test_malicious_signature() {
        let mut s1 = [1_u8; 32];
        let mut s2 = [127_u8; 32];

        let k1 = KeyPairType::from_bytes(s1.as_mut_slice()).unwrap();
        let k2 = KeyPairType::from_bytes(s2.as_mut_slice()).unwrap();

        let node_id = NodeId::new(k1.pubkey());

        let name_record = MonadNameRecord {
            endpoint: NetworkEndpoint {
                socket_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0)),
            },
            node_id,
            seq_num: 0,
        };

        let signed_name_record =
            SignedMonadNameRecord::<SignatureType>::new(name_record.clone(), &k1);
        assert!(VerifiedMonadNameRecord::try_from(signed_name_record).is_ok());

        let hash = monad_crypto::hasher::HasherType::hash_object(&name_record);
        let signature = SecpSignature::sign(hash.as_ref(), &k2);

        let malicious_signed_record = SignedMonadNameRecord::with_signature(name_record, signature);
        assert!(matches!(
            VerifiedMonadNameRecord::try_from(malicious_signed_record)
                .err()
                .unwrap(),
            DiscoveryError::IncorrectSender
        ));
    }
}
