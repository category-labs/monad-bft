pub mod message;

mod nop_discovery;

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use bytes::Bytes;
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey},
    hasher::{Hashable, Hasher as _},
};
use monad_proto::{
    error::ProtoError,
    proto::discovery::{
        proto_ip_address::Address, ProtoIPv4, ProtoIPv6, ProtoIpAddress, ProtoMonadNameRecord,
        ProtoNetworkEndpoint, ProtoSignedMonadNameRecord,
    },
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
    fn hash(&self, state: &mut impl monad_crypto::hasher::Hasher) {
        state.update(self.socket_addr.ip().to_string().as_bytes());
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
    fn hash(&self, state: &mut impl monad_crypto::hasher::Hasher) {
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
    DuplicatePeer(SignedMonadNameRecord<ST>),
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

    pub fn verify(self) -> Result<SignedMonadNameRecord<ST>, DiscoveryError<ST>> {
        let hash = monad_crypto::hasher::HasherType::hash_object(&self.monad_name_record);
        let recovered_sender = self.signature.recover_pubkey(hash.as_ref()).map_err(|_| {
            // give more descriptive error
            DiscoveryError::SenderRecovery
        })?;
        if self.monad_name_record.node_id.pubkey() != recovered_sender {
            return Err(DiscoveryError::IncorrectSender);
        }
        Ok(self)
    }
}

const IPV4_ADDRESS_LEN: usize = 4;
const IPV6_ADDRESS_LEN: usize = 16;

impl TryFrom<ProtoNetworkEndpoint> for NetworkEndpoint {
    type Error = ProtoError;

    fn try_from(value: ProtoNetworkEndpoint) -> Result<Self, Self::Error> {
        let proto_ip = value
            .ip
            .ok_or(ProtoError::MissingRequiredField(
                "ProtoNetworkEndpoint.ip".to_owned(),
            ))?
            .address
            .ok_or(ProtoError::MissingRequiredField(
                "ProtoIpAddress.address".to_owned(),
            ))?;

        let ip = match proto_ip {
            Address::Ipv4(ProtoIPv4 { address }) => {
                if address.len() != IPV4_ADDRESS_LEN {
                    return Err(ProtoError::DeserializeError(format!(
                        "invalid IPV4 address length: {}",
                        address.len()
                    )));
                }
                let mut addr = [0u8; IPV4_ADDRESS_LEN];
                addr.copy_from_slice(&address);
                IpAddr::V4(Ipv4Addr::from(addr))
            }
            Address::Ipv6(ProtoIPv6 { address }) => {
                if address.len() != IPV6_ADDRESS_LEN {
                    return Err(ProtoError::DeserializeError(format!(
                        "invalid IPV6 address length: {}",
                        address.len()
                    )));
                }
                let mut addr = [0u8; IPV6_ADDRESS_LEN];
                addr.copy_from_slice(&address);
                IpAddr::V6(Ipv6Addr::from(addr))
            }
        };

        Ok(Self {
            socket_addr: SocketAddr::new(
                ip,
                value.port.try_into().map_err(|_| {
                    ProtoError::DeserializeError("IP port overflowed a u16".to_owned())
                })?,
            ),
        })
    }
}
impl From<&NetworkEndpoint> for ProtoNetworkEndpoint {
    fn from(value: &NetworkEndpoint) -> Self {
        match value.socket_addr {
            SocketAddr::V4(ipv4) => Self {
                ip: Some(ProtoIpAddress {
                    address: Some(Address::Ipv4(ProtoIPv4 {
                        address: Bytes::copy_from_slice(&ipv4.ip().octets()),
                    })),
                }),
                port: ipv4.port() as u32,
            },
            SocketAddr::V6(ipv6) => Self {
                ip: Some(ProtoIpAddress {
                    address: Some(Address::Ipv6(ProtoIPv6 {
                        address: Bytes::copy_from_slice(&ipv6.ip().octets()),
                    })),
                }),
                port: ipv6.port() as u32,
            },
        }
    }
}

impl<PT: PubKey> TryFrom<ProtoMonadNameRecord> for MonadNameRecord<PT> {
    type Error = ProtoError;

    fn try_from(value: ProtoMonadNameRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            endpoint: value
                .endpoint
                .ok_or(ProtoError::MissingRequiredField(
                    "ProtoMonadNameRecord.endpoint".to_owned(),
                ))?
                .try_into()?,
            node_id: value
                .node_id
                .ok_or(ProtoError::MissingRequiredField(
                    "ProtoMonadNameRecord.node_id".to_owned(),
                ))?
                .try_into()?,
            seq_num: value.seq_num,
        })
    }
}
impl<PT: PubKey> From<&MonadNameRecord<PT>> for ProtoMonadNameRecord {
    fn from(value: &MonadNameRecord<PT>) -> Self {
        let MonadNameRecord {
            endpoint,
            node_id,
            seq_num,
        } = value;
        Self {
            endpoint: Some(endpoint.into()),
            node_id: Some(node_id.into()),
            seq_num: *seq_num,
        }
    }
}

impl<ST: CertificateSignatureRecoverable> TryFrom<ProtoSignedMonadNameRecord>
    for SignedMonadNameRecord<ST>
{
    type Error = ProtoError;

    fn try_from(value: ProtoSignedMonadNameRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            monad_name_record: value
                .name_record
                .ok_or(ProtoError::MissingRequiredField(
                    "ProtoSignedMonadNameRecord.name_record".to_owned(),
                ))?
                .try_into()?,
            signature: ST::deserialize(value.signature.as_bytes()).map_err(|_| {
                // TODO(rene): use more descriptive error
                ProtoError::DeserializeError("failed to deserialize signature".to_string())
            })?,
        })
    }
}

impl<ST: CertificateSignatureRecoverable> From<&SignedMonadNameRecord<ST>>
    for ProtoSignedMonadNameRecord
{
    fn from(value: &SignedMonadNameRecord<ST>) -> Self {
        Self {
            name_record: Some(value.record().into()),
            signature: value.signature.serialize().into(),
        }
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

    use crate::{MonadNameRecord, NetworkEndpoint, SignedMonadNameRecord};

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
        assert!(signed_name_record.verify().is_ok());

        let hash = monad_crypto::hasher::HasherType::hash_object(&name_record);
        let signature = SecpSignature::sign(hash.as_ref(), &k2);

        let malicious_signed_record = SignedMonadNameRecord::with_signature(name_record, signature);
        assert!(malicious_signed_record.verify().is_err());
    }
}
