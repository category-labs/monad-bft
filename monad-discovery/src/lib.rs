pub mod message;

mod nop_discovery;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use bytes::Bytes;
use monad_crypto::certificate_signature::PubKey;
use monad_proto::{
    error::ProtoError,
    proto::discovery::{
        proto_ip_address::Address, ProtoIPv4, ProtoIPv6, ProtoIpAddress, ProtoMonadNameRecord,
        ProtoNetworkEndpoint,
    },
};
use monad_types::NodeId;
pub use nop_discovery::NopDiscovery;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct NetworkEndpoint {
    pub socket_addr: SocketAddr,
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

pub trait Discovery<PT>
where
    PT: PubKey,
{
    fn bootstrap_peers(&self) -> Vec<BootstrapPeer<PT>>;
}
