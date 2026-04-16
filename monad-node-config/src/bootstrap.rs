// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::num::NonZeroU16;

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::PeerEntry;
use monad_types::{deserialize_pubkey, serialize_pubkey};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapConfig<ST: CertificateSignatureRecoverable> {
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub peers: Vec<NodeBootstrapPeerConfig<ST>>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(deny_unknown_fields)]
#[serde(bound = "ST: CertificateSignatureRecoverable")]
pub struct NodeBootstrapPeerConfig<ST: CertificateSignatureRecoverable> {
    pub address: String,
    pub tcp_port: NonZeroU16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub udp_port: Option<NonZeroU16>,

    pub record_seq_num: u64,

    #[serde(serialize_with = "serialize_pubkey::<_, CertificateSignaturePubKey<ST>>")]
    #[serde(deserialize_with = "deserialize_pubkey::<_, CertificateSignaturePubKey<ST>>")]
    pub secp256k1_pubkey: CertificateSignaturePubKey<ST>,

    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub name_record_sig: ST,

    pub auth_port: NonZeroU16,

    #[serde(
        alias = "direct_udp_auth_port",
        skip_serializing_if = "Option::is_none"
    )]
    pub direct_udp_port: Option<NonZeroU16>,
}

#[derive(Deserialize)]
#[serde(bound = "ST: CertificateSignatureRecoverable")]
struct NodeBootstrapPeerConfigSerdeRepr<ST: CertificateSignatureRecoverable> {
    #[serde(flatten)]
    pub endpoint: NodeBootstrapPeerEndpoint,

    pub record_seq_num: u64,

    #[serde(deserialize_with = "deserialize_pubkey::<_, CertificateSignaturePubKey<ST>>")]
    pub secp256k1_pubkey: CertificateSignaturePubKey<ST>,

    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub name_record_sig: ST,

    pub auth_port: NonZeroU16,

    #[serde(alias = "direct_udp_auth_port", default)]
    pub direct_udp_port: Option<NonZeroU16>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum NodeBootstrapPeerEndpoint {
    Split(NodeBootstrapPeerSplitEndpoint),
    Legacy(NodeBootstrapPeerLegacyEndpoint),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeBootstrapPeerLegacyEndpoint {
    pub address: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeBootstrapPeerSplitEndpoint {
    pub address: String,
    pub tcp_port: NonZeroU16,
    #[serde(default)]
    pub udp_port: Option<NonZeroU16>,
}

impl<'de, ST: CertificateSignatureRecoverable> Deserialize<'de> for NodeBootstrapPeerConfig<ST> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let repr = NodeBootstrapPeerConfigSerdeRepr::deserialize(deserializer)?;
        repr.into_config().map_err(D::Error::custom)
    }
}

impl<ST: CertificateSignatureRecoverable> NodeBootstrapPeerConfigSerdeRepr<ST> {
    fn into_config(self) -> Result<NodeBootstrapPeerConfig<ST>, &'static str> {
        let address = self.endpoint.address()?;
        let tcp_port = self.endpoint.tcp_port()?;
        let udp_port = self.endpoint.udp_port()?;

        Ok(NodeBootstrapPeerConfig {
            address,
            tcp_port,
            udp_port,
            record_seq_num: self.record_seq_num,
            secp256k1_pubkey: self.secp256k1_pubkey,
            name_record_sig: self.name_record_sig,
            auth_port: self.auth_port,
            direct_udp_port: self.direct_udp_port,
        })
    }
}

impl NodeBootstrapPeerEndpoint {
    fn address(&self) -> Result<String, &'static str> {
        match self {
            Self::Split(endpoint) => Ok(normalize_split_address(endpoint.address.clone())),
            Self::Legacy(endpoint) => {
                split_host_port(&endpoint.address).map(|(address, _)| address)
            }
        }
    }

    fn tcp_port(&self) -> Result<NonZeroU16, &'static str> {
        match self {
            Self::Split(endpoint) => Ok(endpoint.tcp_port),
            Self::Legacy(endpoint) => split_host_port(&endpoint.address).map(|(_, port)| port),
        }
    }

    fn udp_port(&self) -> Result<Option<NonZeroU16>, &'static str> {
        match self {
            Self::Split(endpoint) => Ok(endpoint.udp_port),
            Self::Legacy(_) => self.tcp_port().map(Some),
        }
    }
}

fn split_host_port(address: &str) -> Result<(String, NonZeroU16), &'static str> {
    let (host, port) = address
        .rsplit_once(':')
        .ok_or("bootstrap peer address must include a port")?;
    if host.is_empty() {
        return Err("bootstrap peer address host must not be empty");
    }

    let port = port
        .parse()
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or("bootstrap peer address port must be non-zero")?;

    Ok((host.to_owned(), port))
}

fn normalize_split_address(address: String) -> String {
    split_host_port(&address)
        .map(|(host, _)| host)
        .unwrap_or(address)
}

impl<ST: CertificateSignatureRecoverable> NodeBootstrapPeerConfig<ST> {
    pub fn ip(&self) -> &str {
        &self.address
    }

    pub fn tcp_port(&self) -> NonZeroU16 {
        self.tcp_port
    }

    pub fn udp_port(&self) -> Option<NonZeroU16> {
        self.udp_port
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.address, self.tcp_port)
    }
}

impl<ST: CertificateSignatureRecoverable> From<PeerEntry<ST>> for NodeBootstrapPeerConfig<ST> {
    fn from(peer: PeerEntry<ST>) -> Self {
        Self {
            address: peer.address.to_string(),
            tcp_port: peer.tcp_port,
            udp_port: peer.udp_port,
            record_seq_num: peer.record_seq_num,
            secp256k1_pubkey: peer.pubkey,
            name_record_sig: peer.signature,
            auth_port: peer.auth_port,
            direct_udp_port: peer.direct_udp_port,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use monad_crypto::NopSignature;

    use super::NodeBootstrapPeerConfig;

    fn bootstrap_peer_toml(address_fields: &str) -> String {
        let pubkey = "01".repeat(32);
        let signature_pubkey = ["1"; 32].join(", ");

        format!(
            r#"{address_fields}
record_seq_num = 42
secp256k1_pubkey = "0x{pubkey}"
name_record_sig = {{ pubkey = [{signature_pubkey}], id = 1234 }}
auth_port = 9000
direct_udp_port = 9001
"#
        )
    }

    #[test]
    fn decodes_legacy_socket_addr_toml() {
        let peer: NodeBootstrapPeerConfig<NopSignature> =
            toml::from_str(&bootstrap_peer_toml(r#"address = "127.0.0.1:8000""#)).unwrap();

        assert_eq!(peer.ip(), "127.0.0.1");
        assert_eq!(peer.tcp_port().get(), 8000);
        assert_eq!(peer.udp_port().map(NonZeroU16::get), Some(8000));
        assert_eq!(peer.address(), "127.0.0.1:8000");
        assert_eq!(peer.direct_udp_port.map(NonZeroU16::get), Some(9001));
    }

    #[test]
    fn decodes_legacy_domain_toml() {
        let peer: NodeBootstrapPeerConfig<NopSignature> =
            toml::from_str(&bootstrap_peer_toml(r#"address = "node.example.com:8000""#)).unwrap();

        assert_eq!(peer.ip(), "node.example.com");
        assert_eq!(peer.tcp_port().get(), 8000);
        assert_eq!(peer.udp_port().map(NonZeroU16::get), Some(8000));
        assert_eq!(peer.address(), "node.example.com:8000");
    }

    #[test]
    fn decodes_split_port_toml() {
        let peer: NodeBootstrapPeerConfig<NopSignature> = toml::from_str(&bootstrap_peer_toml(
            r#"address = "127.0.0.2"
tcp_port = 8001
udp_port = 8002"#,
        ))
        .unwrap();

        assert_eq!(peer.ip(), "127.0.0.2");
        assert_eq!(peer.tcp_port().get(), 8001);
        assert_eq!(peer.udp_port().map(NonZeroU16::get), Some(8002));
        assert_eq!(peer.address(), "127.0.0.2:8001");
    }

    #[test]
    fn decodes_split_port_toml_without_udp_port() {
        let peer: NodeBootstrapPeerConfig<NopSignature> = toml::from_str(&bootstrap_peer_toml(
            r#"address = "127.0.0.3"
tcp_port = 8003"#,
        ))
        .unwrap();

        assert_eq!(peer.ip(), "127.0.0.3");
        assert_eq!(peer.tcp_port().get(), 8003);
        assert_eq!(peer.udp_port(), None);
        assert_eq!(peer.address(), "127.0.0.3:8003");
    }
}
