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

use std::{net::Ipv4Addr, num::NonZeroU16};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{deserialize_pubkey, serialize_pubkey};
use serde::{de::Error as _, Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapConfig<ST: CertificateSignatureRecoverable> {
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    #[serde(deserialize_with = "deserialize_bootstrap_peers")]
    pub peers: Vec<NodeBootstrapPeerConfig<ST>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
#[serde(bound = "ST: CertificateSignatureRecoverable")]
pub struct NodeBootstrapPeerConfig<ST: CertificateSignatureRecoverable> {
    // Supported bootstrap address formats:
    // - Legacy: address = "host:port"
    // - Split: address = "host", tcp_port = 8000, udp_port = 8001 (optional)
    // Host is an IPv4 literal or DNS name; IPv6 is not supported here.
    // Split-format address must not include a port.
    pub address: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_port: Option<NonZeroU16>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
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

fn deserialize_bootstrap_peers<'de, D, ST>(
    deserializer: D,
) -> Result<Vec<NodeBootstrapPeerConfig<ST>>, D::Error>
where
    D: serde::Deserializer<'de>,
    ST: CertificateSignatureRecoverable,
{
    Vec::<NodeBootstrapPeerConfig<ST>>::deserialize(deserializer)?
        .into_iter()
        .map(|peer| peer.validate_address_fields().map_err(D::Error::custom))
        .collect()
}

pub(crate) fn parse_address_fields(
    address: &str,
    tcp_port: Option<NonZeroU16>,
    udp_port: Option<NonZeroU16>,
) -> Result<(String, NonZeroU16, Option<NonZeroU16>), &'static str> {
    let (host, tcp_port, udp_port) = match tcp_port {
        Some(tcp_port) => {
            if address.contains(':') {
                return Err("split bootstrap peer address must not include a port");
            }
            (address, tcp_port, udp_port)
        }
        None => {
            if udp_port.is_some() {
                return Err("bootstrap peer udp_port requires tcp_port");
            }
            let (host, port) = address
                .rsplit_once(':')
                .ok_or("bootstrap peer address must include a port")?;
            let tcp_port = port
                .parse()
                .ok()
                .and_then(NonZeroU16::new)
                .ok_or("bootstrap peer address port must be non-zero")?;
            (host, tcp_port, Some(tcp_port))
        }
    };

    if host.is_empty() {
        return Err("bootstrap peer address host must not be empty");
    }

    Ok((host.to_owned(), tcp_port, udp_port))
}

impl<ST: CertificateSignatureRecoverable> NodeBootstrapPeerConfig<ST> {
    fn validate_address_fields(mut self) -> Result<Self, &'static str> {
        let (address, tcp_port, udp_port) =
            parse_address_fields(&self.address, self.tcp_port, self.udp_port)?;
        self.address = address;
        self.tcp_port = Some(tcp_port);
        self.udp_port = udp_port;
        Ok(self)
    }

    pub fn ip(&self) -> Option<Ipv4Addr> {
        self.address.parse().ok()
    }

    pub fn tcp_port(&self) -> NonZeroU16 {
        self.tcp_port
            .expect("bootstrap peer tcp_port must be present")
    }

    pub fn udp_port(&self) -> Option<NonZeroU16> {
        self.udp_port
    }

    pub fn domain(&self) -> Option<&str> {
        if self.address.parse::<Ipv4Addr>().is_ok() {
            None
        } else {
            Some(&self.address)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use monad_crypto::NopSignature;

    use super::{NodeBootstrapConfig, NodeBootstrapPeerConfig};

    fn bootstrap_peer_toml(address_fields: &str) -> String {
        let pubkey = "01".repeat(32);
        let signature_pubkey = ["1"; 32].join(", ");

        format!(
            r#"[[peers]]
{address_fields}
record_seq_num = 42
secp256k1_pubkey = "0x{pubkey}"
name_record_sig = {{ pubkey = [{signature_pubkey}], id = 1234 }}
auth_port = 9000
direct_udp_port = 9001
"#
        )
    }

    fn parse_bootstrap_peer(
        address_fields: &str,
    ) -> Result<NodeBootstrapPeerConfig<NopSignature>, toml::de::Error> {
        let mut config = toml::from_str::<NodeBootstrapConfig<NopSignature>>(
            &bootstrap_peer_toml(address_fields),
        )?;
        Ok(config.peers.pop().unwrap())
    }

    #[test]
    fn decodes_legacy_socket_addr_toml() {
        let peer = parse_bootstrap_peer(r#"address = "127.0.0.1:8000""#).unwrap();

        assert_eq!(peer.ip().unwrap().to_string(), "127.0.0.1");
        assert_eq!(peer.tcp_port().get(), 8000);
        assert_eq!(peer.udp_port().map(NonZeroU16::get), Some(8000));
        assert_eq!(peer.domain(), None);
        assert_eq!(peer.direct_udp_port.map(NonZeroU16::get), Some(9001));
    }

    #[test]
    fn decodes_legacy_domain_toml() {
        let peer = parse_bootstrap_peer(r#"address = "node.example.com:8000""#).unwrap();

        assert_eq!(peer.ip(), None);
        assert_eq!(peer.tcp_port().get(), 8000);
        assert_eq!(peer.udp_port().map(NonZeroU16::get), Some(8000));
        assert_eq!(peer.domain(), Some("node.example.com"));
    }

    #[test]
    fn rejects_legacy_address_without_port() {
        let err = parse_bootstrap_peer(r#"address = "127.0.0.1""#).unwrap_err();

        assert!(err
            .message()
            .contains("bootstrap peer address must include a port"));
    }

    #[test]
    fn rejects_legacy_address_with_zero_port() {
        let err = parse_bootstrap_peer(r#"address = "127.0.0.1:0""#).unwrap_err();

        assert!(err
            .message()
            .contains("bootstrap peer address port must be non-zero"));
    }

    #[test]
    fn decodes_split_port_toml() {
        let peer = parse_bootstrap_peer(
            r#"address = "127.0.0.2"
tcp_port = 8001
udp_port = 8002"#,
        )
        .unwrap();

        assert_eq!(peer.ip().unwrap().to_string(), "127.0.0.2");
        assert_eq!(peer.tcp_port().get(), 8001);
        assert_eq!(peer.udp_port().map(NonZeroU16::get), Some(8002));
        assert_eq!(peer.domain(), None);
    }

    #[test]
    fn decodes_split_port_toml_without_udp_port() {
        let peer = parse_bootstrap_peer(
            r#"address = "127.0.0.3"
tcp_port = 8003"#,
        )
        .unwrap();

        assert_eq!(peer.ip().unwrap().to_string(), "127.0.0.3");
        assert_eq!(peer.tcp_port().get(), 8003);
        assert_eq!(peer.udp_port(), None);
    }

    #[test]
    fn decodes_split_domain_toml() {
        let peer = parse_bootstrap_peer(
            r#"address = "node.example.com"
tcp_port = 8004"#,
        )
        .unwrap();

        assert_eq!(peer.ip(), None);
        assert_eq!(peer.tcp_port().get(), 8004);
        assert_eq!(peer.udp_port(), None);
        assert_eq!(peer.domain(), Some("node.example.com"));
    }

    #[test]
    fn rejects_split_address_with_embedded_port() {
        let err = parse_bootstrap_peer(
            r#"address = "127.0.0.1:8000"
tcp_port = 8000"#,
        )
        .unwrap_err();

        assert!(err
            .message()
            .contains("split bootstrap peer address must not include a port"));
    }

    #[test]
    fn rejects_unknown_peer_field() {
        let err = parse_bootstrap_peer(
            r#"address = "127.0.0.1:8000"
direct_udp_prt = 9001"#,
        )
        .unwrap_err();

        assert!(err.message().contains("unknown field"));
    }
}
