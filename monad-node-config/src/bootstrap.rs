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

use std::{net::SocketAddrV4, num::NonZeroU16};

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
#[serde(deny_unknown_fields)]
#[serde(bound = "ST: CertificateSignatureRecoverable")]
struct NodeBootstrapPeerConfigSerdeRepr<ST: CertificateSignatureRecoverable> {
    pub address: String,
    pub tcp_port: Option<NonZeroU16>,
    #[serde(default)]
    pub udp_port: Option<NonZeroU16>,

    pub record_seq_num: u64,

    #[serde(deserialize_with = "deserialize_pubkey::<_, CertificateSignaturePubKey<ST>>")]
    pub secp256k1_pubkey: CertificateSignaturePubKey<ST>,

    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub name_record_sig: ST,

    pub auth_port: NonZeroU16,

    #[serde(alias = "direct_udp_auth_port", default)]
    pub direct_udp_port: Option<NonZeroU16>,
}

impl<'de, ST: CertificateSignatureRecoverable> Deserialize<'de> for NodeBootstrapPeerConfig<ST> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let repr = NodeBootstrapPeerConfigSerdeRepr::deserialize(deserializer)?;
        let (address, tcp_port, udp_port) = match repr.address.parse::<SocketAddrV4>() {
            Ok(address) => {
                let port = NonZeroU16::new(address.port())
                    .ok_or_else(|| D::Error::custom("socket address port must be non-zero"))?;
                let tcp_port = repr.tcp_port.unwrap_or(port);
                let udp_port = if repr.tcp_port.is_none() && repr.udp_port.is_none() {
                    Some(port)
                } else {
                    repr.udp_port
                };
                (address.ip().to_string(), tcp_port, udp_port)
            }
            Err(_) => (
                repr.address,
                repr.tcp_port
                    .ok_or_else(|| D::Error::missing_field("tcp_port"))?,
                repr.udp_port,
            ),
        };

        Ok(Self {
            address,
            tcp_port,
            udp_port,
            record_seq_num: repr.record_seq_num,
            secp256k1_pubkey: repr.secp256k1_pubkey,
            name_record_sig: repr.name_record_sig,
            auth_port: repr.auth_port,
            direct_udp_port: repr.direct_udp_port,
        })
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
