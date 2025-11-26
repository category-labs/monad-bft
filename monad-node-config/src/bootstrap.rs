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

use std::net::SocketAddrV4;

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::PeerEntry;
use monad_types::{deserialize_pubkey, serialize_pubkey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapConfig<ST: CertificateSignatureRecoverable> {
    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub peers: Vec<NodeBootstrapPeerConfig<ST>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
#[serde(bound = "ST: CertificateSignatureRecoverable")]
pub struct NodeBootstrapPeerConfig<ST: CertificateSignatureRecoverable> {
    pub address: String,

    pub record_seq_num: u64,

    #[serde(serialize_with = "serialize_pubkey::<_, CertificateSignaturePubKey<ST>>")]
    #[serde(deserialize_with = "deserialize_pubkey::<_, CertificateSignaturePubKey<ST>>")]
    pub secp256k1_pubkey: CertificateSignaturePubKey<ST>,

    #[serde(bound = "ST: CertificateSignatureRecoverable")]
    pub name_record_sig: ST,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_port: Option<u16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_tcp_port: Option<u16>,
}

impl<ST, T> From<T> for NodeBootstrapPeerConfig<ST>
where
    ST: CertificateSignatureRecoverable,
    T: AsRef<PeerEntry<ST>>,
{
    fn from(peer: T) -> Self {
        let peer = peer.as_ref();
        Self {
            address: peer.addr.to_string(),
            secp256k1_pubkey: peer.pubkey,
            name_record_sig: peer.signature,
            record_seq_num: peer.record_seq_num,
            auth_port: peer.auth_port,
            auth_tcp_port: peer.auth_tcp_port,
        }
    }
}

impl<ST: CertificateSignatureRecoverable> NodeBootstrapPeerConfig<ST> {
    pub fn with_resolved_addr(&self, addr: SocketAddrV4) -> PeerEntry<ST> {
        PeerEntry {
            pubkey: self.secp256k1_pubkey,
            addr,
            signature: self.name_record_sig,
            record_seq_num: self.record_seq_num,
            auth_port: self.auth_port,
            auth_tcp_port: self.auth_tcp_port,
        }
    }
}
