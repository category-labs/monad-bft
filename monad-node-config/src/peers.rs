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

use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_types::Round;
use serde::{de::Error as _, Deserialize, Serialize};

use crate::bootstrap::parse_address_fields;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
#[serde(bound = "ST: CertificateSignatureRecoverable")]
pub struct PeerDiscoveryConfig<ST: CertificateSignatureRecoverable> {
    pub self_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_tcp_port: Option<NonZeroU16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_udp_port: Option<NonZeroU16>,

    pub self_auth_port: NonZeroU16,
    #[serde(alias = "self_direct_udp_auth_port")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_direct_udp_port: Option<NonZeroU16>,
    pub self_record_seq_num: u64,

    pub self_name_record_sig: ST,

    pub refresh_period: u64,
    pub request_timeout: u64,
    pub unresponsive_prune_threshold: u32,
    pub last_participation_prune_threshold: Round,
    pub min_num_peers: usize,
    pub max_num_peers: usize,
    pub ping_rate_limit_per_second: u32,
}

pub(crate) fn deserialize_peer_discovery_config<'de, D, ST>(
    deserializer: D,
) -> Result<PeerDiscoveryConfig<ST>, D::Error>
where
    D: serde::Deserializer<'de>,
    ST: CertificateSignatureRecoverable,
{
    PeerDiscoveryConfig::<ST>::deserialize(deserializer)?
        .validate_address_fields()
        .map_err(D::Error::custom)
}

impl<ST: CertificateSignatureRecoverable> PeerDiscoveryConfig<ST> {
    fn validate_address_fields(mut self) -> Result<Self, &'static str> {
        let (self_address, self_tcp_port, self_udp_port) =
            parse_address_fields(&self.self_address, self.self_tcp_port, self.self_udp_port)?;
        self.self_address = self_address;
        self.self_tcp_port = Some(self_tcp_port);
        self.self_udp_port = self_udp_port;
        Ok(self)
    }

    pub fn ip(&self) -> Option<Ipv4Addr> {
        self.self_address.parse().ok()
    }

    pub fn tcp_port(&self) -> NonZeroU16 {
        self.self_tcp_port
            .expect("peer discovery tcp_port must be present")
    }

    pub fn udp_port(&self) -> Option<NonZeroU16> {
        self.self_udp_port
    }

    pub fn domain(&self) -> Option<&str> {
        if self.self_address.parse::<Ipv4Addr>().is_ok() {
            None
        } else {
            Some(&self.self_address)
        }
    }
}
