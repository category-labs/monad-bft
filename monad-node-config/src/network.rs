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

use std::net::Ipv4Addr;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeNetworkConfig {
    pub bind_address_host: Ipv4Addr,
    pub bind_address_port: u16,

    pub max_rtt_ms: u64,
    pub max_mbps: u16,

    #[serde(default = "default_buffer_size")]
    pub buffer_size: Option<usize>,

    #[serde(default = "default_mtu")]
    pub mtu: u16,

    #[serde(default = "default_udp_message_max_age_ms")]
    pub udp_message_max_age_ms: u64,
}

// When running in docker with vpnkit, the maximum safe MTU is 1480, as per:
// https://github.com/moby/vpnkit/tree/v0.5.0/src/hostnet/slirp.ml#L17-L18
fn default_mtu() -> u16 {
    1480
}

fn default_buffer_size() -> Option<usize> {
    // recommended value at the time of the commit
    Some(62_500_000)
}

fn default_udp_message_max_age_ms() -> u64 {
    10_000 // 10 seconds in milliseconds
}
