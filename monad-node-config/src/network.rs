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

use monad_types::DEFAULT_MTU;
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

    #[serde(default = "default_tcp_connections_limit")]
    pub tcp_connections_limit: usize,

    #[serde(default = "default_tcp_per_ip_connections_limit")]
    pub tcp_per_ip_connections_limit: usize,

    #[serde(default = "default_tcp_rate_limit_rps")]
    pub tcp_rate_limit_rps: u32,

    #[serde(default = "default_tcp_rate_limit_burst")]
    pub tcp_rate_limit_burst: u32,
}

fn default_mtu() -> u16 {
    DEFAULT_MTU
}

fn default_buffer_size() -> Option<usize> {
    // recommended value at the time of the commit
    Some(62_500_000)
}

fn default_udp_message_max_age_ms() -> u64 {
    10_000 // 10 seconds in milliseconds
}

fn default_tcp_connections_limit() -> usize {
    1000
}

fn default_tcp_per_ip_connections_limit() -> usize {
    5
}

fn default_tcp_rate_limit_rps() -> u32 {
    1000
}

fn default_tcp_rate_limit_burst() -> u32 {
    200
}
