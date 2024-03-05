use std::net::Ipv4Addr;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeNetworkConfig {
    pub bind_address_host: Ipv4Addr,
    pub bind_address_port: u16,

    pub max_rtt_ms: u64,
    pub max_mbps: u16,
}
