use std::net::Ipv4Addr;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeNetworkConfig {
    pub bind_address_host: Ipv4Addr,
    pub bind_address_port: u16,

    pub libp2p_timeout_ms: u64,
    pub libp2p_keepalive_ms: u64,
}
