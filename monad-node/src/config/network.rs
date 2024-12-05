use std::net::Ipv4Addr;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeNetworkConfig {
    pub bind_address_host: Ipv4Addr,
    pub bind_address_port: u16,

    pub max_rtt_ms: u64,
    pub max_mbps: u16,

    #[serde(default = "default_mtu")]
    pub mtu: u16,
}

// TODO: When running in docker with vpnkit, we seem to occasionally get ICMP frag-neededs
// for 20 bytes less than the expected MTU -- investigate what's causing this.
fn default_mtu() -> u16 {
    1480
}
