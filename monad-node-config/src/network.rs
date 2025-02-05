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

// When running in docker with vpnkit, the maximum safe MTU is 1480, as per:
// https://github.com/moby/vpnkit/tree/v0.5.0/src/hostnet/slirp.ml#L17-L18
fn default_mtu() -> u16 {
    1480
}
