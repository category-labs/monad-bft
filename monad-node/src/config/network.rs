use std::net::Ipv4Addr;

use serde::Deserialize;

use crate::config::bootstrap::LocalNameRecord;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeNetworkConfig {
    pub local_name_record: LocalNameRecord,
    pub bind_address_host: Ipv4Addr,
    pub bind_address_port: u16,

    pub max_rtt_ms: u64,
    pub max_mbps: u16,
}
