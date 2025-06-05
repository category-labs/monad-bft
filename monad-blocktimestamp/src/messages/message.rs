use alloy_rlp::{RlpDecodable, RlpEncodable};

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct PingResponseMessage {
    pub sequence: u64,
    pub avg_wait_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct PingRequestMessage {
    pub sequence: u64,
}
