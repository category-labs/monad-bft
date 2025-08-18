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

use std::{
    collections::{BTreeSet, HashMap},
    net::{Ipv4Addr, SocketAddrV4},
    str::FromStr,
    time::Duration,
};

const SEQUENCE_TOKEN: &str = "seq";
const CAPABILITIES_TOKEN: &str = "capabilities";

use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable, encode_list};
use message::{PeerLookupRequest, PeerLookupResponse, Ping, Pong};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
    },
    signing_domain,
};
use monad_executor::ExecutorMetrics;
use monad_executor_glue::PeerEntry;
use monad_types::{Epoch, NodeId, Round};
use smallvec::SmallVec;
use tracing::warn;

pub mod discovery;
pub mod driver;
pub mod message;
pub mod mock;

pub use message::PeerDiscoveryMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PortTag {
    TCP = 0,
    UDP = 1,
    DirectUdp = 2,
}

impl PortTag {
    pub fn token(&self) -> &'static str {
        match self {
            PortTag::TCP => "tcp",
            PortTag::UDP => "udp",
            PortTag::DirectUdp => "direct_udp",
        }
    }

    pub fn from_token(token: &str) -> Option<Self> {
        match token {
            "tcp" => Some(PortTag::TCP),
            "udp" => Some(PortTag::UDP),
            "direct_udp" => Some(PortTag::DirectUdp),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Port {
    pub tag: u8,
    pub port: u16,
}

impl Port {
    pub fn new(tag: PortTag, port: u16) -> Self {
        Self {
            tag: tag as u8,
            port,
        }
    }

    pub fn tag_enum(&self) -> Option<PortTag> {
        match self.tag {
            0 => Some(PortTag::TCP),
            1 => Some(PortTag::UDP),
            2 => Some(PortTag::DirectUdp),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WireNameRecord {
    pub ip: Ipv4Addr,
    pub ports: SmallVec<[Port; 3]>,
    pub capabilities: u64,
    pub seq: u64,
}

impl Encodable for WireNameRecord {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let ports_vec: Vec<Port> = self.ports.to_vec();
        let enc: [&dyn Encodable; 4] =
            [&self.ip.octets(), &ports_vec, &self.capabilities, &self.seq];
        encode_list::<_, dyn Encodable>(&enc, out);
    }
}

fn decode_smallvec_with_limit<T: Decodable, const N: usize>(
    buf: &mut &[u8],
    limit: usize,
) -> alloy_rlp::Result<SmallVec<[T; N]>> {
    let mut bytes = alloy_rlp::Header::decode_bytes(buf, true)?;
    let mut vec = SmallVec::new();
    let payload_view = &mut bytes;
    while !payload_view.is_empty() {
        if vec.len() >= limit {
            return Err(alloy_rlp::Error::Custom("Too many items in vector"));
        }
        vec.push(T::decode(payload_view)?);
    }
    Ok(vec)
}

impl Decodable for WireNameRecord {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let buf = &mut alloy_rlp::Header::decode_bytes(buf, true)?;

        let Ok(ip_bytes) = <[u8; 4]>::decode(buf) else {
            warn!("ip address decode failed: {:?}", buf);
            return Err(alloy_rlp::Error::Custom("Invalid IPv4 address"));
        };
        let ip = Ipv4Addr::from(ip_bytes);
        let ports = decode_smallvec_with_limit::<Port, 3>(buf, 8)?;
        let capabilities = u64::decode(buf)?;
        let seq = u64::decode(buf)?;

        Ok(Self {
            ip,
            ports,
            capabilities,
            seq,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NameRecord {
    pub ip: Ipv4Addr,
    pub tcp_port: u16,
    pub udp_port: u16,
    pub direct_udp_port: Option<u16>,
    pub capabilities: u64,
    pub seq: u64,
}

impl Encodable for NameRecord {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let mut ports = SmallVec::new();
        ports.push(Port::new(PortTag::TCP, self.tcp_port));
        ports.push(Port::new(PortTag::UDP, self.udp_port));

        if let Some(direct_udp_port) = self.direct_udp_port {
            ports.push(Port::new(PortTag::DirectUdp, direct_udp_port));
        }

        let wire = WireNameRecord {
            ip: self.ip,
            ports,
            capabilities: self.capabilities,
            seq: self.seq,
        };

        wire.encode(out);
    }
}

impl Decodable for NameRecord {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let wire = WireNameRecord::decode(buf)?;

        let mut tcp_port = None;
        let mut udp_port = None;
        let mut direct_udp_port = None;
        let mut seen_tags = std::collections::HashSet::new();

        for port in &wire.ports {
            if !seen_tags.insert(port.tag) {
                return Err(alloy_rlp::Error::Custom("duplicate port tag"));
            }

            match port.tag_enum() {
                Some(PortTag::TCP) => tcp_port = Some(port.port),
                Some(PortTag::UDP) => udp_port = Some(port.port),
                Some(PortTag::DirectUdp) => direct_udp_port = Some(port.port),
                None => return Err(alloy_rlp::Error::Custom("Invalid port tag")),
            }
        }

        let tcp_port = tcp_port.ok_or(alloy_rlp::Error::Custom("Missing TCP port"))?;
        let udp_port = udp_port.ok_or(alloy_rlp::Error::Custom("Missing UDP port"))?;

        Ok(NameRecord {
            ip: wire.ip,
            tcp_port,
            udp_port,
            direct_udp_port,
            capabilities: wire.capabilities,
            seq: wire.seq,
        })
    }
}

impl NameRecord {
    pub fn tcp_socket(&self) -> SocketAddrV4 {
        SocketAddrV4::new(self.ip, self.tcp_port)
    }

    pub fn udp_socket(&self) -> SocketAddrV4 {
        SocketAddrV4::new(self.ip, self.udp_port)
    }

    pub fn direct_udp_socket(&self) -> Option<SocketAddrV4> {
        self.direct_udp_port
            .map(|port| SocketAddrV4::new(self.ip, port))
    }

    pub fn check_capability(&self, capability: Capability) -> bool {
        (self.capabilities & (1u64 << (capability as u8))) != 0
    }

    pub fn set_capability(&mut self, capability: Capability) {
        self.capabilities |= 1u64 << (capability as u8);
    }

    pub fn capabilities_iter(&self) -> CapabilityIterator {
        CapabilityIterator {
            capabilities: self.capabilities,
            current_bit: 0,
        }
    }
}

impl TryFrom<String> for NameRecord {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let value = value.trim();

        let (address_part, params_part) = if let Some(q_pos) = value.find('?') {
            value.split_at(q_pos)
        } else {
            return Err("Missing '?' separator".to_string());
        };

        let ip =
            Ipv4Addr::from_str(address_part).map_err(|e| format!("Invalid IP address: {}", e))?;

        let params_str = &params_part[1..];

        let mut tcp_port = None;
        let mut udp_port = None;
        let mut direct_udp_port = None;
        let mut seq = None;
        let mut capabilities = 0u64;

        for param in params_str.split(',') {
            let param = param.trim();
            if let Some((key, value)) = param.split_once('=') {
                if let Some(port_tag) = PortTag::from_token(key) {
                    let port = value
                        .parse::<u16>()
                        .map_err(|e| format!("Invalid {} port: {}", key, e))?;
                    match port_tag {
                        PortTag::TCP => tcp_port = Some(port),
                        PortTag::UDP => udp_port = Some(port),
                        PortTag::DirectUdp => direct_udp_port = Some(port),
                    }
                } else if key == SEQUENCE_TOKEN {
                    seq = Some(
                        value
                            .parse::<u64>()
                            .map_err(|e| format!("Invalid seq: {}", e))?,
                    );
                } else if key == CAPABILITIES_TOKEN {
                    for cap_token in value.split('+') {
                        let cap_token = cap_token.trim();
                        if let Some(cap) = Capability::from_token(cap_token) {
                            capabilities |= 1u64 << (cap as u8);
                        } else {
                            return Err(format!("Unknown capability: {}", cap_token));
                        }
                    }
                }
            }
        }

        let tcp_port = tcp_port.ok_or("Missing TCP port")?;
        let udp_port = udp_port.ok_or("Missing UDP port")?;
        let seq = seq.ok_or("Missing seq")?;

        Ok(NameRecord {
            ip,
            tcp_port,
            udp_port,
            direct_udp_port,
            capabilities,
            seq,
        })
    }
}

impl From<NameRecord> for String {
    fn from(record: NameRecord) -> Self {
        let mut params = vec![
            format!("{}={}", PortTag::TCP.token(), record.tcp_port),
            format!("{}={}", PortTag::UDP.token(), record.udp_port),
        ];

        if let Some(port) = record.direct_udp_port {
            params.push(format!("{}={}", PortTag::DirectUdp.token(), port));
        }

        params.push(format!("{}={}", SEQUENCE_TOKEN, record.seq));

        if record.capabilities != 0 {
            let caps: Vec<String> = record
                .capabilities_iter()
                .map(|cap| cap.token().to_string())
                .collect();
            if !caps.is_empty() {
                params.push(format!("{}={}", CAPABILITIES_TOKEN, caps.join("+")));
            }
        }

        format!("{}?{}", record.ip, params.join(","))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {
    Test = 0,
}

impl Capability {
    pub fn from_bit(bit: u8) -> Option<Self> {
        match bit {
            0 => Some(Capability::Test),
            _ => None,
        }
    }

    pub fn token(&self) -> &'static str {
        match self {
            Capability::Test => "test",
        }
    }

    pub fn from_token(token: &str) -> Option<Self> {
        match token {
            "test" => Some(Capability::Test),
            _ => None,
        }
    }
}

pub struct CapabilityIterator {
    capabilities: u64,
    current_bit: u8,
}

impl Iterator for CapabilityIterator {
    type Item = Capability;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_bit < 64 {
            let bit = self.current_bit;
            self.current_bit += 1;

            if (self.capabilities & (1u64 << bit)) != 0 {
                if let Some(cap) = Capability::from_bit(bit) {
                    return Some(cap);
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, RlpEncodable, RlpDecodable, Eq)]
pub struct MonadNameRecord<ST: CertificateSignatureRecoverable> {
    pub name_record: NameRecord,
    pub signature: ST,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KnownNameRecord<PK: PubKey> {
    pub name_record: NameRecord,
    pub pubkey: PK,
}

impl<ST: CertificateSignatureRecoverable + Copy> MonadNameRecord<ST> {
    pub fn new(name_record: NameRecord, key: &ST::KeyPairType) -> Self {
        let mut encoded = Vec::new();
        name_record.encode(&mut encoded);
        let signature = ST::sign::<signing_domain::NameRecord>(&encoded, key);
        Self {
            name_record,
            signature,
        }
    }

    pub fn recover_pubkey(
        &self,
    ) -> Result<NodeId<CertificateSignaturePubKey<ST>>, <ST as CertificateSignature>::Error> {
        let mut encoded = Vec::new();
        self.name_record.encode(&mut encoded);
        let pubkey = self
            .signature
            .recover_pubkey::<signing_domain::NameRecord>(&encoded)?;
        Ok(NodeId::new(pubkey))
    }

    pub fn address(&self) -> SocketAddrV4 {
        self.name_record.tcp_socket()
    }

    pub fn seq(&self) -> u64 {
        self.name_record.seq
    }
}

impl<PK: PubKey> KnownNameRecord<PK> {
    pub fn from_monad_record<ST>(record: MonadNameRecord<ST>) -> Result<Self, ST::Error>
    where
        ST: CertificateSignatureRecoverable,
        PK: From<CertificateSignaturePubKey<ST>>,
    {
        let mut encoded = Vec::new();
        record.name_record.encode(&mut encoded);
        let pubkey = record
            .signature
            .recover_pubkey::<signing_domain::NameRecord>(&encoded)?;

        Ok(KnownNameRecord {
            name_record: record.name_record,
            pubkey: PK::from(pubkey),
        })
    }
}

impl<PK: PubKey> TryFrom<String> for KnownNameRecord<PK> {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let value = value.trim();

        let at_pos = value.find('@').ok_or("Missing '@' separator")?;
        let (pubkey_part, addr_params) = value.split_at(at_pos);

        let pubkey_bytes =
            hex::decode(pubkey_part).map_err(|e| format!("Invalid hex pubkey: {}", e))?;

        let pubkey = PK::from_bytes(&pubkey_bytes).map_err(|e| format!("Invalid pubkey: {}", e))?;

        let full_addr_params = addr_params[1..].to_string();
        let name_record = NameRecord::try_from(full_addr_params)?;

        Ok(KnownNameRecord {
            name_record,
            pubkey,
        })
    }
}

impl<PK: PubKey> From<KnownNameRecord<PK>> for String {
    fn from(record: KnownNameRecord<PK>) -> Self {
        let pubkey_bytes = record.pubkey.bytes();
        let pubkey_hex = hex::encode(&pubkey_bytes);

        let name_record_str = String::from(record.name_record);
        format!("{}@{}", pubkey_hex, name_record_str)
    }
}

#[derive(Debug, Clone)]
pub enum PeerDiscoveryEvent<ST: CertificateSignatureRecoverable> {
    SendPing {
        to: NodeId<CertificateSignaturePubKey<ST>>,
    },
    PingRequest {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        ping: Ping<ST>,
    },
    PongResponse {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        pong: Pong,
    },
    PingTimeout {
        to: NodeId<CertificateSignaturePubKey<ST>>,
        ping_id: u32,
    },
    SendPeerLookup {
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        open_discovery: bool,
    },
    PeerLookupRequest {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        request: PeerLookupRequest<ST>,
    },
    PeerLookupResponse {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        response: PeerLookupResponse<ST>,
    },
    PeerLookupTimeout {
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        lookup_id: u32,
    },
    UpdateCurrentRound {
        round: Round,
        epoch: Epoch,
    },
    UpdateValidatorSet {
        epoch: Epoch,
        validators: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    },
    UpdatePeers {
        peers: Vec<PeerEntry<ST>>,
    },
    UpdateConfirmGroup {
        end_round: Round,
        peers: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    },
    Refresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimerKind {
    SendPing,
    PingTimeout,
    RetryPeerLookup { lookup_id: u32 },
    Refresh,
}

#[derive(Debug)]
pub enum PeerDiscoveryTimerCommand<E, ST: CertificateSignatureRecoverable> {
    Schedule {
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
        timer_kind: TimerKind,
        duration: Duration,
        on_timeout: E,
    },
    ScheduleReset {
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
        timer_kind: TimerKind,
    },
}

#[derive(Debug)]
pub struct PeerDiscoveryMetricsCommand(ExecutorMetrics);

#[derive(Debug)]
pub enum PeerDiscoveryCommand<ST: CertificateSignatureRecoverable> {
    RouterCommand {
        target: NodeId<CertificateSignaturePubKey<ST>>,
        message: PeerDiscoveryMessage<ST>,
    },
    TimerCommand(PeerDiscoveryTimerCommand<PeerDiscoveryEvent<ST>, ST>),
    MetricsCommand(PeerDiscoveryMetricsCommand),
}

pub trait PeerDiscoveryAlgo {
    type SignatureType: CertificateSignatureRecoverable;

    fn send_ping(
        &mut self,
        target: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_ping(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        ping: Ping<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_pong(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        pong: Pong,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_ping_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        ping_id: u32,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn send_peer_lookup_request(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        target: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        open_discovery: bool,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_peer_lookup_request(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        request: PeerLookupRequest<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_peer_lookup_response(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        response: PeerLookupResponse<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_peer_lookup_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        target: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        lookup_id: u32,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn refresh(&mut self) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn update_current_round(
        &mut self,
        round: Round,
        epoch: Epoch,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn update_validator_set(
        &mut self,
        epoch: Epoch,
        validators: BTreeSet<NodeId<CertificateSignaturePubKey<Self::SignatureType>>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn update_peers(
        &mut self,
        peers: Vec<PeerEntry<Self::SignatureType>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn update_peer_participation(
        &mut self,
        round: Round,
        peers: BTreeSet<NodeId<CertificateSignaturePubKey<Self::SignatureType>>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn metrics(&self) -> &ExecutorMetrics;

    fn get_addr_by_id(
        &self,
        id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<SocketAddrV4>;

    fn get_known_addrs(
        &self,
    ) -> HashMap<NodeId<CertificateSignaturePubKey<Self::SignatureType>>, SocketAddrV4>;

    fn get_fullnode_addrs(
        &self,
    ) -> HashMap<NodeId<CertificateSignaturePubKey<Self::SignatureType>>, SocketAddrV4>;

    fn get_name_records(
        &self,
    ) -> HashMap<
        NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        MonadNameRecord<Self::SignatureType>,
    >;

    fn get_name_record(
        &self,
        node_id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<MonadNameRecord<Self::SignatureType>>;
}

pub trait PeerDiscoveryAlgoBuilder {
    type PeerDiscoveryAlgoType: PeerDiscoveryAlgo;

    fn build(
        self,
    ) -> (
        Self::PeerDiscoveryAlgoType,
        Vec<
            PeerDiscoveryCommand<<Self::PeerDiscoveryAlgoType as PeerDiscoveryAlgo>::SignatureType>,
        >,
    );
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use monad_secp::KeyPair;
    use rstest::rstest;

    use super::*;

    #[test]
    fn test_name_record_v4_rlp() {
        let name_record = NameRecord {
            ip: Ipv4Addr::from_str("1.1.1.1").unwrap(),
            tcp_port: 8000,
            udp_port: 8001,
            direct_udp_port: None,
            capabilities: 0,
            seq: 2,
        };

        let mut encoded = Vec::new();
        name_record.encode(&mut encoded);

        let result = NameRecord::decode(&mut encoded.as_slice());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), name_record);
    }

    #[test]
    fn test_name_record_validation() {
        let name_record = NameRecord {
            ip: Ipv4Addr::from_str("1.1.1.1").unwrap(),
            tcp_port: 8000,
            udp_port: 8001,
            direct_udp_port: None,
            capabilities: 0,
            seq: 1,
        };

        assert_eq!(
            name_record.tcp_socket(),
            SocketAddrV4::from_str("1.1.1.1:8000").unwrap()
        );
        assert_eq!(
            name_record.udp_socket(),
            SocketAddrV4::from_str("1.1.1.1:8001").unwrap()
        );
        assert_eq!(name_record.direct_udp_socket(), None);

        let name_record_with_direct = NameRecord {
            ip: Ipv4Addr::from_str("1.1.1.1").unwrap(),
            tcp_port: 8000,
            udp_port: 8001,
            direct_udp_port: Some(8002),
            capabilities: 0,
            seq: 1,
        };
        assert_eq!(
            name_record_with_direct.direct_udp_socket(),
            Some(SocketAddrV4::from_str("1.1.1.1:8002").unwrap())
        );
    }

    #[test]
    fn test_name_record_duplicate_port_validation() {
        let wire = WireNameRecord {
            ip: Ipv4Addr::from_str("1.1.1.1").unwrap(),
            ports: smallvec::smallvec![
                Port::new(PortTag::TCP, 8000),
                Port::new(PortTag::UDP, 8001),
                Port::new(PortTag::TCP, 8002),
            ],
            capabilities: 0,
            seq: 1,
        };

        let mut encoded = Vec::new();
        wire.encode(&mut encoded);

        let decoded = NameRecord::decode(&mut encoded.as_slice());
        assert!(decoded.is_err());
    }

    #[test]
    fn test_name_record_missing_port_validation() {
        let wire = WireNameRecord {
            ip: Ipv4Addr::from_str("1.1.1.1").unwrap(),
            ports: smallvec::smallvec![Port::new(PortTag::TCP, 8000)],
            capabilities: 0,
            seq: 1,
        };

        let mut encoded = Vec::new();
        wire.encode(&mut encoded);

        let decoded = NameRecord::decode(&mut encoded.as_slice());
        assert!(decoded.is_err());
    }

    #[test]
    fn test_name_record_encode_snapshot() {
        let name_record = NameRecord {
            ip: Ipv4Addr::from_str("192.168.1.1").unwrap(),
            tcp_port: 8080,
            udp_port: 8081,
            direct_udp_port: None,
            capabilities: 0,
            seq: 42,
        };

        let mut encoded = Vec::new();
        name_record.encode(&mut encoded);
        let hex_encoded = hex::encode(&encoded);
        insta::assert_snapshot!(hex_encoded);
    }

    #[test]
    fn test_name_record_with_direct_udp_encode_snapshot() {
        let name_record = NameRecord {
            ip: Ipv4Addr::from_str("10.0.0.1").unwrap(),
            tcp_port: 9000,
            udp_port: 9001,
            direct_udp_port: Some(9002),
            capabilities: 1,
            seq: 100,
        };

        let mut encoded = Vec::new();
        name_record.encode(&mut encoded);
        let hex_encoded = hex::encode(&encoded);
        insta::assert_snapshot!(hex_encoded);
    }

    #[test]
    fn test_name_record_roundtrip() {
        let original = NameRecord {
            ip: Ipv4Addr::from_str("172.16.0.1").unwrap(),
            tcp_port: 7000,
            udp_port: 7001,
            direct_udp_port: Some(7002),
            capabilities: 255,
            seq: 999,
        };

        let mut encoded = Vec::new();
        original.encode(&mut encoded);

        let decoded = NameRecord::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(original, decoded);
    }

    #[rstest]
    #[case(
        "192.168.1.1?tcp=8080,udp=8081,seq=42",
        "192.168.1.1",
        8080,
        8081,
        None,
        42,
        0
    )]
    #[case(
        "10.0.0.1?tcp=9000,udp=9001,direct_udp=9002,seq=100,capabilities=test",
        "10.0.0.1",
        9000,
        9001,
        Some(9002),
        100,
        1
    )]
    #[case(
        "172.16.0.1?tcp=7000,udp=7001,direct_udp=7002,seq=999,capabilities=test",
        "172.16.0.1",
        7000,
        7001,
        Some(7002),
        999,
        1
    )]
    fn test_name_record_from_string(
        #[case] url: &str,
        #[case] expected_ip: &str,
        #[case] expected_tcp: u16,
        #[case] expected_udp: u16,
        #[case] expected_direct_udp: Option<u16>,
        #[case] expected_seq: u64,
        #[case] expected_capabilities: u64,
    ) {
        let record = NameRecord::try_from(url.to_string()).unwrap();

        assert_eq!(record.ip, Ipv4Addr::from_str(expected_ip).unwrap());
        assert_eq!(record.tcp_port, expected_tcp);
        assert_eq!(record.udp_port, expected_udp);
        assert_eq!(record.direct_udp_port, expected_direct_udp);
        assert_eq!(record.seq, expected_seq);
        assert_eq!(record.capabilities, expected_capabilities);

        // Test roundtrip
        let url_out = String::from(record);
        let record2 = NameRecord::try_from(url_out).unwrap();
        assert_eq!(record, record2);
    }

    #[rstest]
    #[case(
        "192.168.1.1",
        8080,
        8081,
        None,
        0,
        42,
        "192.168.1.1?tcp=8080,udp=8081,seq=42"
    )]
    #[case(
        "10.0.0.1",
        9000,
        9001,
        Some(9002),
        1,
        100,
        "10.0.0.1?tcp=9000,udp=9001,direct_udp=9002,seq=100,capabilities=test"
    )]
    #[case("127.0.0.1", 80, 443, None, 2, 1, "127.0.0.1?tcp=80,udp=443,seq=1")]
    #[case(
        "172.16.0.1",
        3000,
        3001,
        Some(3002),
        3,
        999,
        "172.16.0.1?tcp=3000,udp=3001,direct_udp=3002,seq=999,capabilities=test"
    )]
    fn test_name_record_to_string(
        #[case] ip: &str,
        #[case] tcp_port: u16,
        #[case] udp_port: u16,
        #[case] direct_udp_port: Option<u16>,
        #[case] capabilities: u64,
        #[case] seq: u64,
        #[case] expected_url: &str,
    ) {
        let record = NameRecord {
            ip: Ipv4Addr::from_str(ip).unwrap(),
            tcp_port,
            udp_port,
            direct_udp_port,
            capabilities,
            seq,
        };

        let url = String::from(record);
        assert_eq!(url, expected_url);
    }

    #[rstest]
    #[case("192.168.1.1?udp=8081,seq=42", "Missing TCP port")]
    #[case("192.168.1.1?tcp=8080,seq=42", "Missing UDP port")]
    #[case("192.168.1.1?tcp=8080,udp=8081", "Missing seq")]
    #[case("192.168.1.1tcp=8080,udp=8081,seq=42", "Missing '?' separator")]
    #[case(
        "192.168.1.1?tcp=8080,udp=8081,seq=42,capabilities=unknown",
        "Unknown capability: unknown"
    )]
    #[case(
        "192.168.1.1?tcp=8080,udp=8081,seq=42,capabilities=test+invalid",
        "Unknown capability: invalid"
    )]
    fn test_name_record_from_string_errors(#[case] url: &str, #[case] expected_error: &str) {
        let result = NameRecord::try_from(url.to_string());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), expected_error);
    }

    #[test]
    fn test_name_record_from_string_invalid_ip() {
        let url = "invalid_ip?tcp=8080,udp=8081,seq=42".to_string();
        let result = NameRecord::try_from(url);
        assert!(result.is_err());
        assert!(result.unwrap_err().starts_with("Invalid IP address"));
    }

    #[rstest]
    #[case(0, vec![])]
    #[case(1, vec![Capability::Test])]
    #[case(2, vec![])]
    #[case(3, vec![Capability::Test])]
    fn test_capability_iterator(#[case] capabilities: u64, #[case] expected: Vec<Capability>) {
        let record = NameRecord {
            ip: Ipv4Addr::from_str("127.0.0.1").unwrap(),
            tcp_port: 80,
            udp_port: 80,
            direct_udp_port: None,
            capabilities,
            seq: 1,
        };

        let caps: Vec<Capability> = record.capabilities_iter().collect();
        assert_eq!(caps, expected);
    }

    #[rstest]
    #[case(
        "033b5b7ad0e97ba616fb816354e0a00f957fff11a4a7e5b829bde964aeba00e39f@192.168.1.1?tcp=8080,udp=8081,seq=42",
        "192.168.1.1",
        8080,
        8081,
        None,
        42,
        0
    )]
    #[case(
        "033b5b7ad0e97ba616fb816354e0a00f957fff11a4a7e5b829bde964aeba00e39f@10.0.0.1?tcp=9000,udp=9001,direct_udp=9002,seq=100,capabilities=test",
        "10.0.0.1",
        9000,
        9001,
        Some(9002),
        100,
        1
    )]
    fn test_known_name_record_from_string(
        #[case] url: &str,
        #[case] expected_ip: &str,
        #[case] expected_tcp: u16,
        #[case] expected_udp: u16,
        #[case] expected_direct_udp: Option<u16>,
        #[case] expected_seq: u64,
        #[case] expected_capabilities: u64,
    ) {
        let result = KnownNameRecord::<monad_secp::PubKey>::try_from(url.to_string());

        if let Ok(known_record) = result {
            assert_eq!(
                known_record.name_record.ip,
                Ipv4Addr::from_str(expected_ip).unwrap()
            );
            assert_eq!(known_record.name_record.tcp_port, expected_tcp);
            assert_eq!(known_record.name_record.udp_port, expected_udp);
            assert_eq!(
                known_record.name_record.direct_udp_port,
                expected_direct_udp
            );
            assert_eq!(known_record.name_record.seq, expected_seq);
            assert_eq!(known_record.name_record.capabilities, expected_capabilities);

            // Test roundtrip
            let url_out = String::from(known_record);
            let known_record2 = KnownNameRecord::<monad_secp::PubKey>::try_from(url_out).unwrap();
            assert_eq!(known_record, known_record2);
        }
    }

    #[rstest]
    #[case(
        "invalid_hex@192.168.1.1?tcp=8080,udp=8081,seq=42",
        "Invalid hex pubkey"
    )]
    #[case("192.168.1.1?tcp=8080,udp=8081,seq=42", "Missing '@' separator")]
    #[case(
        "033b5b7ad0e97ba616fb816354e0a00f957fff11a4a7e5b829bde964aeba00e39f@invalid_ip?tcp=8080,udp=8081,seq=42",
        "Invalid IP address"
    )]
    #[case(
        "033b5b7ad0e97ba616fb816354e0a00f957fff11a4a7e5b829bde964aeba00e39f@192.168.1.1?tcp=8080,udp=8081",
        "Missing seq"
    )]
    fn test_known_name_record_from_string_errors(
        #[case] url: &str,
        #[case] expected_error_prefix: &str,
    ) {
        let result = KnownNameRecord::<monad_secp::PubKey>::try_from(url.to_string());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains(expected_error_prefix));
    }

    #[test]
    fn test_known_name_record_from_monad_record() {
        let name_record = NameRecord {
            ip: Ipv4Addr::from_str("192.168.1.1").unwrap(),
            tcp_port: 8080,
            udp_port: 8081,
            direct_udp_port: None,
            capabilities: 0,
            seq: 42,
        };

        let mut secret = [0u8; 32];
        secret[0] = 1;
        let keypair = KeyPair::from_bytes(&mut secret).unwrap();

        let monad_record = MonadNameRecord::<monad_secp::SecpSignature>::new(name_record, &keypair);
        let known_record =
            KnownNameRecord::<monad_secp::PubKey>::from_monad_record(monad_record).unwrap();

        assert_eq!(known_record.name_record, name_record);
        assert_eq!(known_record.pubkey, keypair.pubkey());
    }

    #[test]
    fn test_wire_name_record_compatibility() {
        let name_record = NameRecord {
            ip: Ipv4Addr::from_str("127.0.0.1").unwrap(),
            tcp_port: 8000,
            udp_port: 8001,
            direct_udp_port: Some(8002),
            capabilities: 0,
            seq: 1,
        };

        let mut encoded = Vec::new();
        name_record.encode(&mut encoded);

        let wire = WireNameRecord::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(wire.ports.len(), 3);
        assert_eq!(wire.ports[0].tag, PortTag::TCP as u8);
        assert_eq!(wire.ports[0].port, 8000);
        assert_eq!(wire.ports[1].tag, PortTag::UDP as u8);
        assert_eq!(wire.ports[1].port, 8001);
        assert_eq!(wire.ports[2].tag, PortTag::DirectUdp as u8);
        assert_eq!(wire.ports[2].port, 8002);
    }
}

#[cfg(test)]
mod proptest_tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn test_name_record_string_roundtrip(
            a in 0u8..=255u8,
            b in 0u8..=255u8,
            c in 0u8..=255u8,
            d in 0u8..=255u8,
            tcp_port in 1u16..=65535u16,
            udp_port in 1u16..=65535u16,
            direct_udp_port in prop::option::of(1u16..=65535u16),
            capabilities in 0u64..=1u64,
            seq in 0u64..=u64::MAX,
        ) {
            let original = NameRecord {
                ip: Ipv4Addr::new(a, b, c, d),
                tcp_port,
                udp_port,
                direct_udp_port,
                capabilities,
                seq,
            };

            let url = String::from(original);
            let decoded = NameRecord::try_from(url.clone()).unwrap();

            prop_assert_eq!(original.ip, decoded.ip);
            prop_assert_eq!(original.tcp_port, decoded.tcp_port);
            prop_assert_eq!(original.udp_port, decoded.udp_port);
            prop_assert_eq!(original.direct_udp_port, decoded.direct_udp_port);
            prop_assert_eq!(original.seq, decoded.seq);
            prop_assert_eq!(original.capabilities, decoded.capabilities);
        }

    }
}
