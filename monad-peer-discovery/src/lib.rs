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
    time::Duration,
};

use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable, encode_list};
use message::{PeerLookupRequest, PeerLookupResponse, Ping, Pong};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {}

#[derive(Debug, Clone, Copy, PartialEq, RlpEncodable, RlpDecodable, Eq)]
pub struct MonadNameRecord<ST: CertificateSignatureRecoverable> {
    pub name_record: NameRecord,
    pub signature: ST,
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
