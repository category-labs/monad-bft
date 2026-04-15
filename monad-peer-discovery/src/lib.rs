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
    collections::{BTreeSet, HashMap, HashSet},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    num::NonZeroU16,
    time::Duration,
};

use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable, encode_list};
use arrayvec::ArrayVec;
use message::{PeerLookupRequest, PeerLookupResponse, Ping, Pong};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    signing_domain,
};
use monad_executor::ExecutorMetrics;
use monad_executor_glue::PeerEntry;
use monad_node_config::NodeBootstrapPeerConfig;
use monad_types::{Epoch, NodeId, Round};
use tracing::debug;

pub mod discovery;
pub mod driver;
pub mod ipv4_validation;
pub mod message;
pub mod mock;

pub use message::PeerDiscoveryMessage;

#[derive(Debug, Clone)]
pub struct PeerSource<PK: monad_crypto::certificate_signature::PubKey> {
    pub id: NodeId<PK>,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PortTag {
    TCP = 0,
    UDP = 1,
    AuthenticatedUDP = 2,
    DirectUDP = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Port {
    pub tag: u8,
    pub port: NonZeroU16,
}

impl Port {
    pub fn new(tag: PortTag, port: u16) -> Self {
        Self {
            tag: tag as u8,
            port: NonZeroU16::new(port).expect("name record port must be non-zero"),
        }
    }

    pub fn tag_enum(&self) -> Option<PortTag> {
        match self.tag {
            0 => Some(PortTag::TCP),
            1 => Some(PortTag::UDP),
            2 => Some(PortTag::AuthenticatedUDP),
            3 => Some(PortTag::DirectUDP),
            _ => None,
        }
    }
}

impl Encodable for Port {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let port = self.port.get();
        let enc: [&dyn Encodable; 2] = [&self.tag, &port];
        encode_list::<_, dyn Encodable>(&enc, out);
    }
}

impl Decodable for Port {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let payload = &mut alloy_rlp::Header::decode_bytes(buf, true)?;
        let tag = u8::decode(payload)?;
        let port = NonZeroU16::new(u16::decode(payload)?)
            .ok_or(alloy_rlp::Error::Custom("invalid port"))?;

        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }

        Ok(Self { tag, port })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PortList<const N: usize>(ArrayVec<Port, N>);

impl<const N: usize> Encodable for PortList<N> {
    fn length(&self) -> usize {
        alloy_rlp::list_length(&self.0)
    }

    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        alloy_rlp::encode_list(&self.0, out)
    }
}

impl<const N: usize> Decodable for PortList<N> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let payload = &mut alloy_rlp::Header::decode_bytes(buf, true)?;
        let mut vec = ArrayVec::new();
        while !payload.is_empty() {
            let port = Port::decode(payload)?;
            if vec.try_push(port).is_err() {
                return Err(alloy_rlp::Error::Custom("too many ports"));
            }
        }
        Ok(PortList(vec))
    }
}

impl<const N: usize> std::ops::Deref for PortList<N> {
    type Target = ArrayVec<Port, N>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const N: usize> std::ops::DerefMut for PortList<N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<const N: usize> From<ArrayVec<Port, N>> for PortList<N> {
    fn from(vec: ArrayVec<Port, N>) -> Self {
        Self(vec)
    }
}

impl<const N: usize> AsRef<[Port]> for PortList<N> {
    fn as_ref(&self) -> &[Port] {
        &self.0
    }
}

impl<const N: usize> PortList<N> {
    fn port_by_tag(&self, tag: PortTag) -> Option<NonZeroU16> {
        self.0
            .iter()
            .find(|p| p.tag_enum() == Some(tag))
            .map(|p| p.port)
    }

    fn tcp_port(&self) -> Option<NonZeroU16> {
        self.port_by_tag(PortTag::TCP)
    }

    fn udp_port(&self) -> Option<NonZeroU16> {
        self.port_by_tag(PortTag::UDP)
    }

    fn authenticated_udp_port(&self) -> Option<NonZeroU16> {
        self.port_by_tag(PortTag::AuthenticatedUDP)
    }

    fn direct_udp_port(&self) -> Option<NonZeroU16> {
        self.port_by_tag(PortTag::DirectUDP)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameRecord {
    ip: Ipv4Addr,
    ports: PortList<8>,
    capabilities: u64,
    seq: u64,
}

impl NameRecord {
    fn validate(&self) -> alloy_rlp::Result<()> {
        let mut seen_tags = HashSet::new();
        for port in self.ports.iter() {
            if !seen_tags.insert(port.tag) {
                return Err(alloy_rlp::Error::Custom("duplicate port tag"));
            }

            if port.tag_enum().is_none() {
                debug!(
                    tag = port.tag,
                    port = port.port.get(),
                    "unknown port tag in name record"
                );
            }
        }

        if self.ports.tcp_port().is_none() {
            return Err(alloy_rlp::Error::Custom("Missing TCP port"));
        }
        if self.ports.authenticated_udp_port().is_none() {
            return Err(alloy_rlp::Error::Custom("Missing Authenticated UDP port"));
        }

        Ok(())
    }

    pub fn new(
        ip: Ipv4Addr,
        tcp_port: u16,
        udp_port: Option<u16>,
        authenticated_udp_port: u16,
        capabilities: u64,
        seq: u64,
    ) -> Self {
        let mut record =
            Self::new_with_ports(ip, tcp_port, udp_port, authenticated_udp_port, None, seq);
        record.capabilities = capabilities;
        record
    }

    pub fn new_with_ports(
        ip: Ipv4Addr,
        tcp_port: u16,
        udp_port: Option<u16>,
        authenticated_udp_port: u16,
        direct_udp_port: Option<u16>,
        seq: u64,
    ) -> Self {
        let mut ports_vec = ArrayVec::new();
        ports_vec.push(Port::new(PortTag::TCP, tcp_port));
        if let Some(udp_port) = udp_port {
            ports_vec.push(Port::new(PortTag::UDP, udp_port));
        }
        ports_vec.push(Port::new(PortTag::AuthenticatedUDP, authenticated_udp_port));
        if let Some(direct_udp_port) = direct_udp_port {
            ports_vec.push(Port::new(PortTag::DirectUDP, direct_udp_port));
        }
        Self {
            ip,
            ports: PortList(ports_vec),
            capabilities: 0,
            seq,
        }
    }

    pub fn ip(&self) -> Ipv4Addr {
        self.ip
    }

    pub fn capabilities(&self) -> u64 {
        self.capabilities
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn tcp_port(&self) -> u16 {
        self.ports
            .tcp_port()
            .expect("name record must have TCP port")
            .get()
    }

    pub fn tcp_socket(&self) -> SocketAddrV4 {
        SocketAddrV4::new(self.ip(), self.tcp_port())
    }

    pub fn udp_port(&self) -> Option<u16> {
        self.ports.udp_port().map(NonZeroU16::get)
    }

    pub fn udp_socket(&self) -> Option<SocketAddrV4> {
        self.udp_port()
            .map(|udp_port| SocketAddrV4::new(self.ip(), udp_port))
    }

    pub fn authenticated_udp_port(&self) -> u16 {
        self.ports
            .authenticated_udp_port()
            .expect("name record must have authenticated UDP port")
            .get()
    }

    pub fn authenticated_udp_socket(&self) -> SocketAddrV4 {
        SocketAddrV4::new(self.ip(), self.authenticated_udp_port())
    }

    pub fn direct_udp_port(&self) -> Option<u16> {
        self.ports.direct_udp_port().map(NonZeroU16::get)
    }

    pub fn direct_udp_socket(&self) -> Option<SocketAddrV4> {
        self.direct_udp_port()
            .map(|port| SocketAddrV4::new(self.ip(), port))
    }

    pub fn check_capability(&self, capability: Capability) -> bool {
        (self.capabilities() & (1u64 << (capability as u8))) != 0
    }

    pub fn set_capability(&mut self, capability: Capability) {
        self.capabilities |= 1u64 << (capability as u8);
    }
}

impl Encodable for NameRecord {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let enc: [&dyn Encodable; 4] = [
            &self.ip.octets(),
            &self.ports,
            &self.capabilities,
            &self.seq,
        ];
        encode_list::<_, dyn Encodable>(&enc, out);
    }
}

impl Decodable for NameRecord {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let buf = &mut alloy_rlp::Header::decode_bytes(buf, true)?;

        let Ok(ip_bytes) = <[u8; 4]>::decode(buf) else {
            return Err(alloy_rlp::Error::Custom("Invalid IPv4 address"));
        };
        let record = Self {
            ip: Ipv4Addr::from(ip_bytes),
            ports: PortList::decode(buf)?,
            capabilities: u64::decode(buf)?,
            seq: u64::decode(buf)?,
        };

        if !buf.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }

        record.validate()?;
        Ok(record)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable, Eq)]
pub struct MonadNameRecord<ST: CertificateSignatureRecoverable> {
    pub name_record: NameRecord,
    pub signature: ST,
}

impl<ST: CertificateSignatureRecoverable> MonadNameRecord<ST> {
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

    pub fn udp_address(&self) -> Option<SocketAddrV4> {
        self.name_record.udp_socket()
    }

    pub fn authenticated_udp_address(&self) -> SocketAddrV4 {
        self.name_record.authenticated_udp_socket()
    }

    pub fn direct_udp_address(&self) -> Option<SocketAddrV4> {
        self.name_record.direct_udp_socket()
    }

    pub fn all_udp_sockets(&self) -> impl Iterator<Item = SocketAddrV4> + '_ {
        [
            self.udp_address(),
            Some(self.authenticated_udp_address()),
            self.direct_udp_address(),
        ]
        .into_iter()
        .flatten()
    }

    pub fn seq(&self) -> u64 {
        self.name_record.seq()
    }

    pub fn with_pubkey(
        &self,
        pubkey: CertificateSignaturePubKey<ST>,
    ) -> MonadNameRecordWithPubkey<'_, ST> {
        MonadNameRecordWithPubkey {
            record: self,
            pubkey,
        }
    }
}

#[derive(Debug)]
pub enum PeerEntryConversionError<E> {
    InvalidAddress,
    InvalidSignature(E),
}

impl<ST: CertificateSignatureRecoverable> TryFrom<&PeerEntry<ST>> for MonadNameRecord<ST> {
    type Error = PeerEntryConversionError<<ST as CertificateSignature>::Error>;

    fn try_from(peer: &PeerEntry<ST>) -> Result<Self, Self::Error> {
        let name_record = NameRecord::new_with_ports(
            peer.address,
            peer.tcp_port.get(),
            peer.udp_port.map(NonZeroU16::get),
            peer.auth_port.get(),
            peer.direct_udp_port.map(NonZeroU16::get),
            peer.record_seq_num,
        );

        let mut encoded = Vec::new();
        name_record.encode(&mut encoded);
        peer.signature
            .verify::<signing_domain::NameRecord>(&encoded, &peer.pubkey)
            .map_err(PeerEntryConversionError::InvalidSignature)?;

        Ok(MonadNameRecord {
            name_record,
            signature: peer.signature,
        })
    }
}

impl<ST: CertificateSignatureRecoverable> TryFrom<&MonadNameRecord<ST>> for PeerEntry<ST> {
    type Error = <ST as CertificateSignature>::Error;

    fn try_from(record: &MonadNameRecord<ST>) -> Result<Self, Self::Error> {
        let pubkey = record.recover_pubkey()?.pubkey();

        Ok(PeerEntry {
            pubkey,
            address: record.name_record.ip(),
            tcp_port: NonZeroU16::new(record.name_record.tcp_port())
                .expect("name record TCP port must be non-zero"),
            udp_port: record
                .name_record
                .udp_port()
                .map(|port| NonZeroU16::new(port).expect("name record UDP port must be non-zero")),
            signature: record.signature,
            record_seq_num: record.name_record.seq(),
            auth_port: NonZeroU16::new(record.name_record.authenticated_udp_port())
                .expect("name record authenticated UDP port must be non-zero"),
            direct_udp_port: record.name_record.direct_udp_port().map(|port| {
                NonZeroU16::new(port).expect("name record direct UDP port must be non-zero")
            }),
        })
    }
}

impl<ST: CertificateSignatureRecoverable> From<MonadNameRecordWithPubkey<'_, ST>>
    for NodeBootstrapPeerConfig<ST>
{
    fn from(record_with_pubkey: MonadNameRecordWithPubkey<'_, ST>) -> Self {
        PeerEntry::from(record_with_pubkey).into()
    }
}

#[derive(Debug)]
pub enum PeerConfigConversionError {
    InvalidSignature,
    InvalidAddress(String),
}

impl<ST: CertificateSignatureRecoverable> TryFrom<&NodeBootstrapPeerConfig<ST>>
    for MonadNameRecord<ST>
{
    type Error = PeerConfigConversionError;

    fn try_from(peer_config: &NodeBootstrapPeerConfig<ST>) -> Result<Self, Self::Error> {
        let invalid_address =
            || PeerConfigConversionError::InvalidAddress(peer_config.address.clone());
        let peer_entry = PeerEntry {
            pubkey: peer_config.secp256k1_pubkey,
            address: peer_config
                .address
                .parse::<Ipv4Addr>()
                .map_err(|_| invalid_address())?,
            tcp_port: peer_config.tcp_port,
            udp_port: peer_config.udp_port,
            signature: peer_config.name_record_sig,
            record_seq_num: peer_config.record_seq_num,
            auth_port: peer_config.auth_port,
            direct_udp_port: peer_config.direct_udp_port,
        };

        MonadNameRecord::try_from(&peer_entry).map_err(|error| match error {
            PeerEntryConversionError::InvalidAddress => invalid_address(),
            PeerEntryConversionError::InvalidSignature(_) => {
                PeerConfigConversionError::InvalidSignature
            }
        })
    }
}

pub struct MonadNameRecordWithPubkey<'a, ST: CertificateSignatureRecoverable> {
    record: &'a MonadNameRecord<ST>,
    pubkey: CertificateSignaturePubKey<ST>,
}

impl<ST: CertificateSignatureRecoverable> From<MonadNameRecordWithPubkey<'_, ST>>
    for PeerEntry<ST>
{
    fn from(record_with_pubkey: MonadNameRecordWithPubkey<'_, ST>) -> Self {
        PeerEntry {
            pubkey: record_with_pubkey.pubkey,
            address: record_with_pubkey.record.name_record.ip(),
            tcp_port: NonZeroU16::new(record_with_pubkey.record.name_record.tcp_port())
                .expect("name record TCP port must be non-zero"),
            udp_port: record_with_pubkey
                .record
                .name_record
                .udp_port()
                .map(|port| NonZeroU16::new(port).expect("name record UDP port must be non-zero")),
            signature: record_with_pubkey.record.signature,
            record_seq_num: record_with_pubkey.record.name_record.seq(),
            auth_port: NonZeroU16::new(
                record_with_pubkey
                    .record
                    .name_record
                    .authenticated_udp_port(),
            )
            .expect("name record authenticated UDP port must be non-zero"),
            direct_udp_port: record_with_pubkey
                .record
                .name_record
                .direct_udp_port()
                .map(|port| {
                    NonZeroU16::new(port).expect("name record direct UDP port must be non-zero")
                }),
        }
    }
}

#[derive(Debug, Clone)]
pub enum PeerDiscoveryEvent<ST: CertificateSignatureRecoverable> {
    SendPing {
        to: NodeId<CertificateSignaturePubKey<ST>>,
        name_record: NameRecord,
        ping: Ping<ST>,
    },
    PingRequest {
        from: PeerSource<CertificateSignaturePubKey<ST>>,
        ping: Ping<ST>,
    },
    PongResponse {
        from: PeerSource<CertificateSignaturePubKey<ST>>,
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
        from: PeerSource<CertificateSignaturePubKey<ST>>,
        request: PeerLookupRequest<ST>,
    },
    PeerLookupResponse {
        from: PeerSource<CertificateSignaturePubKey<ST>>,
        response: PeerLookupResponse<ST>,
    },
    PeerLookupTimeout {
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        lookup_id: u32,
    },
    SendFullNodeRaptorcastRequest {
        to: NodeId<CertificateSignaturePubKey<ST>>,
    },
    FullNodeRaptorcastRequest {
        from: PeerSource<CertificateSignaturePubKey<ST>>,
    },
    FullNodeRaptorcastResponse {
        from: PeerSource<CertificateSignaturePubKey<ST>>,
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
    UpdatePinnedNodes {
        dedicated_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
        prioritized_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
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
    FullNodeRaptorcastRequest,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct PeerDiscoveryMetricsCommand(ExecutorMetrics);

#[derive(Debug, Clone)]
pub enum PeerDiscoveryCommand<ST: CertificateSignatureRecoverable> {
    RouterCommand {
        target: NodeId<CertificateSignaturePubKey<ST>>,
        message: PeerDiscoveryMessage<ST>,
    },
    PingPongCommand {
        target: NodeId<CertificateSignaturePubKey<ST>>,
        name_record: NameRecord,
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
        name_record: NameRecord,
        ping: Ping<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_ping(
        &mut self,
        from: PeerSource<CertificateSignaturePubKey<Self::SignatureType>>,
        ping: Ping<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_pong(
        &mut self,
        from: PeerSource<CertificateSignaturePubKey<Self::SignatureType>>,
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
        from: PeerSource<CertificateSignaturePubKey<Self::SignatureType>>,
        request: PeerLookupRequest<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_peer_lookup_response(
        &mut self,
        from: PeerSource<CertificateSignaturePubKey<Self::SignatureType>>,
        response: PeerLookupResponse<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_peer_lookup_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        target: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        lookup_id: u32,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn send_full_node_raptorcast_request(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_full_node_raptorcast_request(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_full_node_raptorcast_response(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
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

    fn update_pinned_nodes(
        &mut self,
        dedicated_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<Self::SignatureType>>>,
        prioritized_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<Self::SignatureType>>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn update_peer_participation(
        &mut self,
        round: Round,
        peers: BTreeSet<NodeId<CertificateSignaturePubKey<Self::SignatureType>>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn metrics(&self) -> &ExecutorMetrics;

    fn get_pending_addr_by_id(
        &self,
        id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<SocketAddrV4>;

    fn get_addr_by_id(
        &self,
        id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<SocketAddrV4>;

    fn get_known_addrs(
        &self,
    ) -> HashMap<NodeId<CertificateSignaturePubKey<Self::SignatureType>>, SocketAddrV4>;

    fn get_secondary_fullnodes(
        &self,
    ) -> Vec<NodeId<CertificateSignaturePubKey<Self::SignatureType>>>;

    fn get_name_records(
        &self,
    ) -> HashMap<
        NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        MonadNameRecord<Self::SignatureType>,
    >;

    fn get_name_record(
        &self,
        id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<&MonadNameRecord<Self::SignatureType>>;
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
    use std::{num::NonZeroU16, str::FromStr};

    use monad_secp::{KeyPair, SecpSignature};
    use rstest::*;

    use super::*;

    fn nz(port: u16) -> NonZeroU16 {
        NonZeroU16::new(port).unwrap()
    }

    #[test]
    fn test_name_record_v4_rlp() {
        let name_record = NameRecord::new(
            Ipv4Addr::from_str("1.1.1.1").unwrap(),
            8000,
            Some(8000),
            8000,
            0,
            2,
        );

        let mut encoded = Vec::new();
        name_record.encode(&mut encoded);

        let result = NameRecord::decode(&mut encoded.as_slice());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), name_record);
    }

    #[test]
    fn test_name_record_duplicate_port() {
        let mut ports_vec = ArrayVec::new();
        ports_vec.push(Port::new(PortTag::TCP, 8000));
        ports_vec.push(Port::new(PortTag::UDP, 8001));
        ports_vec.push(Port::new(PortTag::TCP, 8002));

        let wire = NameRecord {
            ip: Ipv4Addr::from_str("1.1.1.1").unwrap(),
            ports: PortList(ports_vec),
            capabilities: 0,
            seq: 1,
        };

        let mut encoded = Vec::new();
        wire.encode(&mut encoded);

        let decoded = NameRecord::decode(&mut encoded.as_slice());
        assert!(decoded.is_err());
    }

    #[test]
    fn test_name_record_missing_port() {
        let mut ports_vec = ArrayVec::new();
        ports_vec.push(Port::new(PortTag::TCP, 8000));

        let wire = NameRecord {
            ip: Ipv4Addr::from_str("1.1.1.1").unwrap(),
            ports: PortList(ports_vec),
            capabilities: 0,
            seq: 1,
        };

        let mut encoded = Vec::new();
        wire.encode(&mut encoded);

        let decoded = NameRecord::decode(&mut encoded.as_slice());
        assert!(decoded.is_err());
    }

    #[test]
    fn test_name_record_with_unknown_ports_and_capabilities() {
        let mut ports_vec = ArrayVec::new();
        ports_vec.push(Port::new(PortTag::TCP, 9000));
        ports_vec.push(Port::new(PortTag::UDP, 9001));
        ports_vec.push(Port {
            tag: 2,
            port: nz(9002),
        });
        ports_vec.push(Port {
            tag: 5,
            port: nz(9005),
        });

        let wire = NameRecord {
            ip: Ipv4Addr::from_str("10.0.0.1").unwrap(),
            ports: PortList(ports_vec),
            capabilities: 7,
            seq: 100,
        };

        let mut wire_encoded = Vec::new();
        wire.encode(&mut wire_encoded);

        let decoded = NameRecord::decode(&mut wire_encoded.as_slice()).unwrap();

        assert_eq!(decoded.ip(), Ipv4Addr::from_str("10.0.0.1").unwrap());
        assert_eq!(decoded.tcp_port(), 9000);
        assert_eq!(decoded.udp_port(), Some(9001));
        assert_eq!(decoded.capabilities(), 7);
        assert_eq!(decoded.seq(), 100);

        let mut reencoded = Vec::new();
        decoded.encode(&mut reencoded);
        assert_eq!(wire_encoded, reencoded);

        let keypair = KeyPair::from_ikm(b"test keypair for signature veri").unwrap();
        let signature = SecpSignature::sign::<signing_domain::NameRecord>(&wire_encoded, &keypair);

        let signed_record = MonadNameRecord::<SecpSignature> {
            name_record: decoded,
            signature,
        };

        let recovered_node_id = signed_record.recover_pubkey().unwrap();
        let expected_node_id = NodeId::new(keypair.pubkey());

        assert_eq!(recovered_node_id, expected_node_id);
    }

    #[test]
    fn test_name_record_roundtrip() {
        let ip = Ipv4Addr::from_str("192.168.50.100").unwrap();
        let tcp_port = 9000u16;
        let udp_port = 9001u16;
        let authenticated_udp_port = 9002u16;
        let capabilities = 15u64;
        let seq = 42u64;

        let record = NameRecord::new(
            ip,
            tcp_port,
            Some(udp_port),
            authenticated_udp_port,
            capabilities,
            seq,
        );
        let mut encoded = Vec::new();
        record.encode(&mut encoded);

        insta::assert_debug_snapshot!("name_record_encoded", hex::encode(&encoded));

        let decoded = NameRecord::decode(&mut encoded.as_slice()).unwrap();

        assert_eq!(decoded.ip(), ip);
        assert_eq!(decoded.tcp_port(), tcp_port);
        assert_eq!(decoded.udp_port(), Some(udp_port));
        assert_eq!(decoded.authenticated_udp_port(), authenticated_udp_port);
        assert_eq!(decoded.capabilities(), capabilities);
        assert_eq!(decoded.seq(), seq);

        let mut reencoded = Vec::new();
        decoded.encode(&mut reencoded);
        assert_eq!(encoded, reencoded);

        let keypair = KeyPair::from_ikm(b"test name record roundtrip").unwrap();
        let signature = SecpSignature::sign::<signing_domain::NameRecord>(&encoded, &keypair);

        let signed_record = MonadNameRecord::<SecpSignature> {
            name_record: decoded.clone(),
            signature,
        };

        let recovered_node_id = signed_record.recover_pubkey().unwrap();
        let expected_node_id = NodeId::new(keypair.pubkey());
        assert_eq!(recovered_node_id, expected_node_id);

        let mut signed_encoded = Vec::new();
        signed_record.encode(&mut signed_encoded);

        let decoded_signed =
            MonadNameRecord::<SecpSignature>::decode(&mut signed_encoded.as_slice()).unwrap();
        assert_eq!(decoded_signed.name_record.ip(), ip);
        assert_eq!(decoded_signed.name_record.tcp_port(), tcp_port);
        assert_eq!(decoded_signed.name_record.udp_port(), Some(udp_port));
        assert_eq!(
            decoded_signed.name_record.authenticated_udp_port(),
            authenticated_udp_port
        );
        assert_eq!(decoded_signed.name_record.capabilities(), capabilities);
        assert_eq!(decoded_signed.name_record.seq(), seq);

        let recovered_from_decoded = decoded_signed.recover_pubkey().unwrap();
        assert_eq!(recovered_from_decoded, expected_node_id);
    }

    #[test]
    fn test_name_record_with_direct_udp_roundtrip() {
        let ip = Ipv4Addr::from_str("10.0.0.43").unwrap();
        let tcp_port = 9100u16;
        let udp_port = 9101u16;
        let authenticated_udp_port = 9102u16;
        let direct_udp_port = 9103u16;
        let seq = 101u64;

        let record = NameRecord::new_with_ports(
            ip,
            tcp_port,
            Some(udp_port),
            authenticated_udp_port,
            Some(direct_udp_port),
            seq,
        );

        assert_eq!(record.ip(), ip);
        assert_eq!(record.tcp_port(), tcp_port);
        assert_eq!(record.udp_port(), Some(udp_port));
        assert_eq!(record.authenticated_udp_port(), authenticated_udp_port);
        assert_eq!(record.direct_udp_port(), Some(direct_udp_port));
        assert_eq!(
            record.direct_udp_socket(),
            Some(SocketAddrV4::from_str("10.0.0.43:9103").unwrap())
        );
        assert_eq!(record.seq(), seq);

        let mut encoded = Vec::new();
        record.encode(&mut encoded);

        let decoded = NameRecord::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded.ip(), ip);
        assert_eq!(decoded.tcp_port(), tcp_port);
        assert_eq!(decoded.udp_port(), Some(udp_port));
        assert_eq!(decoded.authenticated_udp_port(), authenticated_udp_port);
        assert_eq!(decoded.direct_udp_port(), Some(direct_udp_port));
        assert_eq!(decoded.seq(), seq);

        let mut reencoded = Vec::new();
        decoded.encode(&mut reencoded);
        assert_eq!(encoded, reencoded);
    }

    #[rstest]
    #[case::auth_and_direct(
        NameRecord::new_with_ports(
            Ipv4Addr::new(10, 0, 0, 44),
            9200,
            Some(9201),
            9202,
            Some(9203),
            102,
        ),
        vec![
            SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 44), 9201),
            SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 44), 9202),
            SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 44), 9203),
        ],
    )]
    #[case::auth_only(
        NameRecord::new(
            Ipv4Addr::new(10, 0, 0, 46),
            9400,
            Some(9401),
            9402,
            0,
            104,
        ),
        vec![
            SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 46), 9401),
            SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 46), 9402),
        ],
    )]
    #[case::shared_udp_and_auth(
        NameRecord::new(
            Ipv4Addr::new(10, 0, 0, 47),
            9501,
            Some(9501),
            9501,
            0,
            105,
        ),
        vec![
            SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 47), 9501),
            SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 47), 9501),
        ],
    )]
    fn test_monad_name_record_all_udp_sockets(
        #[case] record: NameRecord,
        #[case] expected_sockets: Vec<SocketAddrV4>,
    ) {
        let keypair = KeyPair::from_ikm(b"test all sockets").unwrap();
        let signed_record = MonadNameRecord::<SecpSignature>::new(record, &keypair);
        let sockets = signed_record.all_udp_sockets().collect::<Vec<_>>();
        assert_eq!(sockets, expected_sockets);
    }

    #[test]
    fn test_peer_entry_to_monad_name_record_invalid_signature() {
        let ip = Ipv4Addr::from_str("192.168.1.200").unwrap();
        let port = 9000u16;
        let seq = 10u64;

        let keypair = KeyPair::from_ikm(b"test invalid sig").unwrap();
        let other_keypair = KeyPair::from_ikm(b"other key").unwrap();

        let name_record = NameRecord::new(ip, port, Some(port), port, 0, seq);
        let mut encoded = Vec::new();
        name_record.encode(&mut encoded);
        let wrong_signature =
            SecpSignature::sign::<signing_domain::NameRecord>(&encoded, &other_keypair);

        let peer_entry = PeerEntry {
            pubkey: keypair.pubkey(),
            address: ip,
            tcp_port: NonZeroU16::new(port).unwrap(),
            udp_port: Some(NonZeroU16::new(port).unwrap()),
            signature: wrong_signature,
            record_seq_num: seq,
            auth_port: NonZeroU16::new(port).unwrap(),
            direct_udp_port: None,
        };

        let result = MonadNameRecord::<SecpSignature>::try_from(&peer_entry);
        assert!(result.is_err());
    }

    #[test]
    fn test_peer_entry_monad_name_record_roundtrip_with_auth() {
        let ip = Ipv4Addr::from_str("172.31.0.100").unwrap();
        let port = 8888u16;
        let auth_port = 8889u16;
        let seq = 99u64;

        let keypair = KeyPair::from_ikm(b"test roundtrip auth").unwrap();
        let name_record = NameRecord::new(ip, port, Some(port), auth_port, 0, seq);
        let original_monad_record = MonadNameRecord::<SecpSignature>::new(name_record, &keypair);

        let pubkey = original_monad_record.recover_pubkey().unwrap().pubkey();
        let peer_entry = PeerEntry::from(original_monad_record.with_pubkey(pubkey));
        assert_eq!(peer_entry.auth_port.get(), auth_port);

        let converted_monad_record =
            MonadNameRecord::<SecpSignature>::try_from(&peer_entry).unwrap();

        assert_eq!(
            converted_monad_record.name_record,
            original_monad_record.name_record
        );
        assert_eq!(
            converted_monad_record.signature,
            original_monad_record.signature
        );
    }

    #[test]
    fn test_peer_entry_monad_name_record_roundtrip_with_direct_udp() {
        let ip = Ipv4Addr::from_str("172.31.0.101").unwrap();
        let port = 8890u16;
        let auth_port = 8891u16;
        let direct_udp_port = 8892u16;
        let seq = 100u64;

        let keypair = KeyPair::from_ikm(b"test roundtrip direct udp").unwrap();
        let name_record =
            NameRecord::new_with_ports(ip, port, Some(port), auth_port, Some(direct_udp_port), seq);
        let original_monad_record = MonadNameRecord::<SecpSignature>::new(name_record, &keypair);

        let pubkey = original_monad_record.recover_pubkey().unwrap().pubkey();
        let peer_entry = PeerEntry::from(original_monad_record.with_pubkey(pubkey));
        assert_eq!(peer_entry.auth_port.get(), auth_port);
        assert_eq!(
            peer_entry.direct_udp_port.map(NonZeroU16::get),
            Some(direct_udp_port)
        );

        let converted_monad_record =
            MonadNameRecord::<SecpSignature>::try_from(&peer_entry).unwrap();

        assert_eq!(
            converted_monad_record.name_record,
            original_monad_record.name_record
        );
        assert_eq!(
            converted_monad_record.signature,
            original_monad_record.signature
        );
        assert_eq!(
            converted_monad_record.name_record.authenticated_udp_port(),
            auth_port
        );
        assert_eq!(
            converted_monad_record.name_record.direct_udp_port(),
            Some(direct_udp_port)
        );
    }

    #[test]
    fn test_bootstrap_peer_config_rejects_zero_auth_port() {
        let ip = Ipv4Addr::from_str("172.31.0.102").unwrap();
        let port = 8893u16;
        let auth_port = 8894u16;
        let seq = 101u64;

        let keypair = KeyPair::from_ikm(b"test bootstrap zero auth port").unwrap();
        let record = MonadNameRecord::<SecpSignature>::new(
            NameRecord::new(ip, port, Some(port), auth_port, 0, seq),
            &keypair,
        );
        let pubkey = record.recover_pubkey().unwrap().pubkey();
        let peer_config = NodeBootstrapPeerConfig::from(record.with_pubkey(pubkey));
        let mut serialized = toml::to_string(&monad_node_config::NodeBootstrapConfig {
            peers: vec![peer_config],
        })
        .unwrap();
        serialized = serialized.replacen(&format!("auth_port = {}", auth_port), "auth_port = 0", 1);

        let parsed =
            toml::from_str::<monad_node_config::NodeBootstrapConfig<SecpSignature>>(&serialized);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_name_record_decode_rejects_zero_port() {
        let zero_port = {
            let enc: [&dyn Encodable; 2] = [&(PortTag::TCP as u8), &0u16];
            let mut encoded = Vec::new();
            encode_list::<_, dyn Encodable>(&enc, &mut encoded);
            encoded
        };
        let udp_port = alloy_rlp::encode(Port::new(PortTag::UDP, 9001));
        let authenticated_udp_port = alloy_rlp::encode(Port::new(PortTag::AuthenticatedUDP, 9002));
        let ports: Vec<u8> = [
            zero_port.as_slice(),
            udp_port.as_slice(),
            authenticated_udp_port.as_slice(),
        ]
        .concat();
        let ports = [&[0xc0 + ports.len() as u8][..], &ports].concat();
        let enc: [&dyn Encodable; 4] = [&[1, 1, 1, 1], &ports, &0u64, &1u64];
        let mut encoded = Vec::new();
        encode_list::<_, dyn Encodable>(&enc, &mut encoded);

        let decoded = NameRecord::decode(&mut encoded.as_slice());
        assert!(decoded.is_err());
    }

    #[test]
    #[should_panic(expected = "name record port must be non-zero")]
    fn test_name_record_new_rejects_zero_port() {
        let _ = NameRecord::new(Ipv4Addr::new(1, 1, 1, 1), 0, Some(9001), 9002, 0, 1);
    }
}
