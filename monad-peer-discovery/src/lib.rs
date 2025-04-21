use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4},
    time::Duration,
};

use alloy_rlp::{Decodable, Encodable, encode_list};
use monad_crypto::certificate_signature::{
    CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::Message;
use monad_types::NodeId;
use tracing::warn;

pub mod discovery;
pub mod mock;

pub type PeerDiscMetrics = HashMap<&'static str, u64>;

#[derive(Debug, Clone, Copy)]
pub struct Ping {
    id: u32,
    local_record_seq: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct Pong {
    ping_id: u32,
    local_record_seq: u64,
}

#[derive(Debug, Clone)]
pub struct PeerLookupRequest<ST: CertificateSignatureRecoverable> {
    lookup_id: u32,
    target: NodeId<CertificateSignaturePubKey<ST>>,
}

#[derive(Debug, Clone)]
pub struct PeerLookupResponse<ST: CertificateSignatureRecoverable> {
    lookup_id: u32,
    target: NodeId<CertificateSignaturePubKey<ST>>,
    name_records: Vec<MonadNameRecord<ST>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NameRecord {
    pub address: SocketAddrV4,
    pub seq: u64,
}

impl Encodable for NameRecord {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let enc: [&dyn Encodable; 3] =
            [&self.address.ip().octets(), &self.address.port(), &self.seq];
        encode_list::<_, dyn Encodable>(&enc, out);
    }
}

impl Decodable for NameRecord {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let buf = &mut alloy_rlp::Header::decode_bytes(buf, true)?;

        let Ok(ip) = <[u8; 4]>::decode(buf) else {
            warn!("ip address decode failed: {:?}", buf);
            return Err(alloy_rlp::Error::Custom("Invalid IPv4 address"));
        };
        let port = u16::decode(buf)?;
        let addr = SocketAddrV4::new(Ipv4Addr::from(ip), port);
        let seq = u64::decode(buf)?;

        Ok(Self { address: addr, seq })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonadNameRecord<ST: CertificateSignatureRecoverable> {
    pub name_record: NameRecord,
    pub signature: ST,
}

impl<ST: CertificateSignatureRecoverable> MonadNameRecord<ST> {
    pub fn recover_pubkey(
        &self,
    ) -> Result<NodeId<CertificateSignaturePubKey<ST>>, <ST as CertificateSignature>::Error> {
        let mut encoded = Vec::new();
        self.name_record.encode(&mut encoded);
        let pubkey = self.signature.recover_pubkey(&encoded)?;
        Ok(NodeId::new(pubkey))
    }

    pub fn address(&self) -> SocketAddrV4 {
        self.name_record.address
    }

    pub fn seq(&self) -> u64 {
        self.name_record.seq
    }
}

pub enum PeerDiscoveryEvent<ST: CertificateSignatureRecoverable> {
    SendPing {
        to: NodeId<CertificateSignaturePubKey<ST>>,
    },
    PingRequest {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        ping: Ping,
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
    Prune,
}

#[derive(Debug, Clone)]
pub enum PeerDiscoveryMessage<ST: CertificateSignatureRecoverable> {
    Ping(Ping),
    Pong(Pong),
    PeerLookupRequest(PeerLookupRequest<ST>),
    PeerLookupResponse(PeerLookupResponse<ST>),
}

impl<ST: CertificateSignatureRecoverable> Message for PeerDiscoveryMessage<ST> {
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    type Event = PeerDiscoveryEvent<ST>;

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        match self {
            PeerDiscoveryMessage::Ping(ping) => PeerDiscoveryEvent::PingRequest { from, ping },
            PeerDiscoveryMessage::Pong(pong) => PeerDiscoveryEvent::PongResponse { from, pong },
            PeerDiscoveryMessage::PeerLookupRequest(request) => {
                PeerDiscoveryEvent::PeerLookupRequest { from, request }
            }
            PeerDiscoveryMessage::PeerLookupResponse(response) => {
                PeerDiscoveryEvent::PeerLookupResponse { from, response }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimerKind {
    SendPing,
    PingTimeout,
    RetryPeerLookup { lookup_id: u32 },
    Prune,
}

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

pub enum PeerDiscoveryCommand<ST: CertificateSignatureRecoverable> {
    RouterCommand {
        target: NodeId<CertificateSignaturePubKey<ST>>,
        message: PeerDiscoveryMessage<ST>,
    },
    TimerCommand(PeerDiscoveryTimerCommand<PeerDiscoveryEvent<ST>, ST>),
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
        ping: Ping,
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

    fn prune(&mut self) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn metrics(&self) -> &PeerDiscMetrics;

    fn get_sock_addr_by_id(
        &self,
        id: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<SocketAddrV4>;
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
            address: SocketAddrV4::from_str("1.1.1.1:8000").unwrap(),
            seq: 2,
        };

        let mut encoded = Vec::new();
        name_record.encode(&mut encoded);

        let result = NameRecord::decode(&mut encoded.as_slice());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), name_record);
    }
}
