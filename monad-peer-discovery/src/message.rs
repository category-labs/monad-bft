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

use std::net::SocketAddr;

use alloy_rlp::{Decodable, Encodable, Header, RlpDecodable, RlpEncodable, encode_list};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::Message;
use monad_types::NodeId;

use crate::{MonadNameRecord, PeerDiscoveryEvent, PeerSource};

const PEER_DISCOVERY_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub enum PeerDiscoveryMessage<ST: CertificateSignatureRecoverable> {
    Ping(Ping<ST>),
    Pong(Pong),
    PeerLookupRequest(PeerLookupRequest<ST>),
    PeerLookupResponse(PeerLookupResponse<ST>),
    FullNodeRaptorcastRequest,
    FullNodeRaptorcastResponse,
}

impl<ST: CertificateSignatureRecoverable> Message for PeerDiscoveryMessage<ST> {
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    type Event = PeerDiscoveryEvent<ST>;

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        self.event_with_source(
            from,
            SocketAddr::V4(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::UNSPECIFIED,
                0,
            )),
        )
    }

    fn event_with_source(
        self,
        from: NodeId<Self::NodeIdPubKey>,
        src_addr: SocketAddr,
    ) -> Self::Event {
        let from = PeerSource {
            id: from,
            addr: src_addr,
        };
        match self {
            PeerDiscoveryMessage::Ping(ping) => PeerDiscoveryEvent::PingRequest { from, ping },
            PeerDiscoveryMessage::Pong(pong) => PeerDiscoveryEvent::PongResponse { from, pong },
            PeerDiscoveryMessage::PeerLookupRequest(request) => {
                PeerDiscoveryEvent::PeerLookupRequest { from, request }
            }
            PeerDiscoveryMessage::PeerLookupResponse(response) => {
                PeerDiscoveryEvent::PeerLookupResponse { from, response }
            }
            PeerDiscoveryMessage::FullNodeRaptorcastRequest => {
                PeerDiscoveryEvent::FullNodeRaptorcastRequest { from }
            }
            PeerDiscoveryMessage::FullNodeRaptorcastResponse => {
                PeerDiscoveryEvent::FullNodeRaptorcastResponse { from }
            }
        }
    }
}

impl<ST: CertificateSignatureRecoverable> Encodable for PeerDiscoveryMessage<ST> {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let version = PEER_DISCOVERY_VERSION;

        match self {
            PeerDiscoveryMessage::Ping(ping) => {
                let enc: [&dyn Encodable; 3] = [&version, &1_u8, ping];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            PeerDiscoveryMessage::Pong(pong) => {
                let enc: [&dyn Encodable; 3] = [&version, &2_u8, pong];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            PeerDiscoveryMessage::PeerLookupRequest(peer_lookup_request) => {
                let enc: [&dyn Encodable; 3] = [&version, &3_u8, peer_lookup_request];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            PeerDiscoveryMessage::PeerLookupResponse(peer_lookup_response) => {
                let enc: [&dyn Encodable; 3] = [&version, &4_u8, peer_lookup_response];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            PeerDiscoveryMessage::FullNodeRaptorcastRequest => {
                let enc: [&dyn Encodable; 2] = [&version, &5_u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            PeerDiscoveryMessage::FullNodeRaptorcastResponse => {
                let enc: [&dyn Encodable; 2] = [&version, &6_u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST: CertificateSignatureRecoverable> Decodable for PeerDiscoveryMessage<ST> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let _peer_discovery_version = u16::decode(&mut payload)?;

        match u8::decode(&mut payload)? {
            1 => Ok(Self::Ping(Ping::decode(&mut payload)?)),
            2 => Ok(Self::Pong(Pong::decode(&mut payload)?)),
            3 => Ok(Self::PeerLookupRequest(PeerLookupRequest::decode(
                &mut payload,
            )?)),
            4 => Ok(Self::PeerLookupResponse(PeerLookupResponse::decode(
                &mut payload,
            )?)),
            5 => Ok(Self::FullNodeRaptorcastRequest),
            6 => Ok(Self::FullNodeRaptorcastResponse),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown PeerDiscoveryMessage",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, RlpDecodable, RlpEncodable)]
#[rlp(trailing)]
pub struct Ping<ST: CertificateSignatureRecoverable> {
    pub id: u32,
    pub local_name_record: MonadNameRecord<ST>,
}

impl<ST: CertificateSignatureRecoverable> Eq for Ping<ST> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpDecodable, RlpEncodable)]
pub struct Pong {
    pub ping_id: u32,
    pub local_record_seq: u64,
}

#[derive(Debug, Clone, RlpDecodable, RlpEncodable)]
pub struct PeerLookupRequest<ST: CertificateSignatureRecoverable> {
    pub lookup_id: u32,
    pub target: NodeId<CertificateSignaturePubKey<ST>>,
    pub open_discovery: bool,
}

#[derive(Debug, Clone, RlpDecodable, RlpEncodable)]
pub struct PeerLookupResponse<ST: CertificateSignatureRecoverable> {
    pub lookup_id: u32,
    pub target: NodeId<CertificateSignaturePubKey<ST>>,
    pub name_records: Vec<MonadNameRecord<ST>>,
}

#[cfg(test)]
mod test {
    use std::{net::SocketAddrV4, str::FromStr};

    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;

    use super::*;
    use crate::NameRecord;

    type SignatureType = SecpSignature;

    #[test]
    fn test_ping_rlp_roundtrip() {
        let key = get_key::<SignatureType>(37);
        let ping = Ping {
            id: 257,
            local_name_record: MonadNameRecord::<SignatureType>::new(
                NameRecord::new(
                    *SocketAddrV4::from_str("127.0.0.1:8000").unwrap().ip(),
                    8000,
                    2,
                ),
                &key,
            ),
        };

        let mut encoded = Vec::new();
        ping.encode(&mut encoded);

        let decoded = Ping::<SignatureType>::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ping, decoded);
    }

    #[test]
    fn test_peer_discovery_message_ping_encoding() {
        let key = get_key::<SignatureType>(37);
        let ping = Ping {
            id: 257,
            local_name_record: MonadNameRecord::<SignatureType>::new(
                NameRecord::new(
                    *SocketAddrV4::from_str("127.0.0.1:8000").unwrap().ip(),
                    8000,
                    2,
                ),
                &key,
            ),
        };
        let message = PeerDiscoveryMessage::Ping(ping);

        let mut encoded = Vec::new();
        message.encode(&mut encoded);
        insta::assert_debug_snapshot!(hex::encode(encoded));
    }

    #[test]
    fn test_peer_discovery_message_pong_encoding() {
        let pong = Pong {
            ping_id: 123,
            local_record_seq: 456,
        };
        let message = PeerDiscoveryMessage::<SignatureType>::Pong(pong);

        let mut encoded = Vec::new();
        message.encode(&mut encoded);
        insta::assert_debug_snapshot!(hex::encode(encoded));
    }

    #[test]
    fn test_peer_discovery_message_peer_lookup_request_encoding() {
        let key = get_key::<SignatureType>(42);
        let request = PeerLookupRequest::<SignatureType> {
            lookup_id: 789,
            target: NodeId::new(key.pubkey()),
            open_discovery: true,
        };
        let message = PeerDiscoveryMessage::PeerLookupRequest(request);

        let mut encoded = Vec::new();
        message.encode(&mut encoded);
        insta::assert_debug_snapshot!(hex::encode(encoded));
    }

    #[test]
    fn test_peer_discovery_message_peer_lookup_response_encoding() {
        let key1 = get_key::<SignatureType>(37);
        let key2 = get_key::<SignatureType>(42);
        let key3 = get_key::<SignatureType>(55);

        let target_key = get_key::<SignatureType>(100);

        let response = PeerLookupResponse {
            lookup_id: 999,
            target: NodeId::new(target_key.pubkey()),
            name_records: vec![
                MonadNameRecord::<SignatureType>::new(
                    NameRecord::new(
                        *SocketAddrV4::from_str("192.168.1.1:8000").unwrap().ip(),
                        8000,
                        1,
                    ),
                    &key1,
                ),
                MonadNameRecord::<SignatureType>::new(
                    NameRecord::new(
                        *SocketAddrV4::from_str("192.168.1.2:8001").unwrap().ip(),
                        8001,
                        2,
                    ),
                    &key2,
                ),
                MonadNameRecord::<SignatureType>::new(
                    NameRecord::new(
                        *SocketAddrV4::from_str("192.168.1.3:8002").unwrap().ip(),
                        8002,
                        3,
                    ),
                    &key3,
                ),
            ],
        };
        let message = PeerDiscoveryMessage::PeerLookupResponse(response);

        let mut encoded = Vec::new();
        message.encode(&mut encoded);
        insta::assert_debug_snapshot!(hex::encode(encoded));
    }

    #[test]
    fn test_peer_discovery_message_full_node_raptorcast_request_encoding() {
        let message = PeerDiscoveryMessage::<SignatureType>::FullNodeRaptorcastRequest;

        let mut encoded = Vec::new();
        message.encode(&mut encoded);
        insta::assert_debug_snapshot!(hex::encode(encoded));
    }

    #[test]
    fn test_peer_discovery_message_full_node_raptorcast_response_encoding() {
        let message = PeerDiscoveryMessage::<SignatureType>::FullNodeRaptorcastResponse;

        let mut encoded = Vec::new();
        message.encode(&mut encoded);
        insta::assert_debug_snapshot!(hex::encode(encoded));
    }
}
