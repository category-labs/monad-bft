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

use alloy_rlp::{encode_list, Decodable, Encodable, Header, RlpDecodable, RlpEncodable};
use bytes::BufMut;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_peer_discovery::MonadNameRecord;
use monad_types::{BoundedU64, LimitedVec, NodeId, Round, RoundSpan};

/// Maximum number of peers/name records allowed in a secondary raptorcast message.
/// This is to set an upper bound on RLP deserialization memory usage.
const MAX_PEERS_IN_GROUP: usize = 500;

#[derive(RlpEncodable, RlpDecodable, Debug, Eq, PartialEq, Clone)]
pub struct PrepareGroup<PT: PubKey> {
    pub validator_id: NodeId<PT>,
    pub max_group_size: usize,
    pub start_round: Round,
    pub end_round: Round,
}

#[derive(Debug, Clone, RlpEncodable, RlpDecodable, Eq, PartialEq)]
pub struct PrepareGroupResponse<PT: PubKey> {
    pub req: PrepareGroup<PT>,
    pub node_id: NodeId<PT>,
    pub accept: bool,
}

#[derive(Debug, Clone, RlpEncodable, RlpDecodable, Eq, PartialEq)]
pub struct ConfirmGroup<ST: CertificateSignatureRecoverable> {
    pub prepare: PrepareGroup<CertificateSignaturePubKey<ST>>,
    pub peers: LimitedVec<NodeId<CertificateSignaturePubKey<ST>>, MAX_PEERS_IN_GROUP>,
    pub name_records: LimitedVec<MonadNameRecord<ST>, MAX_PEERS_IN_GROUP>,
}

const NO_CONF_REASON_GROUP_FULL: u8 = 1;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum NoConfirmReason {
    GroupFull,
}

impl Encodable for NoConfirmReason {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::GroupFull => {
                let enc: [&dyn Encodable; 1] = [&NO_CONF_REASON_GROUP_FULL];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl Decodable for NoConfirmReason {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let reason = match u8::decode(&mut payload)? {
            NO_CONF_REASON_GROUP_FULL => Self::GroupFull,
            _ => {
                return Err(alloy_rlp::Error::Custom(
                    "Unknown NoConfirmReason enum variant",
                ))
            }
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::Custom("Extra bytes in NoConfirmReason"));
        }
        Ok(reason)
    }
}

#[derive(Debug, Clone, RlpEncodable, RlpDecodable, Eq, PartialEq)]
pub struct NoConfirm<PT: PubKey> {
    pub prepare: PrepareGroup<PT>,
    pub reason: NoConfirmReason,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct PeerParticipation<PT: PubKey> {
    pub peer: NodeId<PT>,
    pub participation_score: BoundedU64<100>,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct PeerParticipationReport<PT: PubKey> {
    pub reporter: NodeId<PT>,
    pub validator_id: NodeId<PT>,
    pub round_span: RoundSpan,
    pub peer_scores: LimitedVec<PeerParticipation<PT>, MAX_PEERS_IN_GROUP>,
}

const GROUP_MSG_VERSION: u8 = 1;

const MESSAGE_TYPE_PREP_REQ: u8 = 1;
const MESSAGE_TYPE_PREP_RES: u8 = 2;
const MESSAGE_TYPE_CONF_GRP: u8 = 3;
const MESSAGE_TYPE_NO_CONF: u8 = 4;
const MESSAGE_TYPE_PARTICIPATION_REPORT: u8 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FullNodesGroupMessage<ST: CertificateSignatureRecoverable> {
    PrepareGroup(PrepareGroup<CertificateSignaturePubKey<ST>>), // MESSAGE_TYPE_PREP_REQ
    PrepareGroupResponse(PrepareGroupResponse<CertificateSignaturePubKey<ST>>), // MESSAGE_TYPE_PREP_RES
    ConfirmGroup(ConfirmGroup<ST>), // MESSAGE_TYPE_CONF_GRP
    NoConfirm(NoConfirm<CertificateSignaturePubKey<ST>>), // MESSAGE_TYPE_NO_CONF
    ParticipationReport(PeerParticipationReport<CertificateSignaturePubKey<ST>>), // MESSAGE_TYPE_PARTICIPATION_REPORT
}

impl<ST: CertificateSignatureRecoverable> Encodable for FullNodesGroupMessage<ST> {
    fn encode(&self, out: &mut dyn BufMut) {
        let version = GROUP_MSG_VERSION;
        match self {
            Self::PrepareGroup(inner_msg) => {
                let enc: [&dyn Encodable; 3] = [&version, &MESSAGE_TYPE_PREP_REQ, inner_msg];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::PrepareGroupResponse(inner_msg) => {
                let enc: [&dyn Encodable; 3] = [&version, &MESSAGE_TYPE_PREP_RES, inner_msg];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::ConfirmGroup(inner_msg) => {
                let enc: [&dyn Encodable; 3] = [&version, &MESSAGE_TYPE_CONF_GRP, inner_msg];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::NoConfirm(inner_msg) => {
                let enc: [&dyn Encodable; 3] = [&version, &MESSAGE_TYPE_NO_CONF, inner_msg];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::ParticipationReport(inner_msg) => {
                let enc: [&dyn Encodable; 3] =
                    [&version, &MESSAGE_TYPE_PARTICIPATION_REPORT, inner_msg];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST: CertificateSignatureRecoverable> Decodable for FullNodesGroupMessage<ST> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let version = u8::decode(&mut payload)?;
        if version != GROUP_MSG_VERSION {
            return Err(alloy_rlp::Error::Custom("Unknown group message version"));
        }
        let result = match u8::decode(&mut payload)? {
            MESSAGE_TYPE_PREP_REQ => Self::PrepareGroup(PrepareGroup::decode(&mut payload)?),
            MESSAGE_TYPE_PREP_RES => {
                Self::PrepareGroupResponse(PrepareGroupResponse::decode(&mut payload)?)
            }
            MESSAGE_TYPE_CONF_GRP => Self::ConfirmGroup(ConfirmGroup::decode(&mut payload)?),
            MESSAGE_TYPE_NO_CONF => Self::NoConfirm(NoConfirm::decode(&mut payload)?),
            MESSAGE_TYPE_PARTICIPATION_REPORT => {
                Self::ParticipationReport(PeerParticipationReport::decode(&mut payload)?)
            }
            _ => {
                return Err(alloy_rlp::Error::Custom(
                    "Unknown FullNodesGroupMessage enum variant",
                ))
            }
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::certificate_signature::CertificateSignaturePubKey;
    use monad_peer_discovery::NameRecord;
    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;
    use monad_types::{NodeId, Round};

    use super::*;

    type ST = SecpSignature;
    type PubKeyType = CertificateSignaturePubKey<ST>;

    fn nid(seed: u64) -> NodeId<PubKeyType> {
        let key_pair = get_key::<ST>(seed);
        let pub_key = key_pair.pubkey();
        NodeId::new(pub_key)
    }

    fn make_prep_group(seed: u32) -> PrepareGroup<CertificateSignaturePubKey<ST>> {
        PrepareGroup {
            validator_id: nid(seed as u64),
            max_group_size: 1 + seed as usize,
            start_round: Round(11 + seed as u64),
            end_round: Round(17 + seed as u64),
        }
    }

    fn make_name_records(seed: u32, count: usize) -> Vec<MonadNameRecord<ST>> {
        (0..count)
            .map(|_| {
                let key = get_key::<ST>(seed as u64 + 42);
                let ip = std::net::Ipv4Addr::new(seed as u8, 0, 0, 1);
                let port = (seed + 16) as u16;

                MonadNameRecord::<ST>::new(NameRecord::new(ip, port, (seed + 200) as u64), &key)
            })
            .collect()
    }

    #[test]
    fn serialize_roundtrip_prep_group() {
        let org_msg = make_prep_group(3);
        let org_enum = FullNodesGroupMessage::PrepareGroup(org_msg);

        let mut encoded_bytes = Vec::new();
        org_enum.encode(&mut encoded_bytes); // 41 bytes

        let decoded_enum =
            FullNodesGroupMessage::<ST>::decode(&mut encoded_bytes.as_slice()).unwrap();
        assert_eq!(decoded_enum, org_enum);
    }

    #[test]
    fn serialize_roundtrip_group_res() {
        let org_msg = PrepareGroupResponse {
            req: make_prep_group(5),
            node_id: nid(2),
            accept: true,
        };
        let org_enum = FullNodesGroupMessage::PrepareGroupResponse(org_msg);

        let mut encoded_bytes = Vec::new();
        org_enum.encode(&mut encoded_bytes); // 79 bytes

        let decoded_enum =
            FullNodesGroupMessage::<ST>::decode(&mut encoded_bytes.as_slice()).unwrap();
        assert_eq!(decoded_enum, org_enum);
    }

    #[test]
    fn serialize_roundtrip_group_conf() {
        let org_msg = ConfirmGroup {
            prepare: make_prep_group(7),
            peers: [nid(8), nid(9), nid(10)].to_vec().into(),
            name_records: make_name_records(11, 3).into(),
        };
        let org_enum = FullNodesGroupMessage::ConfirmGroup(org_msg);

        let mut encoded_bytes = Vec::new();
        org_enum.encode(&mut encoded_bytes); // 306 bytes

        let decoded_enum =
            FullNodesGroupMessage::<ST>::decode(&mut encoded_bytes.as_slice()).unwrap();
        assert_eq!(decoded_enum, org_enum);
    }

    #[test]
    fn serialize_roundtrip_group_no_conf() {
        let org_msg = NoConfirm {
            prepare: make_prep_group(13),
            reason: NoConfirmReason::GroupFull,
        };
        let org_enum = FullNodesGroupMessage::NoConfirm(org_msg);

        let mut encoded_bytes = Vec::new();
        org_enum.encode(&mut encoded_bytes); // 44 bytes

        insta::assert_debug_snapshot!("no_conf_encoded", hex::encode(&encoded_bytes));

        let decoded_enum =
            FullNodesGroupMessage::<ST>::decode(&mut encoded_bytes.as_slice()).unwrap();
        assert_eq!(decoded_enum, org_enum);
    }

    #[test]
    fn no_confirm_reason_rejects_extra_bytes() {
        // Encode NoConfirmReason::GroupFull normally: RLP list with single u8
        // Valid encoding is [0xc1, 0x01] - a list of length 1 containing the byte 0x01
        // We'll create a malformed encoding with extra bytes: [0xc2, 0x01, 0xff]
        let malformed_encoding: &[u8] = &[0xc2, 0x01, 0xff];

        let result = NoConfirmReason::decode(&mut &malformed_encoding[..]);
        assert_eq!(
            result.unwrap_err(),
            alloy_rlp::Error::Custom("Extra bytes in NoConfirmReason")
        );
    }

    #[test]
    fn serialize_roundtrip_participation_report() {
        let org_msg = PeerParticipationReport {
            reporter: nid(5),
            validator_id: nid(1),
            round_span: RoundSpan::new(Round(10), Round(20)).unwrap(),
            peer_scores: vec![
                PeerParticipation {
                    peer: nid(2),
                    participation_score: BoundedU64::new(85).unwrap(),
                },
                PeerParticipation {
                    peer: nid(3),
                    participation_score: BoundedU64::new(100).unwrap(),
                },
                PeerParticipation {
                    peer: nid(4),
                    participation_score: BoundedU64::new(0).unwrap(),
                },
            ]
            .into(),
        };
        let message = FullNodesGroupMessage::ParticipationReport(org_msg);

        let mut encoded_bytes = Vec::new();
        message.encode(&mut encoded_bytes);

        insta::assert_debug_snapshot!("participation_report_encoded", hex::encode(&encoded_bytes));

        let decoded_enum =
            FullNodesGroupMessage::<ST>::decode(&mut encoded_bytes.as_slice()).unwrap();
        assert_eq!(decoded_enum, message);
    }
}
