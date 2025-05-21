use alloy_rlp::{encode_list, Decodable, Encodable, Header};
use bytes::{Bytes, BytesMut};
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;

use super::{
    raptorcast_secondary::group_message::FullNodesGroupMessage,
    rlp::{
        try_deserialize_rlp_message, try_serialize_rlp_message, DeserializeError,
        NetworkMessageVersion, SerializeError,
    },
};

const MESSAGE_TYPE_APP: u8 = 1;
const MESSAGE_TYPE_GROUP: u8 = 3;

pub enum OutboundRouterMessage<OM, ST: CertificateSignatureRecoverable> {
    AppMessage(OM),                            // MESSAGE_TYPE_APP
    FullNodesGroup(FullNodesGroupMessage<ST>), // MESSAGE_TYPE_GROUP
}

impl<OM: Encodable, ST: CertificateSignatureRecoverable> OutboundRouterMessage<OM, ST> {
    pub fn try_serialize(self) -> Result<Bytes, SerializeError> {
        let mut buf = BytesMut::new();

        let version = NetworkMessageVersion::version();
        match self {
            Self::AppMessage(app_message) => {
                try_serialize_rlp_message(MESSAGE_TYPE_APP, app_message, version, &mut buf)?;
            }
            Self::FullNodesGroup(generic_grp_message) => {
                let enc: [&dyn Encodable; 3] = [&version, &MESSAGE_TYPE_GROUP, &generic_grp_message];
                encode_list::<_, dyn Encodable>(&enc, &mut buf);
            }
        };

        Ok(buf.into())
    }
}

pub enum InboundRouterMessage<AM, ST: CertificateSignatureRecoverable> {
    AppMessage(AM),                            // MESSAGE_TYPE_APP
    FullNodesGroup(FullNodesGroupMessage<ST>), // MESSAGE_TYPE_GROUP
}

impl<AM: Decodable, ST: CertificateSignatureRecoverable> InboundRouterMessage<AM, ST> {
    pub fn try_deserialize(data: &Bytes) -> Result<Self, DeserializeError> {
        let mut payload =
            Header::decode_bytes(&mut data.as_ref(), true).map_err(DeserializeError::from)?;
        let version =
            NetworkMessageVersion::decode(&mut payload).map_err(DeserializeError::from)?;
        let message_type = u8::decode(&mut payload).map_err(DeserializeError::from)?;
        match message_type {
            MESSAGE_TYPE_APP => {
                let decoded_msg: AM = try_deserialize_rlp_message(version, &mut payload)?;
                Ok(Self::AppMessage(decoded_msg))
            }
            MESSAGE_TYPE_GROUP => {
                let decoded_msg = FullNodesGroupMessage::decode(&mut payload)?;
                Ok(Self::FullNodesGroup(decoded_msg))
            }
            _ => Err(DeserializeError("unknown message type".into())),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{
        raptorcast_secondary::group_message::FullNodesGroupMessage,
        raptorcast_secondary::group_message::PrepareGroup,
    };
    use monad_secp::SecpSignature;
    use monad_types::{NodeId, Round};
    use monad_testutil::signing::get_key;
    use monad_crypto::certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    };

    type ST = SecpSignature;
    type PubKeyType = CertificateSignaturePubKey<ST>;
    type NodeIdST<ST> = NodeId<CertificateSignaturePubKey<ST>>;
    type OM = String;

    fn nid(seed: u64) -> NodeId<PubKeyType> {
        let key_pair = get_key::<ST>(seed);
        let pub_key = key_pair.pubkey();
        NodeId::new(pub_key)
    }

    fn make_prep_group(seed: u32) -> PrepareGroup<ST>
    {
        PrepareGroup {
            validator_id: nid(seed as u64),
            max_group_size: 1 + seed as usize,
            start_round: Round(11 + seed as u64),
            end_round: Round(17 + seed as u64),
        }
    }

    #[test]
    fn serialize_roundtrip_app_msg() {
        let org_msg = "MyProposal".to_string();
        let serialized = OutboundRouterMessage::<OM, ST>::AppMessage(org_msg.clone())
            .try_serialize()
            .unwrap();
        //pub struct NetworkMessageVersion {
        //    pub serialize_version: u32,
        //    pub compression_version: u32,
        //}

        // https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp
        // 10 bytes for the string "MyProposal"
        //                   M  y  P  r  o  p  o  s  a  l
        // cf c2 01 01 01 8a 4d 79 50 72 6f 70 6f 73 61 6c
        println!("Serialized {:?} bytes: {}", serialized.len(), hex::encode(&serialized));
        let deserialized = InboundRouterMessage::<OM, ST>::try_deserialize(&serialized).unwrap();
        match deserialized {
            InboundRouterMessage::AppMessage(dser_msg) => {
                assert_eq!(dser_msg, org_msg);
            },
            _ => panic!("Expected FullNodesGroup"),
        };
    }

    #[test]
    fn serialize_roundtrip_prep_group() {
        let org_msg = make_prep_group(3);
        let prep_group = FullNodesGroupMessage::PrepareGroup(org_msg.clone());
        let serialized = OutboundRouterMessage::<OM, ST>::FullNodesGroup(prep_group.clone())
            .try_serialize()
            .unwrap();
        println!("Serialized {:?} bytes", serialized.len());
        let deserialized = InboundRouterMessage::<OM, ST>::try_deserialize(&serialized).unwrap();
        match deserialized {
            InboundRouterMessage::FullNodesGroup(dser_msg) => {
                match dser_msg {
                    FullNodesGroupMessage::PrepareGroup(dser_msg) => {
                        assert_eq!(dser_msg.validator_id, org_msg.validator_id);
                        assert_eq!(dser_msg.max_group_size, org_msg.max_group_size);
                        assert_eq!(dser_msg.start_round, org_msg.start_round);
                        assert_eq!(dser_msg.end_round, org_msg.end_round);
                    },
                    FullNodesGroupMessage::PrepareGroupResponse(_) => {
                        panic!("Expected PrepareGroup, got PrepareGroupResponse");
                    },
                    FullNodesGroupMessage::ConfirmGroup(_) => {
                        panic!("Expected PrepareGroup, got ConfirmGroup");
                    }
                }
            },
            InboundRouterMessage::AppMessage(msg) => {
                panic!("Expected FullNodesGroup, got AppMessage: {:?}", msg);
            },
            _ => panic!("Expected FullNodesGroup"),
        };
    }

}
