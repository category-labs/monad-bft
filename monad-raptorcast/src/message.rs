use alloy_rlp::{Decodable, Encodable, Header};
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
const MESSAGE_TYPE_GROUP: u8 = 2;

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
                generic_grp_message.try_serialize_inner(version)?;
            }
        };

        Ok(buf.into())
    }
}

pub enum InboundRouterMessage<M, ST: CertificateSignatureRecoverable> {
    AppMessage(M),                             // MESSAGE_TYPE_APP
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
                let decoded_msg =
                    FullNodesGroupMessage::try_deserialize_inner(version, &mut payload)?;
                Ok(Self::FullNodesGroup(decoded_msg))
            }
            _ => Err(DeserializeError("unknown message type".into())),
        }
    }
}
