use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_proto::{error::ProtoError, proto::message::ProtoUnverifiedConsensusMessage};
use prost::{
    bytes::{Bytes, BytesMut},
    Message,
};

use super::message::{UnverifiedConsensusMessage, VerifiedConsensusMessage};

pub fn serialize_verified_consensus_message(
    msg: &VerifiedConsensusMessage<impl MessageSignature, impl SignatureCollection>,
) -> Bytes {
    let proto_msg: ProtoUnverifiedConsensusMessage = msg.into();
    let mut buf = BytesMut::new();
    proto_msg
        .encode(&mut buf)
        .expect("message serialization shouldn't fail");
    buf.into()
}

pub fn deserialize_unverified_consensus_message<MS: MessageSignature, SCT: SignatureCollection>(
    data: &[u8],
) -> Result<UnverifiedConsensusMessage<MS, SCT>, ProtoError> {
    let msg = ProtoUnverifiedConsensusMessage::decode(data)?;
    msg.try_into()
}
