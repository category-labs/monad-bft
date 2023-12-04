use bytes::{Bytes, BytesMut};
use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_proto::{error::ProtoError, proto::message::ProtoUnverifiedConsensusMessage};
use prost::Message;

use super::message::{UnverifiedConsensusMessage, VerifiedConsensusMessage};

pub fn serialize_verified_consensus_message(
    msg: &VerifiedConsensusMessage<impl MessageSignature, impl SignatureCollection>,
) -> Bytes {
    let proto_msg: ProtoUnverifiedConsensusMessage = {
        let mut _convert_span = tracing::info_span!("convert_span").entered();
        msg.into()
    };
    let mut _encode_span = tracing::info_span!("encode_span").entered();
    // FIXME this copy can be avoided
    let mut buf = BytesMut::new();
    proto_msg
        .encode(&mut buf)
        .expect("message serialization shouldn't fail");
    buf.into()
}

pub fn deserialize_unverified_consensus_message<MS: MessageSignature, SCT: SignatureCollection>(
    data: Bytes,
) -> Result<UnverifiedConsensusMessage<MS, SCT>, ProtoError> {
    let message_len = data.len();
    let msg = {
        let mut _decode_span = tracing::info_span!("decode_span", ?message_len).entered();
        ProtoUnverifiedConsensusMessage::decode(data)?
    };
    let mut _convert_span = tracing::info_span!("convert_span", ?message_len).entered();
    msg.try_into()
}
