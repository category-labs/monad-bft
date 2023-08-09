use monad_consensus_types::{
    certificate_signature::CertificateSignatureRecoverable, message_signature::MessageSignature,
};
use monad_proto::{error::ProtoError, proto::message::ProtoUnverifiedConsensusMessage};
use prost::Message;

use super::message::{UnverifiedConsensusMessage, VerifiedConsensusMessage};

pub fn serialize_verified_consensus_message(
    msg: &VerifiedConsensusMessage<impl MessageSignature, impl CertificateSignatureRecoverable>,
) -> Vec<u8> {
    let proto_msg: ProtoUnverifiedConsensusMessage = msg.into();
    proto_msg.encode_to_vec()
}

pub fn deserialize_unverified_consensus_message<
    MS: MessageSignature,
    CS: CertificateSignatureRecoverable,
>(
    data: &[u8],
) -> Result<UnverifiedConsensusMessage<MS, CS>, ProtoError> {
    let msg = ProtoUnverifiedConsensusMessage::decode(data)?;
    msg.try_into()
}
