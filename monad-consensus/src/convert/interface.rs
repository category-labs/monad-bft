use monad_crypto::Signature;
use prost::Message;

use monad_proto::error::ProtoError;
use monad_proto::proto::message::ProtoUnverifiedConsensusMessage;

use super::message::{UnverifiedConsensusMessage, VerifiedConsensusMessage};

pub fn serialize_verified_consensus_message(
    msg: &VerifiedConsensusMessage<impl Signature>,
) -> Vec<u8> {
    let proto_msg: ProtoUnverifiedConsensusMessage = msg.into();
    proto_msg.encode_to_vec()
}

pub fn deserialize_unverified_consensus_message<S: Signature>(
    data: &[u8],
) -> Result<UnverifiedConsensusMessage<S>, ProtoError> {
    let msg = ProtoUnverifiedConsensusMessage::decode(data)?;
    msg.try_into()
}
