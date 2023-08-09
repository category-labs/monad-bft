use monad_consensus_types::{
    certificate_signature::CertificateSignatureRecoverable, message_signature::MessageSignature,
};
use monad_proto::{error::ProtoError, proto::event::ProtoMonadEvent};
use prost::Message;

use super::event::MonadEvent;

pub fn serialize_event(
    event: &MonadEvent<impl MessageSignature + CertificateSignatureRecoverable>,
) -> Vec<u8> {
    let proto_event: ProtoMonadEvent = event.into();
    proto_event.encode_to_vec()
}

pub fn deserialize_event<S: MessageSignature + CertificateSignatureRecoverable>(
    data: &[u8],
) -> Result<MonadEvent<S>, ProtoError> {
    let event = ProtoMonadEvent::decode(data)?;
    event.try_into()
}
