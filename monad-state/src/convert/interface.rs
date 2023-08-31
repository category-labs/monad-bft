use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_proto::{error::ProtoError, proto::event::ProtoMonadEvent};
use prost::Message;

use crate::MonadEvent;

pub fn serialize_event(
    event: &MonadEvent<impl MessageSignature, impl SignatureCollection>,
) -> Vec<u8> {
    let proto_event: ProtoMonadEvent = event.into();
    proto_event.encode_to_vec()
}

pub fn deserialize_event<S: MessageSignature, SCT: SignatureCollection>(
    data: &[u8],
) -> Result<MonadEvent<S, SCT>, ProtoError> {
    let event = ProtoMonadEvent::decode(data)?;
    event.try_into()
}
