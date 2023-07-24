use monad_crypto::Signature;
use monad_proto::{error::ProtoError, proto::event::ProtoMonadEvent};
use prost::Message;

use super::event::MonadEvent;

pub fn serialize_event(event: &MonadEvent<impl Signature>) -> Vec<u8> {
    let proto_event: ProtoMonadEvent = event.into();
    proto_event.encode_to_vec()
}

pub fn deserialize_event<S: Signature>(data: &[u8]) -> Result<MonadEvent<S>, ProtoError> {
    let event = ProtoMonadEvent::decode(data)?;
    event.try_into()
}
