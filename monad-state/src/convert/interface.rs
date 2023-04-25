use super::event::MonadEvent;
use monad_proto::{error::ProtoError, proto::event::ProtoMonadEvent};
use prost::Message;

pub fn serialize_event(event: &MonadEvent) -> Vec<u8> {
    let proto_event: ProtoMonadEvent = event.into();
    proto_event.encode_to_vec()
}

pub fn deserialize_event(data: &[u8]) -> Result<MonadEvent, ProtoError> {
    let event = ProtoMonadEvent::decode(data)?;
    event.try_into()
}
