use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_proto::{error::ProtoError, proto::event::ProtoMonadEvent};
use prost::Message;

use crate::MonadEvent;

pub fn serialize_event<S: MessageSignature, SCT: SignatureCollection>(
    event: &MonadEvent<S, SCT>,
) -> Vec<u8>
where
    for<'a> &'a MonadEvent<S, SCT>: Into<ProtoMonadEvent>,
{
    let proto_event: ProtoMonadEvent = event.into();
    proto_event.encode_to_vec()
}

pub fn deserialize_event<S: MessageSignature, SCT: SignatureCollection>(
    data: &[u8],
) -> Result<MonadEvent<S, SCT>, ProtoError>
where
    MonadEvent<S, SCT>: TryFrom<ProtoMonadEvent, Error = ProtoError>,
{
    let event = ProtoMonadEvent::decode(data)?;
    event.try_into()
}
