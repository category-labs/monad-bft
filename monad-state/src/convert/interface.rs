use super::event::MonadEvent;
use monad_crypto::{
    secp256k1::{SecpError, SecpPubKey},
    Signature,
};
use monad_proto::{error::ProtoError, proto::event::ProtoMonadEvent};
use prost::Message;

pub fn serialize_event(
    event: &MonadEvent<impl Signature<PubKey = SecpPubKey, Error = SecpError>>,
) -> Vec<u8> {
    let proto_event: ProtoMonadEvent = event.into();
    proto_event.encode_to_vec()
}

pub fn deserialize_event<S: Signature<PubKey = SecpPubKey, Error = SecpError>>(
    data: &[u8],
) -> Result<MonadEvent<S>, ProtoError> {
    let event = ProtoMonadEvent::decode(data)?;
    event.try_into()
}
