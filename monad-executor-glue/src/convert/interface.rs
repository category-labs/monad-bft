use monad_consensus_types::{
    message_signature::MessageSignature,
    signature_collection::{SignatureCollection, SignatureCollectionPubKeyType},
};
use monad_proto::{
    error::ProtoError,
    proto::{basic::ProtoPubkey, event::ProtoMonadEvent},
};
use prost::Message;

use crate::MonadEvent;

pub fn serialize_event<S: MessageSignature, SCT: SignatureCollection>(
    event: &MonadEvent<S, SCT>,
) -> Vec<u8>
where
    for<'a> &'a SignatureCollectionPubKeyType<SCT>: Into<ProtoPubkey>,
{
    let proto_event: ProtoMonadEvent = event.into();
    proto_event.encode_to_vec()
}

pub fn deserialize_event<S: MessageSignature, SCT: SignatureCollection>(
    data: &[u8],
) -> Result<MonadEvent<S, SCT>, ProtoError>
where
    ProtoPubkey: TryInto<SignatureCollectionPubKeyType<SCT>, Error = ProtoError>,
{
    let event = ProtoMonadEvent::decode(data)?;
    event.try_into()
}
