use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_proto::{error::ProtoError, proto::event::*};

use crate::{MonadEvent, ConsensusEvent};

impl<S: MessageSignature, SCT: SignatureCollection> From<&MonadEvent<S, SCT>> for ProtoMonadEvent
where
    for<'a> &'a ConsensusEvent<S, SCT>: Into<ProtoConsensusEvent>
{
    fn from(value: &MonadEvent<S, SCT>) -> Self {
        let event = match value {
            MonadEvent::ConsensusEvent(msg) => proto_monad_event::Event::ConsensusEvent(msg.into()),
        };
        Self { event: Some(event) }
    }
}

impl<S: MessageSignature, SCT: SignatureCollection> TryFrom<ProtoMonadEvent>
    for MonadEvent<S, SCT>
where
    ConsensusEvent<S, SCT>: TryFrom<ProtoConsensusEvent, Error = ProtoError>
{
    type Error = ProtoError;
    fn try_from(value: ProtoMonadEvent) -> Result<Self, Self::Error> {
        let event: MonadEvent<S, SCT> = match value.event {
            Some(proto_monad_event::Event::ConsensusEvent(event)) => {
                MonadEvent::ConsensusEvent(event.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "MonadEvent.event".to_owned(),
            ))?,
        };
        Ok(event)
    }
}
