use bytes::{Bytes, BytesMut};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_proto::{error::ProtoError, proto::event::ProtoMonadEvent};
use prost::Message;

use crate::MonadEvent;

pub fn serialize_event<S: CertificateSignatureRecoverable, SCT: SignatureCollection>(
    event: &MonadEvent<S, SCT>,
) -> Bytes {
    let proto_event: ProtoMonadEvent = event.into();
    let mut buf = BytesMut::new();
    proto_event
        .encode(&mut buf)
        .expect("event serialization shouldn't fail");
    buf.into()
}

pub fn deserialize_event<S: CertificateSignatureRecoverable, SCT: SignatureCollection>(
    data: &[u8],
) -> Result<MonadEvent<S, SCT>, ProtoError> {
    let event = ProtoMonadEvent::decode(data)?;
    event.try_into()
}
