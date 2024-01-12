use bytes::{Bytes, BytesMut};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_proto::{error::ProtoError, proto::message::ProtoMonadMessage};
use prost::Message;

use crate::{MonadMessage, VerifiedMonadMessage};

pub fn serialize_verified_monad_message<
    MS: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<MS>>,
>(
    msg: &VerifiedMonadMessage<MS, SCT>,
) -> Bytes {
    let proto_msg: ProtoMonadMessage = {
        let mut _convert_span = tracing::info_span!("convert_span").entered();
        msg.into()
    };
    let mut _encode_span = tracing::info_span!("encode_span").entered();
    // FIXME this copy can be avoided
    let mut buf = BytesMut::new();
    proto_msg
        .encode(&mut buf)
        .expect("message serialization shouldn't fail");
    buf.into()
}

pub fn deserialize_monad_message<MS, SCT>(data: Bytes) -> Result<MonadMessage<MS, SCT>, ProtoError>
where
    MS: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<MS>>,
{
    let message_len = data.len();
    let msg = {
        let mut _decode_span = tracing::info_span!("decode_span", ?message_len).entered();
        ProtoMonadMessage::decode(data)?
    };
    let mut _convert_span = tracing::info_span!("convert_span", ?message_len).entered();
    msg.try_into()
}
