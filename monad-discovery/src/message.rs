use bytes::{Bytes, BytesMut};
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_proto::{
    error::ProtoError,
    proto::message::{
        proto_discovery_message::Message, ProtoDiscoveryMessage, ProtoDiscoveryRequest,
        ProtoDiscoveryResponse, ProtoRouterMessage,
    },
};
use monad_types::{Deserializable, Serializable};
use prost::Message as _;

use crate::SignedMonadNameRecord;

#[derive(Debug)]
pub struct DiscoveryRequest<ST: CertificateSignatureRecoverable> {
    pub sender: SignedMonadNameRecord<ST>,
}

impl<ST: CertificateSignatureRecoverable> TryFrom<ProtoDiscoveryRequest> for DiscoveryRequest<ST> {
    type Error = ProtoError;

    fn try_from(value: ProtoDiscoveryRequest) -> Result<Self, Self::Error> {
        let sender = value
            .self_
            .ok_or(ProtoError::MissingRequiredField(
                "ProtoDiscoveryRequest".to_owned(),
            ))?
            .try_into()?;
        Ok(Self { sender })
    }
}
impl<ST: CertificateSignatureRecoverable> From<&DiscoveryRequest<ST>> for ProtoDiscoveryRequest {
    fn from(value: &DiscoveryRequest<ST>) -> Self {
        ProtoDiscoveryRequest {
            self_: Some((&value.sender).into()),
        }
    }
}

#[derive(Debug)]
pub struct DiscoveryResponse<ST: CertificateSignatureRecoverable> {
    pub peers: Vec<SignedMonadNameRecord<ST>>,
}

impl<ST: CertificateSignatureRecoverable> TryFrom<ProtoDiscoveryResponse>
    for DiscoveryResponse<ST>
{
    type Error = ProtoError;

    fn try_from(value: ProtoDiscoveryResponse) -> Result<Self, Self::Error> {
        let peers = value
            .peers
            .into_iter()
            .map(SignedMonadNameRecord::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { peers })
    }
}
impl<ST: CertificateSignatureRecoverable> From<&DiscoveryResponse<ST>> for ProtoDiscoveryResponse {
    fn from(value: &DiscoveryResponse<ST>) -> Self {
        ProtoDiscoveryResponse {
            peers: value.peers.iter().map(Into::into).collect::<Vec<_>>(),
        }
    }
}

#[derive(Debug)]
pub enum DiscoveryMessage<ST: CertificateSignatureRecoverable> {
    Request(DiscoveryRequest<ST>),
    Response(DiscoveryResponse<ST>),
}

impl<ST: CertificateSignatureRecoverable> TryFrom<ProtoDiscoveryMessage> for DiscoveryMessage<ST> {
    type Error = ProtoError;

    fn try_from(value: ProtoDiscoveryMessage) -> Result<Self, Self::Error> {
        match value.message.ok_or(ProtoError::MissingRequiredField(
            "ProtoDiscoveryMessage.message".to_owned(),
        ))? {
            Message::Request(request) => Ok(DiscoveryMessage::Request(request.try_into()?)),
            Message::Response(response) => Ok(DiscoveryMessage::Response(response.try_into()?)),
        }
    }
}

impl<ST: CertificateSignatureRecoverable> From<&DiscoveryMessage<ST>> for ProtoDiscoveryMessage {
    fn from(value: &DiscoveryMessage<ST>) -> Self {
        match value {
            DiscoveryMessage::Request(request) => ProtoDiscoveryMessage {
                message: Some(Message::Request(request.into())),
            },
            DiscoveryMessage::Response(response) => ProtoDiscoveryMessage {
                message: Some(Message::Response(response.into())),
            },
        }
    }
}

pub enum OutboundRouterMessage<'a, OM, ST: CertificateSignatureRecoverable> {
    Application(&'a OM),
    Discovery(DiscoveryMessage<ST>),
}

impl<'a, OM, ST: CertificateSignatureRecoverable> From<&OutboundRouterMessage<'a, OM, ST>>
    for ProtoRouterMessage
where
    OM: Serializable<Bytes>,
{
    fn from(value: &OutboundRouterMessage<'a, OM, ST>) -> Self {
        match value {
            OutboundRouterMessage::Application(app_message) => {
                let serialized_app_message: Bytes = (*app_message).serialize();
                ProtoRouterMessage {
                    message: Some(
                        monad_proto::proto::message::proto_router_message::Message::AppMessage(
                            serialized_app_message,
                        ),
                    ),
                }
            }
            OutboundRouterMessage::Discovery(discovery) => ProtoRouterMessage {
                message: Some(
                    monad_proto::proto::message::proto_router_message::Message::DiscoveryMessage(
                        discovery.into(),
                    ),
                ),
            },
        }
    }
}

impl<OM: Serializable<Bytes>, ST: CertificateSignatureRecoverable> Serializable<Bytes>
    for OutboundRouterMessage<'_, OM, ST>
{
    fn serialize(&self) -> Bytes {
        let msg: ProtoRouterMessage = self.into();

        let mut buf = BytesMut::new();
        msg.encode(&mut buf)
            .expect("message serialization shouldn't fail");
        buf.into()
    }
}

pub enum InboundRouterMessage<M, ST: CertificateSignatureRecoverable> {
    Application(M),
    Discovery(DiscoveryMessage<ST>),
}

impl<M: Deserializable<Bytes>, ST: CertificateSignatureRecoverable> TryFrom<ProtoRouterMessage>
    for InboundRouterMessage<M, ST>
{
    type Error = ProtoError;

    fn try_from(value: ProtoRouterMessage) -> Result<Self, Self::Error> {
        match value.message.ok_or(ProtoError::MissingRequiredField(
            "ProtoRouterMessage.message".to_owned(),
        ))? {
            monad_proto::proto::message::proto_router_message::Message::AppMessage(app_message) => {
                let app_message = M::deserialize(&app_message).map_err(|_| {
                    /*
                        TODO(rene): This map_err is not ideal because it effectively drops a future
                        error type for an opaque string error. We can remove this map_err and
                        convert to ProtoError using ?, but then we would have to specify the
                        ReadError generic associated type in the Deserializable<Bytes> bound on M
                        like so

                        M: Deserializable<Bytes, ReadError = ProtoError>

                        but then this bound has to propagate upwards to the RaptorCast type, which
                        is also not ideal.
                    */
                    ProtoError::DeserializeError("unknown deserialization error".to_owned())
                })?;
                Ok(InboundRouterMessage::Application(app_message))
            }
            monad_proto::proto::message::proto_router_message::Message::DiscoveryMessage(
                discovery_message,
            ) => Ok(InboundRouterMessage::Discovery(
                discovery_message.try_into()?,
            )),
        }
    }
}

impl<M: Deserializable<Bytes>, ST: CertificateSignatureRecoverable> Deserializable<Bytes>
    for InboundRouterMessage<M, ST>
{
    type ReadError = ProtoError;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        ProtoRouterMessage::decode(message.clone())?.try_into()
    }
}
