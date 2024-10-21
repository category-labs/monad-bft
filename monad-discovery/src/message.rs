use bytes::{Bytes, BytesMut};
use monad_crypto::certificate_signature::PubKey;
use monad_proto::{
    error::ProtoError,
    proto::message::{
        proto_discovery_message::Message, ProtoDiscoveryMessage, ProtoDiscoveryRequest,
        ProtoDiscoveryResponse, ProtoRouterMessage,
    },
};
use monad_types::{Deserializable, Serializable};
use prost::Message as _;

use crate::MonadNameRecord;

#[derive(Debug)]
pub struct DiscoveryRequest<PT: PubKey> {
    sender: MonadNameRecord<PT>,
}

impl<PT: PubKey> TryFrom<ProtoDiscoveryRequest> for DiscoveryRequest<PT> {
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
impl<PT: PubKey> From<&DiscoveryRequest<PT>> for ProtoDiscoveryRequest {
    fn from(value: &DiscoveryRequest<PT>) -> Self {
        ProtoDiscoveryRequest {
            self_: Some((&value.sender).into()),
        }
    }
}

#[derive(Debug)]
pub struct DiscoveryResponse<PT: PubKey> {
    peers: Vec<MonadNameRecord<PT>>,
}

impl<PT: PubKey> TryFrom<ProtoDiscoveryResponse> for DiscoveryResponse<PT> {
    type Error = ProtoError;

    fn try_from(value: ProtoDiscoveryResponse) -> Result<Self, Self::Error> {
        let peers = value
            .peers
            .into_iter()
            .map(MonadNameRecord::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { peers })
    }
}
impl<PT: PubKey> From<&DiscoveryResponse<PT>> for ProtoDiscoveryResponse {
    fn from(value: &DiscoveryResponse<PT>) -> Self {
        ProtoDiscoveryResponse {
            peers: value.peers.iter().map(Into::into).collect::<Vec<_>>(),
        }
    }
}

#[derive(Debug)]
pub enum DiscoveryMessage<PT: PubKey> {
    DiscoveryRequest(DiscoveryRequest<PT>),
    DiscoveryResponse(DiscoveryResponse<PT>),
}

impl<PT: PubKey> TryFrom<ProtoDiscoveryMessage> for DiscoveryMessage<PT> {
    type Error = ProtoError;

    fn try_from(value: ProtoDiscoveryMessage) -> Result<Self, Self::Error> {
        match value.message.ok_or(ProtoError::MissingRequiredField(
            "ProtoDiscoveryMessage.message".to_owned(),
        ))? {
            Message::Request(request) => {
                Ok(DiscoveryMessage::DiscoveryRequest(request.try_into()?))
            }
            Message::Response(response) => {
                Ok(DiscoveryMessage::DiscoveryResponse(response.try_into()?))
            }
        }
    }
}

impl<PT: PubKey> From<&DiscoveryMessage<PT>> for ProtoDiscoveryMessage {
    fn from(value: &DiscoveryMessage<PT>) -> Self {
        match value {
            DiscoveryMessage::DiscoveryRequest(request) => ProtoDiscoveryMessage {
                message: Some(Message::Request(request.into())),
            },
            DiscoveryMessage::DiscoveryResponse(response) => ProtoDiscoveryMessage {
                message: Some(Message::Response(response.into())),
            },
        }
    }
}

impl<PT: PubKey> Serializable<Bytes> for DiscoveryMessage<PT> {
    fn serialize(&self) -> Bytes {
        let msg: ProtoDiscoveryMessage = self.into();

        let mut buf = BytesMut::new();
        msg.encode(&mut buf)
            .expect("message serialization shouldn't fail");
        buf.into()
    }
}

pub enum OutboundRouterMessage<'a, OM, PT: PubKey> {
    Application(&'a OM),
    Discovery(DiscoveryMessage<PT>),
}

impl<'a, OM, PT: PubKey> From<&OutboundRouterMessage<'a, OM, PT>> for ProtoRouterMessage
where
    OM: Serializable<Bytes>,
{
    fn from(value: &OutboundRouterMessage<'a, OM, PT>) -> Self {
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

impl<OM: Serializable<Bytes>, PT: PubKey> Serializable<Bytes>
    for OutboundRouterMessage<'_, OM, PT>
{
    fn serialize(&self) -> Bytes {
        let msg: ProtoRouterMessage = self.into();

        let mut buf = BytesMut::new();
        msg.encode(&mut buf)
            .expect("message serialization shouldn't fail");
        buf.into()
    }
}

pub enum InboundRouterMessage<M, PT: PubKey> {
    Application(M),
    Discovery(DiscoveryMessage<PT>),
}

impl<M: Deserializable<Bytes>, PT: PubKey> TryFrom<ProtoRouterMessage>
    for InboundRouterMessage<M, PT>
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

impl<M: Deserializable<Bytes>, PT: PubKey> Deserializable<Bytes> for InboundRouterMessage<M, PT> {
    type ReadError = ProtoError;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        ProtoRouterMessage::decode(message.clone())?.try_into()
    }
}
