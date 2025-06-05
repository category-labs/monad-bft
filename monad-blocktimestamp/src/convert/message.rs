use monad_proto::{
    error::ProtoError,
    proto::blocktimestamp::{ProtoPingRequest, ProtoPingResponse},
};

use crate::messages::message::{PingRequestMessage, PingResponseMessage};

impl From<&PingResponseMessage> for ProtoPingResponse {
    fn from(value: &PingResponseMessage) -> Self {
        let PingResponseMessage {
            sequence,
            avg_wait_ns,
        } = value;
        Self {
            sequence: *sequence,
            avg_wait_ns: *avg_wait_ns,
        }
    }
}

impl TryFrom<ProtoPingResponse> for PingResponseMessage {
    type Error = ProtoError;

    fn try_from(value: ProtoPingResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            sequence: value.sequence,
            avg_wait_ns: value.avg_wait_ns,
        })
    }
}

impl From<&PingRequestMessage> for ProtoPingRequest {
    fn from(value: &PingRequestMessage) -> Self {
        let PingRequestMessage { sequence } = value;
        Self {
            sequence: *sequence,
        }
    }
}

impl TryFrom<ProtoPingRequest> for PingRequestMessage {
    type Error = ProtoError;

    fn try_from(value: ProtoPingRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            sequence: value.sequence,
        })
    }
}
