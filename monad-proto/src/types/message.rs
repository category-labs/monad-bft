use crate::error::ProtoError;
use monad_consensus::types::message::VoteMessage;
use monad_consensus::validation::signing::{Signed, Unverified};
use prost::Message;

include!(concat!(env!("OUT_DIR"), "/monad_proto.message.rs"));

impl From<&VoteMessage> for ProtoVoteMessage {
    fn from(votemsg: &VoteMessage) -> Self {
        ProtoVoteMessage {
            vote_info: Some((&votemsg.vote_info).into()),
            ledger_commit_info: Some((&votemsg.ledger_commit_info).into()),
        }
    }
}

impl From<&Unverified<VoteMessage>> for ProtoUnverifiedVoteMessage {
    fn from(votemsg: &Unverified<VoteMessage>) -> Self {
        ProtoUnverifiedVoteMessage {
            vote_msg: Some((&votemsg.0.obj).into()),
            author: Some((&votemsg.0.author).into()),
            author_signature: Some((&votemsg.0.author_signature).into()),
        }
    }
}

impl TryFrom<ProtoVoteMessage> for VoteMessage {
    type Error = ProtoError;

    fn try_from(proto_votemsg: ProtoVoteMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            vote_info: proto_votemsg
                .vote_info
                .ok_or(Self::Error::MissingRequiredField(
                    "VoteMessage.vote_info".to_owned(),
                ))?
                .try_into()?,
            ledger_commit_info: proto_votemsg
                .ledger_commit_info
                .ok_or(Self::Error::MissingRequiredField(
                    "VoteMessage.ledger_commit_info".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

impl TryFrom<ProtoUnverifiedVoteMessage> for Unverified<VoteMessage> {
    type Error = ProtoError;
    fn try_from(value: ProtoUnverifiedVoteMessage) -> Result<Self, Self::Error> {
        Ok(Unverified::<VoteMessage> {
            0: Signed::<VoteMessage, false> {
                obj: value
                    .vote_msg
                    .ok_or(Self::Error::MissingRequiredField(
                        "Unverified<VoteMessage>.obj".to_owned(),
                    ))?
                    .try_into()?,
                author: value
                    .author
                    .ok_or(Self::Error::MissingRequiredField(
                        "Unverified<VoteMessage>.author".to_owned(),
                    ))?
                    .try_into()?,
                author_signature: value
                    .author_signature
                    .ok_or(Self::Error::MissingRequiredField(
                        "Unverified<VoteMessage>.signature".to_owned(),
                    ))?
                    .try_into()?,
            },
        })
    }
}
pub fn serialize_unverified_vote_message(votemsg: &Unverified<VoteMessage>) -> Vec<u8> {
    let proto_votemsg: ProtoUnverifiedVoteMessage = votemsg.into();
    let mut buf = Vec::with_capacity(proto_votemsg.encoded_len());
    proto_votemsg.encode(&mut buf).unwrap();
    buf
}

pub fn deserialize_unverified_vote_message(
    buf: &[u8],
) -> Result<Unverified<VoteMessage>, ProtoError> {
    let proto_votemsg = ProtoUnverifiedVoteMessage::decode(buf)?;
    let votemsg: Unverified<VoteMessage> = proto_votemsg.try_into()?;
    Ok(votemsg)
}
