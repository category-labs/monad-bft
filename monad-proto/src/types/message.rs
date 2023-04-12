use crate::error::ProtoError;
use monad_consensus::types::message::VoteMessage;
use monad_consensus::types::signature::ConsensusSignature;
use monad_consensus::validation::signing::{Unverified, Verified};
use prost::Message;

include!(concat!(env!("OUT_DIR"), "/monad_proto.message.rs"));

impl From<&VoteMessage> for ProtoVoteMessage {
    fn from(value: &VoteMessage) -> Self {
        ProtoVoteMessage {
            vote_info: Some((&value.vote_info).into()),
            ledger_commit_info: Some((&value.ledger_commit_info).into()),
        }
    }
}

impl TryFrom<ProtoVoteMessage> for VoteMessage {
    type Error = ProtoError;

    fn try_from(value: ProtoVoteMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            vote_info: value
                .vote_info
                .ok_or(Self::Error::MissingRequiredField(
                    "VoteMessage.vote_info".to_owned(),
                ))?
                .try_into()?,
            ledger_commit_info: value
                .ledger_commit_info
                .ok_or(Self::Error::MissingRequiredField(
                    "VoteMessage.ledger_commit_info".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

impl From<&Verified<VoteMessage>> for ProtoUnverifiedVoteMessage {
    fn from(value: &Verified<VoteMessage>) -> Self {
        Self {
            vote_msg: Some((&(**value)).into()),
            author_signature: Some(value.author_signature().into()),
        }
    }
}

impl TryFrom<ProtoUnverifiedVoteMessage> for Unverified<VoteMessage> {
    type Error = ProtoError;
    fn try_from(value: ProtoUnverifiedVoteMessage) -> Result<Self, Self::Error> {
        let msg: VoteMessage = value
            .vote_msg
            .ok_or(Self::Error::MissingRequiredField(
                "Unverified<VoteMessage>.obj".to_owned(),
            ))?
            .try_into()?;
        let signature: ConsensusSignature = value
            .author_signature
            .ok_or(Self::Error::MissingRequiredField(
                "Unverified<VoteMessage>.signature".to_owned(),
            ))?
            .try_into()?;
        Ok(Unverified::new(msg, signature))
    }
}

pub fn serialize_verified_vote_message(votemsg: &Verified<VoteMessage>) -> Vec<u8> {
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
