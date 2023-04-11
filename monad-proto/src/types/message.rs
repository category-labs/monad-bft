use prost::Message;

use monad_consensus::types::message::{TimeoutMessage as ConsensusTmoMsg, VoteMessage};
use monad_consensus::validation::signing::{Unverified, Verified};
use monad_crypto::secp256k1::SecpSignature;

use crate::error::ProtoError;

use super::signing::AggSecpSignature;

type VerifiedVoteMessage = Verified<SecpSignature, VoteMessage>;
type UnverifiedVoteMessage = Unverified<SecpSignature, VoteMessage>;
type TimeoutMessage = ConsensusTmoMsg<SecpSignature, AggSecpSignature>;
type VerifiedTimeoutMessage = Verified<SecpSignature, TimeoutMessage>;
type UnverifiedTimeoutMessage = Unverified<SecpSignature, TimeoutMessage>;

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

impl From<&VerifiedVoteMessage> for ProtoUnverifiedVoteMessage {
    fn from(value: &VerifiedVoteMessage) -> Self {
        Self {
            vote_msg: Some((&(**value)).into()),
            author_signature: Some(value.author_signature().into()),
        }
    }
}

impl TryFrom<ProtoUnverifiedVoteMessage> for UnverifiedVoteMessage {
    type Error = ProtoError;
    fn try_from(value: ProtoUnverifiedVoteMessage) -> Result<Self, Self::Error> {
        let msg: VoteMessage = value
            .vote_msg
            .ok_or(Self::Error::MissingRequiredField(
                "Unverified<VoteMessage>.obj".to_owned(),
            ))?
            .try_into()?;
        let signature: SecpSignature = value
            .author_signature
            .ok_or(Self::Error::MissingRequiredField(
                "Unverified<VoteMessage>.signature".to_owned(),
            ))?
            .try_into()?;
        Ok(Unverified::new(msg, signature))
    }
}

pub fn serialize_verified_vote_message(votemsg: &VerifiedVoteMessage) -> Vec<u8> {
    let proto_votemsg: ProtoUnverifiedVoteMessage = votemsg.into();
    proto_votemsg.encode_to_vec()
}

pub fn deserialize_unverified_vote_message(
    buf: &[u8],
) -> Result<UnverifiedVoteMessage, ProtoError> {
    let proto_votemsg = ProtoUnverifiedVoteMessage::decode(buf)?;
    let votemsg: UnverifiedVoteMessage = proto_votemsg.try_into()?;
    Ok(votemsg)
}

impl From<&TimeoutMessage> for ProtoTimeoutMessage {
    fn from(value: &TimeoutMessage) -> Self {
        ProtoTimeoutMessage {
            tminfo: Some((&value.tminfo).into()),
            last_round_tc: (value.last_round_tc.as_ref().map(|v| v.into())),
        }
    }
}

impl TryFrom<ProtoTimeoutMessage> for TimeoutMessage {
    type Error = ProtoError;
    fn try_from(value: ProtoTimeoutMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            tminfo: value
                .tminfo
                .ok_or(Self::Error::MissingRequiredField(
                    "TmoMsg<AggSig>.tminfo".to_owned(),
                ))?
                .try_into()?,

            last_round_tc: value.last_round_tc.map(|v| v.try_into()).transpose()?,
        })
    }
}

impl From<&VerifiedTimeoutMessage> for ProtoUnverifiedTimeoutMessage {
    fn from(value: &VerifiedTimeoutMessage) -> Self {
        Self {
            tmo_msg: Some((&(**value)).into()),
            author_signature: Some(value.author_signature().into()),
        }
    }
}

impl TryFrom<ProtoUnverifiedTimeoutMessage> for UnverifiedTimeoutMessage {
    type Error = ProtoError;
    fn try_from(value: ProtoUnverifiedTimeoutMessage) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value
                .tmo_msg
                .ok_or(Self::Error::MissingRequiredField(
                    "Unverified<TimeoutMessage<AggregateSignatures>>.obj".to_owned(),
                ))?
                .try_into()?,
            value
                .author_signature
                .ok_or(Self::Error::MissingRequiredField(
                    "Unverified<TimeoutMessage<AggregateSignatures>>.author_signature".to_owned(),
                ))?
                .try_into()?,
        ))
    }
}

pub fn serialize_verified_timeout_message(tmomsg: &VerifiedTimeoutMessage) -> Vec<u8> {
    let proto_tmomsg: ProtoUnverifiedTimeoutMessage = tmomsg.into();
    proto_tmomsg.encode_to_vec()
}

pub fn deserialize_unverified_timeout_message(
    buf: &[u8],
) -> Result<UnverifiedTimeoutMessage, ProtoError> {
    let proto_tmomsg = ProtoUnverifiedTimeoutMessage::decode(buf)?;
    let tmomsg: UnverifiedTimeoutMessage = proto_tmomsg.try_into()?;
    Ok(tmomsg)
}
