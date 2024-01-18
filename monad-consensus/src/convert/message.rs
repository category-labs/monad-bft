use std::ops::Deref;

use monad_consensus_types::{
    convert::signing::{certificate_signature_to_proto, proto_to_certificate_signature},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{CertificateSignatureRecoverable, PubKey};
use monad_proto::{error::ProtoError, proto::message::*};

use crate::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{
            BlockSyncResponseMessage, CascadeTxMessage, PeerStateRootMessage, ProposalMessage,
            RequestBlockSyncMessage, TimeoutMessage, VoteMessage,
        },
    },
    validation::signing::{Unvalidated, Unverified, Validated, Verified},
};

pub(crate) type VerifiedConsensusMessage<MS, SCT> = Verified<MS, Validated<ConsensusMessage<SCT>>>;
pub(crate) type UnverifiedConsensusMessage<MS, SCT> =
    Unverified<MS, Unvalidated<ConsensusMessage<SCT>>>;

impl<SCT: SignatureCollection> From<&VoteMessage<SCT>> for ProtoVoteMessage {
    fn from(value: &VoteMessage<SCT>) -> Self {
        ProtoVoteMessage {
            vote: Some((&value.vote).into()),
            sig: Some(certificate_signature_to_proto(&value.sig)),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoVoteMessage> for VoteMessage<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoVoteMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            vote: value
                .vote
                .ok_or(ProtoError::MissingRequiredField("VoteMsg.vote".to_owned()))?
                .try_into()?,
            sig: proto_to_certificate_signature(
                value
                    .sig
                    .ok_or(ProtoError::MissingRequiredField("VoteMsg.sig".to_owned()))?,
            )?,
        })
    }
}

impl<SCT: SignatureCollection> From<&TimeoutMessage<SCT>> for ProtoTimeoutMessage {
    fn from(value: &TimeoutMessage<SCT>) -> Self {
        ProtoTimeoutMessage {
            timeout: Some((&value.timeout).into()),
            sig: Some(certificate_signature_to_proto(&value.sig)),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoTimeoutMessage> for TimeoutMessage<SCT> {
    type Error = ProtoError;
    fn try_from(value: ProtoTimeoutMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            timeout: value
                .timeout
                .ok_or(ProtoError::MissingRequiredField(
                    "TimeoutMessage.timeout".to_owned(),
                ))?
                .try_into()?,
            sig: proto_to_certificate_signature(value.sig.ok_or(
                ProtoError::MissingRequiredField("TimeoutMessage.sig".to_owned()),
            )?)?,
        })
    }
}

impl<SCT: SignatureCollection> From<&ProposalMessage<SCT>> for ProtoProposalMessage {
    fn from(value: &ProposalMessage<SCT>) -> Self {
        Self {
            block: Some((&value.block).into()),
            last_round_tc: value.last_round_tc.as_ref().map(|v| v.into()),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoProposalMessage> for ProposalMessage<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoProposalMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            block: value
                .block
                .ok_or(Self::Error::MissingRequiredField(
                    "ProposalMessage<AggregateSignatures>.block".to_owned(),
                ))?
                .try_into()?,
            last_round_tc: value.last_round_tc.map(|v| v.try_into()).transpose()?,
        })
    }
}

impl From<&Validated<RequestBlockSyncMessage>> for ProtoRequestBlockSyncMessage {
    fn from(value: &Validated<RequestBlockSyncMessage>) -> Self {
        ProtoRequestBlockSyncMessage {
            block_id: Some((&value.block_id).into()),
        }
    }
}

impl TryFrom<ProtoRequestBlockSyncMessage> for Unvalidated<RequestBlockSyncMessage> {
    type Error = ProtoError;

    fn try_from(value: ProtoRequestBlockSyncMessage) -> Result<Self, Self::Error> {
        Ok(Unvalidated::new(RequestBlockSyncMessage {
            block_id: value
                .block_id
                .ok_or(Self::Error::MissingRequiredField(
                    "RequestBlockSyncMessage.block_id".to_owned(),
                ))?
                .try_into()?,
        }))
    }
}

impl<SCT: SignatureCollection> From<&Validated<BlockSyncResponseMessage<SCT>>>
    for ProtoBlockSyncMessage
{
    fn from(value: &Validated<BlockSyncResponseMessage<SCT>>) -> Self {
        Self {
            oneof_message: Some(match value.deref() {
                BlockSyncResponseMessage::BlockFound(b) => {
                    proto_block_sync_message::OneofMessage::BlockFound(b.into())
                }
                BlockSyncResponseMessage::NotAvailable(bid) => {
                    proto_block_sync_message::OneofMessage::NotAvailable(bid.into())
                }
            }),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoBlockSyncMessage>
    for Unvalidated<BlockSyncResponseMessage<SCT>>
{
    type Error = ProtoError;

    fn try_from(value: ProtoBlockSyncMessage) -> Result<Self, Self::Error> {
        let msg = match value.oneof_message {
            Some(proto_block_sync_message::OneofMessage::BlockFound(b)) => {
                BlockSyncResponseMessage::BlockFound(b.try_into()?)
            }
            Some(proto_block_sync_message::OneofMessage::NotAvailable(bid)) => {
                BlockSyncResponseMessage::NotAvailable(bid.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "BlockSyncMessage.oneofmessage".to_owned(),
            ))?,
        };

        Ok(Unvalidated::new(msg))
    }
}

impl From<&Validated<CascadeTxMessage>> for ProtoCascadeTxMessage {
    fn from(value: &Validated<CascadeTxMessage>) -> Self {
        ProtoCascadeTxMessage {
            txns: value.txns.clone(),
        }
    }
}

impl TryFrom<ProtoCascadeTxMessage> for Unvalidated<CascadeTxMessage> {
    type Error = ProtoError;

    fn try_from(value: ProtoCascadeTxMessage) -> Result<Self, Self::Error> {
        Ok(Unvalidated::new(CascadeTxMessage { txns: value.txns }))
    }
}

impl<MS: CertificateSignatureRecoverable, SCT: SignatureCollection>
    From<&VerifiedConsensusMessage<MS, SCT>> for ProtoUnverifiedConsensusMessage
{
    fn from(value: &VerifiedConsensusMessage<MS, SCT>) -> Self {
        let oneof_message = match value.deref().deref() {
            ConsensusMessage::Proposal(msg) => {
                proto_unverified_consensus_message::OneofMessage::Proposal(msg.into())
            }
            ConsensusMessage::Vote(msg) => {
                proto_unverified_consensus_message::OneofMessage::Vote(msg.into())
            }
            ConsensusMessage::Timeout(msg) => {
                proto_unverified_consensus_message::OneofMessage::Timeout(msg.into())
            }
        };
        Self {
            oneof_message: Some(oneof_message),
            author_signature: Some(certificate_signature_to_proto(value.author_signature())),
        }
    }
}

impl<MS: CertificateSignatureRecoverable, SCT: SignatureCollection>
    TryFrom<ProtoUnverifiedConsensusMessage> for UnverifiedConsensusMessage<MS, SCT>
{
    type Error = ProtoError;

    fn try_from(value: ProtoUnverifiedConsensusMessage) -> Result<Self, Self::Error> {
        let message = match value.oneof_message {
            Some(proto_unverified_consensus_message::OneofMessage::Proposal(msg)) => {
                ConsensusMessage::Proposal(msg.try_into()?)
            }
            Some(proto_unverified_consensus_message::OneofMessage::Timeout(msg)) => {
                ConsensusMessage::Timeout(msg.try_into()?)
            }
            Some(proto_unverified_consensus_message::OneofMessage::Vote(msg)) => {
                ConsensusMessage::Vote(msg.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "Unverified<ConsensusMessage>.oneofmessage".to_owned(),
            ))?,
        };
        let signature = proto_to_certificate_signature(value.author_signature.ok_or(
            Self::Error::MissingRequiredField("Unverified<ConsensusMessage>.signature".to_owned()),
        )?)?;
        Ok(Unverified::new(Unvalidated::new(message), signature))
    }
}

// TODO-2: PeerStateRootMessage doesn't belong to monad-consensus. Create a new
// crate for it?
impl<P: PubKey> From<&Validated<PeerStateRootMessage<P>>> for ProtoPeerStateRootMessage {
    fn from(value: &Validated<PeerStateRootMessage<P>>) -> Self {
        let msg = value.deref();
        Self {
            peer: Some((&msg.peer).into()),
            seq_num: Some((&msg.seq_num).into()),
            round: Some((&msg.round).into()),
            state_root: Some((&msg.state_root).into()),
        }
    }
}

impl<P: PubKey> TryFrom<ProtoPeerStateRootMessage> for Unvalidated<PeerStateRootMessage<P>> {
    type Error = ProtoError;

    fn try_from(value: ProtoPeerStateRootMessage) -> Result<Self, Self::Error> {
        let msg = PeerStateRootMessage {
            peer: value
                .peer
                .ok_or(ProtoError::MissingRequiredField(
                    "PeerStateRootMessage.peer".to_owned(),
                ))?
                .try_into()?,
            seq_num: value
                .seq_num
                .ok_or(ProtoError::MissingRequiredField(
                    "PeerStateRootMessage.seq_num".to_owned(),
                ))?
                .try_into()?,
            round: value
                .round
                .ok_or(ProtoError::MissingRequiredField(
                    "PeerStateRootMessage.round".to_owned(),
                ))?
                .try_into()?,
            state_root: value
                .state_root
                .ok_or(ProtoError::MissingRequiredField(
                    "PeerStateRootMessage.state_root_hash".to_owned(),
                ))?
                .try_into()?,
        };

        Ok(Unvalidated::new(msg))
    }
}
