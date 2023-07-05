use monad_consensus::pacemaker::PacemakerTimerExpire;
use monad_consensus::signatures::multi_sig::MultiSig;
use monad_crypto::Signature;
use monad_proto::error::ProtoError;
use monad_proto::proto::event::*;
use monad_proto::proto::pacemaker::ProtoPacemakerTimerExpire;

use crate::ConsensusEvent as TypeConsensusEvent;
use crate::FetchedTxs;
use crate::MonadEvent as TypeMonadEvent;

pub(super) type MonadEvent<S> = TypeMonadEvent<S, MultiSig<S>>;
pub(super) type ConsensusEvent<S> = TypeConsensusEvent<S, MultiSig<S>>;

impl<S: Signature> From<&ConsensusEvent<S>> for ProtoConsensusEvent {
    fn from(value: &ConsensusEvent<S>) -> Self {
        let event = match value {
            TypeConsensusEvent::Message {
                sender,
                unverified_message,
            } => proto_consensus_event::Event::Message(ProtoMessageWithSender {
                sender: Some(sender.into()),
                unverified_message: Some(unverified_message.into()),
            }),
            TypeConsensusEvent::Timeout(_tmo) => {
                proto_consensus_event::Event::Timeout(ProtoPacemakerTimerExpire {})
            }
            TypeConsensusEvent::FetchedTxs(fetched) => {
                proto_consensus_event::Event::FetchedTxs(ProtoFetchedTxs {
                    node_id: Some((&fetched.node_id).into()),
                    round: Some((&fetched.round).into()),
                    high_qc: Some((&fetched.high_qc).into()),
                    last_round_tc: fetched.last_round_tc.as_ref().map(Into::into),

                    txns: Some((&fetched.txns).into()),
                })
            }
        };
        Self { event: Some(event) }
    }
}

impl<S: Signature> TryFrom<ProtoConsensusEvent> for ConsensusEvent<S> {
    type Error = ProtoError;

    fn try_from(value: ProtoConsensusEvent) -> Result<Self, Self::Error> {
        let event = match value.event {
            Some(proto_consensus_event::Event::Message(msg)) => ConsensusEvent::Message {
                sender: msg
                    .sender
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::message.sender".to_owned(),
                    ))?
                    .try_into()?,
                unverified_message: msg
                    .unverified_message
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::message.unverified_message".to_owned(),
                    ))?
                    .try_into()?,
            },
            Some(proto_consensus_event::Event::Timeout(_msg)) => {
                ConsensusEvent::Timeout(PacemakerTimerExpire)
            }
            Some(proto_consensus_event::Event::FetchedTxs(fetched_txs)) => {
                ConsensusEvent::FetchedTxs(FetchedTxs {
                    node_id: fetched_txs
                        .node_id
                        .ok_or(ProtoError::MissingRequiredField(
                            "ConsensusEvent::fetched_txs.node_id".to_owned(),
                        ))?
                        .try_into()?,
                    round: fetched_txs
                        .round
                        .ok_or(ProtoError::MissingRequiredField(
                            "ConsensusEvent::fetched_txs.round".to_owned(),
                        ))?
                        .try_into()?,
                    high_qc: fetched_txs
                        .high_qc
                        .ok_or(ProtoError::MissingRequiredField(
                            "ConsensusEvent::fetched_txs.high_qc".to_owned(),
                        ))?
                        .try_into()?,
                    last_round_tc: fetched_txs
                        .last_round_tc
                        .map(TryInto::try_into)
                        .transpose()?,
                    txns: fetched_txs
                        .txns
                        .ok_or(ProtoError::MissingRequiredField(
                            "ConsensusEvent::fetched_txs.txns".to_owned(),
                        ))?
                        .try_into()?,
                })
            }
            None => Err(ProtoError::MissingRequiredField(
                "ConsensusEvent.event".to_owned(),
            ))?,
        };
        Ok(event)
    }
}

impl<S: Signature> From<&MonadEvent<S>> for ProtoMonadEvent {
    fn from(value: &MonadEvent<S>) -> Self {
        let event = match value {
            TypeMonadEvent::ConsensusEvent(msg) => {
                proto_monad_event::Event::ConsensusEvent(msg.into())
            }
        };
        Self { event: Some(event) }
    }
}

impl<S: Signature> TryFrom<ProtoMonadEvent> for MonadEvent<S> {
    type Error = ProtoError;
    fn try_from(value: ProtoMonadEvent) -> Result<Self, Self::Error> {
        let event: MonadEvent<S> = match value.event {
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
