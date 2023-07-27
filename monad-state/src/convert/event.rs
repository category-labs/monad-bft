use monad_consensus::pacemaker::PacemakerTimerExpire;
use monad_consensus_types::{
    block::{FullTransactionList, TransactionList},
    multi_sig::MultiSig,
};
use monad_crypto::Signature;
use monad_proto::{
    error::ProtoError,
    proto::{event::*, pacemaker::ProtoPacemakerTimerExpire},
};

use crate::{
    ConsensusEvent as TypeConsensusEvent, FetchedFullTxs, FetchedTxs, MonadEvent as TypeMonadEvent,
};

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

                    tx_hashes: fetched.txns.0.clone(),
                })
            }
            TypeConsensusEvent::FetchedFullTxs(fetched_full) => {
                proto_consensus_event::Event::FetchedFullTxs(ProtoFetchedFullTxs {
                    author: Some((&fetched_full.author).into()),
                    p: Some((&fetched_full.p).into()),
                    full_txs: fetched_full
                        .txns
                        .as_ref()
                        .map(|txns| txns.0.clone())
                        .unwrap_or_default(),
                })
            }
            TypeConsensusEvent::LoadEpoch(epoch, valset, upcoming_valset) => {
                proto_consensus_event::Event::LoadEpoch(ProtoLoadEpochEvent {
                    epoch: Some(epoch.into()),
                    validator_set: Some(valset.into()),
                    upcoming_validator_set: Some(upcoming_valset.into()),
                })
            }
            TypeConsensusEvent::AdvanceEpoch(validator_set) => {
                proto_consensus_event::Event::AdvanceEpoch(ProtoAdvanceEpochEvent {
                    validator_set: validator_set.as_ref().map(|x| x.into()),
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
                    txns: TransactionList(fetched_txs.tx_hashes),
                })
            }
            Some(proto_consensus_event::Event::FetchedFullTxs(fetched_full_txs)) => {
                ConsensusEvent::FetchedFullTxs(FetchedFullTxs {
                    author: fetched_full_txs
                        .author
                        .ok_or(ProtoError::MissingRequiredField(
                            "ConsensusEvent::fetched_full_txs.author".to_owned(),
                        ))?
                        .try_into()?,
                    p: fetched_full_txs
                        .p
                        .ok_or(ProtoError::MissingRequiredField(
                            "ConsensusEvent::fetched_full_txs.p".to_owned(),
                        ))?
                        .try_into()?,
                    txns: Some(FullTransactionList(fetched_full_txs.full_txs)),
                })
            }
            Some(proto_consensus_event::Event::LoadEpoch(epoch_event)) => {
                let e = epoch_event
                    .epoch
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::LoadEpoch::epoch".to_owned(),
                    ))?
                    .try_into()?;
                let vset = epoch_event
                    .validator_set
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::LoadEpoch::validator_set".to_owned(),
                    ))?
                    .try_into()?;
                let uvset = epoch_event
                    .upcoming_validator_set
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::LoadEpoch::upcoming_validator_set".to_owned(),
                    ))?
                    .try_into()?;
                ConsensusEvent::LoadEpoch(e, vset, uvset)
            }
            Some(proto_consensus_event::Event::AdvanceEpoch(epoch_event)) => {
                match epoch_event.validator_set {
                    None => ConsensusEvent::AdvanceEpoch(None),
                    Some(vs) => {
                        let a = vs.try_into()?;
                        ConsensusEvent::AdvanceEpoch(Some(a))
                    }
                }
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
