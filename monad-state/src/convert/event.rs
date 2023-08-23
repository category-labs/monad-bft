use monad_consensus::pacemaker::PacemakerTimerExpire;
use monad_consensus_state::command::FetchedBlock;
use monad_consensus_types::{
    message_signature::MessageSignature,
    payload::{FullTransactionList, TransactionList},
    signature_collection::SignatureCollection,
};
use monad_proto::{
    error::ProtoError,
    proto::{event::*, pacemaker::ProtoPacemakerTimerExpire},
};

use crate::{ConsensusEvent, FetchedFullTxs, FetchedTxs, MonadEvent};

impl<S: MessageSignature, SCT: SignatureCollection> From<&ConsensusEvent<S, SCT>>
    for ProtoConsensusEvent
{
    fn from(value: &ConsensusEvent<S, SCT>) -> Self {
        let event = match value {
            ConsensusEvent::Message {
                sender,
                unverified_message,
            } => proto_consensus_event::Event::Message(ProtoMessageWithSender {
                sender: Some(sender.into()),
                unverified_message: Some(unverified_message.into()),
            }),
            ConsensusEvent::Timeout(_tmo) => {
                proto_consensus_event::Event::Timeout(ProtoPacemakerTimerExpire {})
            }
            ConsensusEvent::FetchedTxs(fetched) => {
                proto_consensus_event::Event::FetchedTxs(ProtoFetchedTxs {
                    node_id: Some((&fetched.node_id).into()),
                    round: Some((&fetched.round).into()),
                    high_qc: Some((&fetched.high_qc).into()),
                    last_round_tc: fetched.last_round_tc.as_ref().map(Into::into),

                    tx_hashes: fetched.txns.0.clone(),
                    seq_num: fetched.seq_num,
                    state_root_hash: Some((&fetched.state_root_hash).into()),
                })
            }
            ConsensusEvent::FetchedFullTxs(fetched_full) => {
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
            ConsensusEvent::FetchedBlock(fetched_block) => {
                proto_consensus_event::Event::FetchedBlock(ProtoFetchedBlock {
                    requester: Some((&fetched_block.requester).into()),
                    block: fetched_block.block.as_ref().map(|b| b.into()),
                })
            }
            ConsensusEvent::LoadEpoch(epoch, valset, upcoming_valset) => {
                proto_consensus_event::Event::LoadEpoch(ProtoLoadEpochEvent {
                    epoch: Some(epoch.into()),
                    validator_set: Some(valset.into()),
                    upcoming_validator_set: Some(upcoming_valset.into()),
                })
            }
            ConsensusEvent::AdvanceEpoch(validator_set) => {
                proto_consensus_event::Event::AdvanceEpoch(ProtoAdvanceEpochEvent {
                    validator_set: validator_set.as_ref().map(|x| x.into()),
                })
            }
            ConsensusEvent::StateUpdate((seq_num, hash)) => {
                proto_consensus_event::Event::StateUpdate(ProtoStateUpdateEvent {
                    seq_num: *seq_num,
                    state_root_hash: Some(hash.into()),
                })
            }
        };
        Self { event: Some(event) }
    }
}

impl<S: MessageSignature, SCT: SignatureCollection> TryFrom<ProtoConsensusEvent>
    for ConsensusEvent<S, SCT>
{
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
                    seq_num: fetched_txs.seq_num,
                    state_root_hash: fetched_txs
                        .state_root_hash
                        .ok_or(ProtoError::MissingRequiredField(
                            "ConsensusEvent::fetched_txs.state_root_hash".to_owned(),
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
            Some(proto_consensus_event::Event::FetchedBlock(fetched_block)) => {
                ConsensusEvent::FetchedBlock(FetchedBlock {
                    requester: fetched_block
                        .requester
                        .ok_or(ProtoError::MissingRequiredField(
                            "ConsensusEvent::fetched_block.requester".to_owned(),
                        ))?
                        .try_into()?,
                    block: Some(
                        fetched_block
                            .block
                            .ok_or(ProtoError::MissingRequiredField(
                                "ConsensusEvent::fetched_block.block".to_owned(),
                            ))?
                            .try_into()?,
                    ),
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
            Some(proto_consensus_event::Event::StateUpdate(event)) => {
                let h = event
                    .state_root_hash
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::StateUpdate::state_root_hash".to_owned(),
                    ))?
                    .try_into()?;
                ConsensusEvent::StateUpdate((event.seq_num, h))
            }
            None => Err(ProtoError::MissingRequiredField(
                "ConsensusEvent.event".to_owned(),
            ))?,
        };
        Ok(event)
    }
}

impl<S: MessageSignature, SCT: SignatureCollection> From<&MonadEvent<S, SCT>> for ProtoMonadEvent {
    fn from(value: &MonadEvent<S, SCT>) -> Self {
        let event = match value {
            MonadEvent::ConsensusEvent(msg) => proto_monad_event::Event::ConsensusEvent(msg.into()),
        };
        Self { event: Some(event) }
    }
}

impl<S: MessageSignature, SCT: SignatureCollection> TryFrom<ProtoMonadEvent>
    for MonadEvent<S, SCT>
{
    type Error = ProtoError;
    fn try_from(value: ProtoMonadEvent) -> Result<Self, Self::Error> {
        let event: MonadEvent<S, SCT> = match value.event {
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
