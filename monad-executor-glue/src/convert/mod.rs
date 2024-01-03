use monad_consensus_types::{
    message_signature::MessageSignature,
    payload::{FullTransactionList, TransactionHashList},
    signature_collection::{SignatureCollection, SignatureCollectionPubKeyType},
    validator_data::ValidatorData,
};
use monad_proto::{
    error::ProtoError,
    proto::{basic::ProtoPubkey, event::*, validator_set::ProtoValidatorSetData},
};
use monad_types::TimeoutVariant;

use crate::{ConsensusEvent, FetchFullTxParams, FetchTxParams};

pub mod event;
pub mod interface;

impl<S: MessageSignature, SCT: SignatureCollection> From<&ConsensusEvent<S, SCT>>
    for ProtoConsensusEvent
where
    for<'a> &'a SignatureCollectionPubKeyType<SCT>: Into<ProtoPubkey>,
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
            ConsensusEvent::Timeout(tmo_event) => match tmo_event {
                TimeoutVariant::Pacemaker => {
                    proto_consensus_event::Event::Timeout(ProtoScheduleTimeout {
                        event: Some(proto_schedule_timeout::Event::Pacemaker(
                            ProtoPaceMakerTimeout {},
                        )),
                    })
                }
                TimeoutVariant::BlockSync(bid) => {
                    proto_consensus_event::Event::Timeout(ProtoScheduleTimeout {
                        event: Some(proto_schedule_timeout::Event::BlockSync(
                            ProtoBlockSyncTimeout {
                                block_id: Some((bid).into()),
                            },
                        )),
                    })
                }
            },
            ConsensusEvent::FetchedTxs(fetched, txns) => {
                proto_consensus_event::Event::FetchedTxs(ProtoFetchedTxs {
                    node_id: Some((&fetched.node_id).into()),
                    round: Some((&fetched.round).into()),
                    high_qc: Some((&fetched.high_qc).into()),
                    last_round_tc: fetched.last_round_tc.as_ref().map(Into::into),

                    tx_hashes: txns.bytes().clone(),
                    seq_num: Some((&fetched.seq_num).into()),
                    state_root_hash: Some((&fetched.state_root_hash).into()),
                })
            }
            ConsensusEvent::FetchedFullTxs(fetched_full, txns) => {
                proto_consensus_event::Event::FetchedFullTxs(ProtoFetchedFullTxs {
                    author: Some((&fetched_full.author).into()),
                    p_block: Some((&fetched_full.p_block).into()),
                    p_last_round_tc: fetched_full.p_last_round_tc.as_ref().map(Into::into),
                    full_txs: txns
                        .as_ref()
                        .map(|txns| txns.bytes().clone())
                        .unwrap_or_default(),
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
                    seq_num: Some(seq_num.into()),
                    state_root_hash: Some(hash.into()),
                })
            }
            ConsensusEvent::BlockSyncResponse {
                sender,
                unvalidated_response,
            } => proto_consensus_event::Event::BlockSyncResp(ProtoBlockSyncResponseWithSender {
                sender: Some(sender.into()),
                response: Some(unvalidated_response.into()),
            }),
        };
        Self { event: Some(event) }
    }
}

impl<S: MessageSignature, SCT: SignatureCollection> TryFrom<ProtoConsensusEvent>
    for ConsensusEvent<S, SCT>
where
    ValidatorData<SCT>: TryFrom<ProtoValidatorSetData, Error = ProtoError>,
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
            Some(proto_consensus_event::Event::Timeout(tmo)) => {
                ConsensusEvent::Timeout(match tmo.event {
                    Some(proto_schedule_timeout::Event::Pacemaker(_)) => TimeoutVariant::Pacemaker,
                    Some(proto_schedule_timeout::Event::BlockSync(bsync_tmo)) => {
                        TimeoutVariant::BlockSync(
                            bsync_tmo
                                .block_id
                                .ok_or(ProtoError::MissingRequiredField(
                                    "ConsensusEvent::Timeout.bsync_tmo.block_id".to_owned(),
                                ))?
                                .try_into()?,
                        )
                    }
                    None => Err(ProtoError::MissingRequiredField(
                        "ConsensusEvent.event".to_owned(),
                    ))?,
                })
            }
            Some(proto_consensus_event::Event::FetchedTxs(fetched_txs)) => {
                ConsensusEvent::FetchedTxs(
                    FetchTxParams {
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
                        seq_num: fetched_txs
                            .seq_num
                            .ok_or(ProtoError::MissingRequiredField(
                                "ConsensusEvent::fetched_txs.seq_num".to_owned(),
                            ))?
                            .try_into()?,
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
                    },
                    TransactionHashList::new(fetched_txs.tx_hashes),
                )
            }
            Some(proto_consensus_event::Event::FetchedFullTxs(fetched_full_txs)) => {
                ConsensusEvent::FetchedFullTxs(
                    FetchFullTxParams {
                        author: fetched_full_txs
                            .author
                            .ok_or(ProtoError::MissingRequiredField(
                                "ConsensusEvent::fetched_full_txs.author".to_owned(),
                            ))?
                            .try_into()?,
                        p_block: fetched_full_txs
                            .p_block
                            .ok_or(ProtoError::MissingRequiredField(
                                "ConsensusEvent::fetched_full_txs.p_block".to_owned(),
                            ))?
                            .try_into()?,
                        p_last_round_tc: fetched_full_txs
                            .p_last_round_tc
                            .map(TryInto::try_into)
                            .transpose()?,
                    },
                    Some(FullTransactionList::new(fetched_full_txs.full_txs)),
                )
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
                let s = event
                    .seq_num
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::StateUpdate::seq_num".to_owned(),
                    ))?
                    .try_into()?;

                ConsensusEvent::StateUpdate((s, h))
            }
            Some(proto_consensus_event::Event::BlockSyncResp(event)) => {
                let sender = event
                    .sender
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::BlockSyncResp::sender".to_owned(),
                    ))?
                    .try_into()?;

                let unvalidated_response = event
                    .response
                    .ok_or(ProtoError::MissingRequiredField(
                        "ConsensusEvent::BlockSyncResp::response".to_owned(),
                    ))?
                    .try_into()?;

                ConsensusEvent::BlockSyncResponse {
                    sender,
                    unvalidated_response,
                }
            }

            None => Err(ProtoError::MissingRequiredField(
                "ConsensusEvent.event".to_owned(),
            ))?,
        };
        Ok(event)
    }
}
