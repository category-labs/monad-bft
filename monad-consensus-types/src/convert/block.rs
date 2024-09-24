use monad_eth_types::EthAddress;
use monad_proto::{
    error::ProtoError,
    proto::{
        basic::{ProtoBloom, ProtoGas, ProtoPayloadId},
        block::*,
    },
};

use crate::{
    block::{Block, BlockKind, FullBlock},
    payload::{
        Bloom, ExecutionProtocol, FullTransactionList, Gas, Payload, PayloadId, RandaoReveal,
        TransactionPayload,
    },
    signature_collection::SignatureCollection,
};

impl From<&BlockKind> for i32 {
    fn from(kind: &BlockKind) -> Self {
        match kind {
            BlockKind::Executable => 0,
            BlockKind::Null => 1,
        }
    }
}

impl TryFrom<i32> for BlockKind {
    type Error = ProtoError;
    fn try_from(kind: i32) -> Result<Self, Self::Error> {
        match kind {
            0 => Ok(BlockKind::Executable),
            1 => Ok(BlockKind::Null),
            _ => Err(ProtoError::DeserializeError(
                "unknown block kind".to_owned(),
            )),
        }
    }
}

impl From<&PayloadId> for ProtoPayloadId {
    fn from(value: &PayloadId) -> Self {
        Self {
            pid: Some((&(value.0)).into()),
        }
    }
}

impl TryFrom<ProtoPayloadId> for PayloadId {
    type Error = ProtoError;

    fn try_from(value: ProtoPayloadId) -> Result<Self, Self::Error> {
        Ok(Self(
            value
                .pid
                .ok_or(Self::Error::MissingRequiredField(
                    "ProtoPayloadId.pid".to_owned(),
                ))?
                .try_into()?,
        ))
    }
}

impl<SCT: SignatureCollection> From<&Block<SCT>> for ProtoBlock {
    fn from(value: &Block<SCT>) -> Self {
        Self {
            author: Some((&value.author).into()),
            epoch: Some((&value.epoch).into()),
            round: Some((&value.round).into()),
            execution: Some((&value.execution).into()),
            payload_id: Some((&value.payload_id).into()),
            block_kind: (&value.block_kind).into(),
            qc: Some((&value.qc).into()),
            timestamp: value.timestamp,
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoBlock> for Block<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoBlock) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value
                .author
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.author".to_owned(),
                ))?
                .try_into()?,
            value.timestamp,
            value
                .epoch
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.epoch".to_owned(),
                ))?
                .try_into()?,
            value
                .round
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.round".to_owned(),
                ))?
                .try_into()?,
            &value
                .execution
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.execution".to_owned(),
                ))?
                .try_into()?,
            value
                .payload_id
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.payload_id".to_owned(),
                ))?
                .try_into()?,
            value.block_kind.try_into()?,
            &value
                .qc
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.qc".to_owned(),
                ))?
                .try_into()?,
        ))
    }
}

impl<SCT: SignatureCollection> From<&FullBlock<SCT>> for ProtoFullBlock {
    fn from(value: &FullBlock<SCT>) -> Self {
        Self {
            block: Some((&value.block).into()),
            payload: Some((&value.payload).into()),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoFullBlock> for FullBlock<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoFullBlock) -> Result<Self, Self::Error> {
        Ok(Self {
            block: value
                .block
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.block".to_owned(),
                ))?
                .try_into()?,
            payload: value
                .payload
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.payload".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

impl From<&Gas> for ProtoGas {
    fn from(value: &Gas) -> Self {
        Self { gas: value.0 }
    }
}

impl TryFrom<ProtoGas> for Gas {
    type Error = ProtoError;
    fn try_from(value: ProtoGas) -> Result<Self, Self::Error> {
        Ok(Self(value.gas))
    }
}

impl From<&Bloom> for ProtoBloom {
    fn from(value: &Bloom) -> Self {
        Self {
            bloom: value.0.to_vec().into(),
        }
    }
}

impl TryFrom<ProtoBloom> for Bloom {
    type Error = ProtoError;
    fn try_from(value: ProtoBloom) -> Result<Self, Self::Error> {
        Ok(Self(value.bloom.to_vec().try_into().map_err(
            |e: Vec<_>| Self::Error::WrongHashLen(format!("{}", e.len())),
        )?))
    }
}

impl From<&ExecutionProtocol> for ProtoExecutionProtocol {
    fn from(value: &ExecutionProtocol) -> Self {
        Self {
            state_root: Some((&(value.state_root)).into()),
            seq_num: Some((&(value.seq_num)).into()),
            beneficiary: value.beneficiary.0.to_vec().into(),
            randao_reveal: value.randao_reveal.0.to_vec().into(),
        }
    }
}

impl TryFrom<ProtoExecutionProtocol> for ExecutionProtocol {
    type Error = ProtoError;
    fn try_from(value: ProtoExecutionProtocol) -> Result<Self, Self::Error> {
        Ok(Self {
            state_root: value
                .state_root
                .ok_or(Self::Error::MissingRequiredField(
                    "ExecutionProtocol.state_root".to_owned(),
                ))?
                .try_into()?,
            seq_num: value
                .seq_num
                .ok_or(Self::Error::MissingRequiredField(
                    "Payload.seq_num".to_owned(),
                ))?
                .try_into()?,
            beneficiary: EthAddress::from_bytes(
                value
                    .beneficiary
                    .to_vec()
                    .try_into()
                    .map_err(|_| Self::Error::WrongHashLen("Payload.beneficiary".to_owned()))?,
            ),
            randao_reveal: RandaoReveal(value.randao_reveal.to_vec()),
        })
    }
}

impl From<&TransactionPayload> for ProtoTransactionPayload {
    fn from(value: &TransactionPayload) -> Self {
        let txns = Some(match value {
            TransactionPayload::List(txns) => {
                proto_transaction_payload::Txns::List(txns.bytes().clone())
            }
            TransactionPayload::Null => {
                proto_transaction_payload::Txns::Empty(ProtoEmptyBlockTransactionList {})
            }
        });
        Self { txns }
    }
}

impl TryFrom<ProtoTransactionPayload> for TransactionPayload {
    type Error = ProtoError;

    fn try_from(value: ProtoTransactionPayload) -> Result<Self, Self::Error> {
        let txns = value.txns.ok_or(Self::Error::MissingRequiredField(
            "TransactionPayload.txns".to_owned(),
        ))?;
        let txn_payload = match txns {
            proto_transaction_payload::Txns::List(txns) => {
                TransactionPayload::List(FullTransactionList::new(txns))
            }
            proto_transaction_payload::Txns::Empty(_) => TransactionPayload::Null,
        };
        Ok(txn_payload)
    }
}

impl From<&Payload> for ProtoPayload {
    fn from(value: &Payload) -> Self {
        ProtoPayload {
            txns: Some((&(value.txns)).into()),
        }
    }
}

impl TryFrom<ProtoPayload> for Payload {
    type Error = ProtoError;
    fn try_from(value: ProtoPayload) -> Result<Self, Self::Error> {
        Ok(Self {
            txns: value
                .txns
                .ok_or(Self::Error::MissingRequiredField("Payload.txns".to_owned()))?
                .try_into()?,
        })
    }
}
