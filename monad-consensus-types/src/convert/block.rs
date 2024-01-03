use monad_eth_types::EthAddress;
use monad_proto::{
    error::ProtoError,
    proto::{
        basic::{ProtoBloom, ProtoGas},
        block::*,
    },
};

use crate::{
    block::{Block, UnverifiedBlock},
    payload::{
        Bloom, ExecutionArtifacts, FullTransactionList, Gas, Payload, RandaoReveal,
        TransactionHashList,
    },
    signature_collection::SignatureCollection,
};

impl From<&TransactionHashList> for ProtoTransactionList {
    fn from(value: &TransactionHashList) -> Self {
        Self {
            data: value.bytes().clone(),
        }
    }
}

impl TryFrom<ProtoTransactionList> for TransactionHashList {
    type Error = ProtoError;
    fn try_from(value: ProtoTransactionList) -> Result<TransactionHashList, Self::Error> {
        Ok(Self::new(value.data))
    }
}

impl<SCT: SignatureCollection> From<&Block<SCT>> for ProtoBlock {
    fn from(value: &Block<SCT>) -> Self {
        Self {
            author: Some((&value.author).into()),
            round: Some((&value.round).into()),
            payload: Some((&value.payload).into()),
            qc: Some((&value.qc).into()),
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
            value
                .round
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.round".to_owned(),
                ))?
                .try_into()?,
            &value
                .payload
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.payload".to_owned(),
                ))?
                .try_into()?,
            &value
                .qc
                .ok_or(Self::Error::MissingRequiredField(
                    "Block<AggregateSignatures>.qc".to_owned(),
                ))?
                .try_into()?,
        ))
    }
}

impl<SCT: SignatureCollection> From<&UnverifiedBlock<SCT>> for ProtoUnverifiedBlock {
    fn from(value: &UnverifiedBlock<SCT>) -> Self {
        Self {
            block: Some((&value.0).into()),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoUnverifiedBlock> for UnverifiedBlock<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoUnverifiedBlock) -> Result<Self, Self::Error> {
        Ok(Self(
            value
                .block
                .ok_or(Self::Error::MissingRequiredField(
                    "UnverifiedBlock<AggregateSignatures>.block".to_owned(),
                ))?
                .try_into()?,
        ))
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

impl From<&ExecutionArtifacts> for ProtoExecutionArtifacts {
    fn from(value: &ExecutionArtifacts) -> Self {
        Self {
            parent_hash: Some((&(value.parent_hash)).into()),
            state_root: Some((&(value.state_root)).into()),
            transactions_root: Some((&(value.transactions_root)).into()),
            receipts_root: Some((&(value.receipts_root)).into()),
            logs_bloom: Some((&(value.logs_bloom)).into()),
            gas_used: Some((&(value.gas_used)).into()),
        }
    }
}

impl TryFrom<ProtoExecutionArtifacts> for ExecutionArtifacts {
    type Error = ProtoError;
    fn try_from(value: ProtoExecutionArtifacts) -> Result<Self, Self::Error> {
        Ok(Self {
            parent_hash: value
                .parent_hash
                .ok_or(Self::Error::MissingRequiredField(
                    "ExecutionArtifacts.parent_hash".to_owned(),
                ))?
                .try_into()?,
            state_root: value
                .state_root
                .ok_or(Self::Error::MissingRequiredField(
                    "ExecutionArtifacts.state_root".to_owned(),
                ))?
                .try_into()?,
            transactions_root: value
                .transactions_root
                .ok_or(Self::Error::MissingRequiredField(
                    "ExecutionArtifacts.transactions_root".to_owned(),
                ))?
                .try_into()?,
            receipts_root: value
                .receipts_root
                .ok_or(Self::Error::MissingRequiredField(
                    "ExecutionArtifacts.receipts_root".to_owned(),
                ))?
                .try_into()?,
            logs_bloom: value
                .logs_bloom
                .ok_or(Self::Error::MissingRequiredField(
                    "ExecutionArtifacts.logs_bloom".to_owned(),
                ))?
                .try_into()?,
            gas_used: value
                .gas_used
                .ok_or(Self::Error::MissingRequiredField(
                    "ExecutionArtifacts.gas_used".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

impl From<&Payload> for ProtoPayload {
    fn from(value: &Payload) -> Self {
        ProtoPayload {
            txns: value.txns.bytes().clone(),
            header: Some((&(value.header)).into()),
            seq_num: Some((&(value.seq_num)).into()),
            beneficiary: value.beneficiary.0.to_vec().into(),
            randao_reveal: value.randao_reveal.0.to_vec().into(),
        }
    }
}

impl TryFrom<ProtoPayload> for Payload {
    type Error = ProtoError;
    fn try_from(value: ProtoPayload) -> Result<Self, Self::Error> {
        Ok(Self {
            txns: FullTransactionList::new(value.txns),
            header: value
                .header
                .ok_or(Self::Error::MissingRequiredField(
                    "Payload.header".to_owned(),
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
