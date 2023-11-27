use monad_crypto::hasher::HasherType;
use monad_eth_types::EthAddress;
use monad_proto::{
    error::ProtoError,
    proto::{
        basic::{ProtoBloom, ProtoGas},
        block::*,
    },
};

use crate::{
    block::{Block, FullBlock, UnverifiedFullBlock},
    payload::{
        Bloom, ExecutionArtifacts, FullTransactionList, Gas, Payload, RandaoReveal,
        TransactionHashList,
    },
    signature_collection::SignatureCollection,
};

impl From<&TransactionHashList> for ProtoTransactionList {
    fn from(value: &TransactionHashList) -> Self {
        Self {
            data: value.as_bytes().to_vec(),
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
        Ok(Self::new::<HasherType>(
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

impl<SCT: SignatureCollection> From<&UnverifiedFullBlock<SCT>> for ProtoUnverifiedFullBlock {
    fn from(value: &UnverifiedFullBlock<SCT>) -> Self {
        Self {
            block: Some((&value.block).into()),
            full_txs: value.full_txs.as_bytes().to_vec(),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoUnverifiedFullBlock> for UnverifiedFullBlock<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoUnverifiedFullBlock) -> Result<Self, Self::Error> {
        Ok(Self {
            block: value
                .block
                .ok_or(Self::Error::MissingRequiredField(
                    "UnverifiedFullBlock<AggregateSignatures>.block".to_owned(),
                ))?
                .try_into()?,
            full_txs: FullTransactionList::new(value.full_txs),
        })
    }
}

impl<SCT: SignatureCollection> From<&FullBlock<SCT>> for ProtoUnverifiedFullBlock {
    fn from(value: &FullBlock<SCT>) -> Self {
        Self {
            block: Some(value.get_block().into()),
            full_txs: value.get_full_txs().as_bytes().to_vec(),
        }
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
            bloom: value.0.to_vec(),
        }
    }
}

impl TryFrom<ProtoBloom> for Bloom {
    type Error = ProtoError;
    fn try_from(value: ProtoBloom) -> Result<Self, Self::Error> {
        Ok(Self(value.bloom.try_into().map_err(|e: Vec<_>| {
            Self::Error::WrongHashLen(format!("{}", e.len()))
        })?))
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
            txns: Some((&(value.txns)).into()),
            header: Some((&(value.header)).into()),
            seq_num: Some((&(value.seq_num)).into()),
            beneficiary: value.beneficiary.0.to_vec(),
            randao_reveal: value.randao_reveal.0.clone(),
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
                    .try_into()
                    .map_err(|_| Self::Error::WrongHashLen("Payload.beneficiary".to_owned()))?,
            ),
            randao_reveal: RandaoReveal(value.randao_reveal),
        })
    }
}
