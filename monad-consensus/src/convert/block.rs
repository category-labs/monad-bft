use monad_proto::error::ProtoError;
use monad_proto::proto::block::*;

use crate::{
    types::block::{Block, TransactionList},
    validation::hashing::Sha256Hash,
};

use super::signing::AggSecpSignature;

impl From<&TransactionList> for ProtoTransactionList {
    fn from(value: &TransactionList) -> Self {
        Self {
            data: value.0.clone(),
        }
    }
}

impl TryFrom<ProtoTransactionList> for TransactionList {
    type Error = ProtoError;
    fn try_from(value: ProtoTransactionList) -> Result<Self, Self::Error> {
        Ok(Self(value.data))
    }
}

impl From<&Block<AggSecpSignature>> for ProtoBlockAggSig {
    fn from(value: &Block<AggSecpSignature>) -> Self {
        Self {
            author: Some((&value.author).into()),
            round: Some((&value.round).into()),
            payload: Some((&value.payload).into()),
            qc: Some((&value.qc).into()),
        }
    }
}

impl TryFrom<ProtoBlockAggSig> for Block<AggSecpSignature> {
    type Error = ProtoError;

    fn try_from(value: ProtoBlockAggSig) -> Result<Self, Self::Error> {
        // The hasher is hard-coded to be Sha256Hash
        Ok(Block::<AggSecpSignature>::new::<Sha256Hash>(
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
