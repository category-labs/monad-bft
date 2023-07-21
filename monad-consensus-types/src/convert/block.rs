use monad_crypto::{GenericSignature, Signature};
use monad_proto::{error::ProtoError, proto::block::*};

use crate::{
    block::{Block, TransactionList},
    multi_sig::MultiSig,
    validation::{Hashable, Sha256Hash},
};

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

impl<S: Signature + GenericSignature + Hashable> From<&Block<MultiSig<S>>> for ProtoBlockAggSig {
    fn from(value: &Block<MultiSig<S>>) -> Self {
        Self {
            author: Some((&value.author).into()),
            round: Some((&value.round).into()),
            payload: Some((&value.payload).into()),
            qc: Some((&value.qc).into()),
        }
    }
}

impl<S: Signature + GenericSignature + Hashable> TryFrom<ProtoBlockAggSig> for Block<MultiSig<S>> {
    type Error = ProtoError;

    fn try_from(value: ProtoBlockAggSig) -> Result<Self, Self::Error> {
        // The hasher is hard-coded to be Sha256Hash
        Ok(Self::new::<Sha256Hash>(
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
