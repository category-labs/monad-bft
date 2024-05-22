use monad_proto::{error::ProtoError, proto::timeout::*};

use super::signing::{proto_to_signature_collection, signature_collection_to_proto};
use crate::{
    signature_collection::SignatureCollection,
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
};

impl From<&HighQcRound> for ProtoHighQcRound {
    fn from(value: &HighQcRound) -> Self {
        ProtoHighQcRound {
            qc_round: Some((&value.qc_round).into()),
        }
    }
}

impl TryFrom<ProtoHighQcRound> for HighQcRound {
    type Error = ProtoError;
    fn try_from(value: ProtoHighQcRound) -> Result<Self, Self::Error> {
        Ok(Self {
            qc_round: value
                .qc_round
                .ok_or(Self::Error::MissingRequiredField(
                    "HighQcRound.round".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

impl<SCT: SignatureCollection> From<&HighQcRoundSigColTuple<SCT>> for ProtoHighQcRoundSigColTuple {
    fn from(value: &HighQcRoundSigColTuple<SCT>) -> Self {
        Self {
            high_qc_round: Some((&value.high_qc_round).into()),
            sigs: Some(signature_collection_to_proto(&value.sigs)),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoHighQcRoundSigColTuple>
    for HighQcRoundSigColTuple<SCT>
{
    type Error = ProtoError;
    fn try_from(value: ProtoHighQcRoundSigColTuple) -> Result<Self, Self::Error> {
        Ok(Self {
            high_qc_round: value
                .high_qc_round
                .ok_or(Self::Error::MissingRequiredField(
                    "HighQcRoundSigColTuple.high_qc_round".to_owned(),
                ))?
                .try_into()?,
            sigs: proto_to_signature_collection(value.sigs.ok_or(
                Self::Error::MissingRequiredField("HighQcRoundSigColTuple.sigs".to_owned()),
            )?)?,
        })
    }
}
impl<SCT: SignatureCollection> From<&TimeoutCertificate<SCT>> for ProtoTimeoutCertificate {
    fn from(value: &TimeoutCertificate<SCT>) -> Self {
        Self {
            epoch: Some((&value.epoch).into()),
            round: Some((&value.round).into()),
            high_qc_rounds: value
                .high_qc_rounds
                .iter()
                .map(|v| v.into())
                .collect::<Vec<_>>(),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoTimeoutCertificate> for TimeoutCertificate<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoTimeoutCertificate) -> Result<Self, Self::Error> {
        Ok(Self {
            epoch: value
                .epoch
                .ok_or(Self::Error::MissingRequiredField(
                    "TimeoutCertificate.epoch".to_owned(),
                ))?
                .try_into()?,
            round: value
                .round
                .ok_or(Self::Error::MissingRequiredField(
                    "TimeoutCertificate.round".to_owned(),
                ))?
                .try_into()?,
            high_qc_rounds: value
                .high_qc_rounds
                .into_iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl<SCT: SignatureCollection> From<&TimeoutInfo<SCT>> for ProtoTimeoutInfo {
    fn from(value: &TimeoutInfo<SCT>) -> Self {
        Self {
            epoch: Some((&value.epoch).into()),
            round: Some((&value.round).into()),
            high_qc: Some((&value.high_qc).into()),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoTimeoutInfo> for TimeoutInfo<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoTimeoutInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            epoch: value
                .epoch
                .ok_or(Self::Error::MissingRequiredField(
                    "TimeoutInfo<AggregateSignatures>.epoch".to_owned(),
                ))?
                .try_into()?,
            round: value
                .round
                .ok_or(Self::Error::MissingRequiredField(
                    "TimeoutInfo<AggregateSignatures>.round".to_owned(),
                ))?
                .try_into()?,
            high_qc: value
                .high_qc
                .ok_or(Self::Error::MissingRequiredField(
                    "TimeoutInfo<AggregateSignatures>.high_qc".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

impl<SCT: SignatureCollection> From<&Timeout<SCT>> for ProtoTimeout {
    fn from(value: &Timeout<SCT>) -> Self {
        Self {
            tminfo: Some((&value.tminfo).into()),
            last_round_tc: value.last_round_tc.as_ref().map(|v| v.into()),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoTimeout> for Timeout<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoTimeout) -> Result<Self, Self::Error> {
        Ok(Self {
            tminfo: value
                .tminfo
                .ok_or(ProtoError::MissingRequiredField(
                    "Timeout.tminfo".to_owned(),
                ))?
                .try_into()?,
            last_round_tc: value.last_round_tc.map(|v| v.try_into()).transpose()?,
        })
    }
}
