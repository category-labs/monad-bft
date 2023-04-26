use monad_crypto::secp256k1::SecpSignature;
use monad_proto::error::ProtoError;
use monad_proto::proto::timeout::*;

use crate::types::timeout::{
    HighQcRound, HighQcRoundSigTuple as TypeHighQcRoundSigTuple, TimeoutCertificate as ConsensusTC,
    TimeoutInfo as ConsensusTmoInfo,
};

use super::signing::AggSecpSignature;

type HighQcRoundSigTuple = TypeHighQcRoundSigTuple<SecpSignature>;
type TimeoutCertificate = ConsensusTC<SecpSignature>;
type TimeoutInfo = ConsensusTmoInfo<AggSecpSignature>;

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

impl From<&HighQcRoundSigTuple> for ProtoHighQcRoundSigTuple {
    fn from(value: &HighQcRoundSigTuple) -> Self {
        Self {
            high_qc_round: Some((&value.high_qc_round).into()),
            author_signature: Some((&value.author_signature).into()),
        }
    }
}

impl TryFrom<ProtoHighQcRoundSigTuple> for HighQcRoundSigTuple {
    type Error = ProtoError;
    fn try_from(value: ProtoHighQcRoundSigTuple) -> Result<Self, Self::Error> {
        Ok(Self {
            high_qc_round: value
                .high_qc_round
                .ok_or(Self::Error::MissingRequiredField(
                    "Unverified<HighQcRound>.obj".to_owned(),
                ))?
                .try_into()?,
            author_signature: value
                .author_signature
                .ok_or(Self::Error::MissingRequiredField(
                    "Unverified<HighQcRound>.author_signature".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

impl From<&TimeoutCertificate> for ProtoTimeoutCertificate {
    fn from(value: &TimeoutCertificate) -> Self {
        Self {
            round: Some((&value.round).into()),
            high_qc_rounds: value
                .high_qc_rounds
                .iter()
                .map(|v| v.into())
                .collect::<Vec<_>>(),
        }
    }
}

impl TryFrom<ProtoTimeoutCertificate> for TimeoutCertificate {
    type Error = ProtoError;

    fn try_from(value: ProtoTimeoutCertificate) -> Result<Self, Self::Error> {
        Ok(Self {
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

impl From<&TimeoutInfo> for ProtoTimeoutInfoAggSig {
    fn from(value: &TimeoutInfo) -> Self {
        Self {
            round: Some((&value.round).into()),
            high_qc: Some((&value.high_qc).into()),
        }
    }
}

impl TryFrom<ProtoTimeoutInfoAggSig> for TimeoutInfo {
    type Error = ProtoError;

    fn try_from(value: ProtoTimeoutInfoAggSig) -> Result<Self, Self::Error> {
        Ok(Self {
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
