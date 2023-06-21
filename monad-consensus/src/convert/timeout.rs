use monad_crypto::{
    convert::{proto_to_signature, signature_to_proto},
    secp256k1::{SecpError, SecpPubKey},
    Signature,
};
use monad_proto::error::ProtoError;
use monad_proto::proto::timeout::*;

use crate::{
    signatures::aggregate_signature::AggregateSignatures,
    types::timeout::{
        HighQcRound, HighQcRoundSigTuple as TypeHighQcRoundSigTuple,
        TimeoutCertificate as ConsensusTC, TimeoutInfo as ConsensusTmoInfo,
    },
};

type HighQcRoundSigTuple<S> = TypeHighQcRoundSigTuple<S>;
type TimeoutCertificate<S> = ConsensusTC<S>;
type TimeoutInfo<S> = ConsensusTmoInfo<AggregateSignatures<S>>;

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

impl<S: Signature> From<&HighQcRoundSigTuple<S>> for ProtoHighQcRoundSigTuple {
    fn from(value: &HighQcRoundSigTuple<S>) -> Self {
        Self {
            high_qc_round: Some((&value.high_qc_round).into()),
            author_signature: Some(signature_to_proto(&value.author_signature)),
        }
    }
}

impl<S: Signature> TryFrom<ProtoHighQcRoundSigTuple> for HighQcRoundSigTuple<S> {
    type Error = ProtoError;
    fn try_from(value: ProtoHighQcRoundSigTuple) -> Result<Self, Self::Error> {
        Ok(Self {
            high_qc_round: value
                .high_qc_round
                .ok_or(Self::Error::MissingRequiredField(
                    "Unverified<HighQcRound>.obj".to_owned(),
                ))?
                .try_into()?,
            author_signature: proto_to_signature(value.author_signature.ok_or(
                Self::Error::MissingRequiredField(
                    "Unverified<HighQcRound>.author_signature".to_owned(),
                ),
            )?)?,
        })
    }
}

impl<S: Signature> From<&TimeoutCertificate<S>> for ProtoTimeoutCertificate {
    fn from(value: &TimeoutCertificate<S>) -> Self {
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

impl<S: Signature> TryFrom<ProtoTimeoutCertificate> for TimeoutCertificate<S> {
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

impl<S: Signature> From<&TimeoutInfo<S>> for ProtoTimeoutInfoAggSig {
    fn from(value: &TimeoutInfo<S>) -> Self {
        Self {
            round: Some((&value.round).into()),
            high_qc: Some((&value.high_qc).into()),
        }
    }
}

impl<S: Signature<PubKey = SecpPubKey, Error = SecpError>> TryFrom<ProtoTimeoutInfoAggSig>
    for TimeoutInfo<S>
{
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
