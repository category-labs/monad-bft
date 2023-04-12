use crate::error::ProtoError;
use monad_consensus::types::signature::ConsensusSignature;
use monad_crypto::secp256k1::Signature;
use monad_types::Round;
use zerocopy::AsBytes;

include!(concat!(env!("OUT_DIR"), "/monad_proto.timeout.rs"));

impl From<&Round> for ProtoHighQcRound {
    fn from(value: &Round) -> Self {
        ProtoHighQcRound {
            qc_round: value.into(),
        }
    }
}

impl TryFrom<ProtoHighQcRound> for Round {
    type Error = ProtoError;
    fn try_from(value: ProtoHighQcRound) -> Result<Self, Self::Error> {
        Ok(Round(value.qc_round.map(f)))
    }
}
