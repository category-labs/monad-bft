use crate::error::ProtoError;
use monad_consensus::types::signature::ConsensusSignature;
use monad_crypto::secp256k1::Signature;
use zerocopy::AsBytes;

include!(concat!(env!("OUT_DIR"), "/monad_proto.signing.rs"));

impl From<&ConsensusSignature> for ProtoConsensusSignature {
    fn from(sig: &ConsensusSignature) -> Self {
        ProtoConsensusSignature {
            sig: sig.0.serialize().to_vec(),
        }
    }
}

impl TryFrom<ProtoConsensusSignature> for ConsensusSignature {
    type Error = ProtoError;

    fn try_from(value: ProtoConsensusSignature) -> Result<Self, Self::Error> {
        Ok(Self {
            0: Signature::deserialize(value.sig.as_bytes())?,
        })
    }
}
