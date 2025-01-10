use std::marker::PhantomData;

use monad_crypto::certificate_signature::PubKey;
use monad_proto::{error::ProtoError, proto::signing::ProtoBlsSignatureCollection};

use crate::{
    aggregation_tree::{BlsSignatureCollection, SignerMap},
    BlsAggregateSignature,
};

impl<PT: PubKey> From<&BlsSignatureCollection<PT>> for ProtoBlsSignatureCollection {
    fn from(value: &BlsSignatureCollection<PT>) -> Self {
        Self {
            signers: serde_cbor::to_vec(&value.signers.0)
                .expect("serialization success")
                .into(),
            sig: value.sig.serialize().into(),
        }
    }
}

impl<PT: PubKey> TryFrom<ProtoBlsSignatureCollection> for BlsSignatureCollection<PT> {
    type Error = ProtoError;

    fn try_from(value: ProtoBlsSignatureCollection) -> Result<Self, Self::Error> {
        let signers = serde_cbor::from_slice(&value.signers)
            .map_err(|e| ProtoError::DeserializeError(format!("{}", e)))?;
        Ok(Self {
            signers: SignerMap(signers),
            sig: BlsAggregateSignature::deserialize(&value.sig)
                .map_err(|e| ProtoError::CryptoError(format!("{}", e)))?,
            _phantom: PhantomData,
        })
    }
}
