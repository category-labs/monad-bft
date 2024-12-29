use monad_proto::{error::ProtoError, proto::quorum_certificate::*};

use super::signing::{proto_to_signature_collection, signature_collection_to_proto};
use crate::{quorum_certificate::QuorumCertificate, signature_collection::SignatureCollection};

impl<SCT: SignatureCollection> From<&QuorumCertificate<SCT>> for ProtoQuorumCertificate {
    fn from(value: &QuorumCertificate<SCT>) -> Self {
        Self {
            info: Some((&value.info).into()),
            signatures: Some(signature_collection_to_proto(&value.signatures)),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoQuorumCertificate> for QuorumCertificate<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoQuorumCertificate) -> Result<Self, Self::Error> {
        Ok(QuorumCertificate::new(
            value
                .info
                .ok_or(Self::Error::MissingRequiredField("QC.info".to_owned()))?
                .try_into()?,
            proto_to_signature_collection(value.signatures.ok_or(
                Self::Error::MissingRequiredField("QC.signatures".to_owned()),
            )?)?,
        ))
    }
}
