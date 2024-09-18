use monad_compress::lz4::Lz4Compression;
use monad_consensus::messages::{consensus_message::ProtocolMessage, message::ProposalMessage};
use monad_consensus::validation::signing::Validated;
use monad_consensus_types::{
    payload::TransactionPayload, signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_state::VerifiedMonadMessage;

pub trait Compressible {
    type Output;
    type Error;
    fn compress(&self) -> Result<Self::Output, Self::Error>;
}

#[derive(Debug)]
pub enum CompressionError {
    Unsupported,
    AlreadyCompressed,
}

impl<ST, SCT> Compressible for monad_state::VerifiedMonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Output = monad_state::VerifiedMonadMessage<ST, SCT>;
    type Error = CompressionError;

    fn compress(&self) -> Result<Self::Output, Self::Error> {
        match self.clone() {
            VerifiedMonadMessage::Consensus(m) => match &m.message {
                ProtocolMessage::Proposal(ProposalMessage { payload, .. }) => match &payload.txns {
                    TransactionPayload::List(full_transaction_list) => {
                        Ok(VerifiedMonadMessage::Consensus())
                    }
                    TransactionPayload::CompressedList { .. } => {
                        Err(CompressionError::AlreadyCompressed)
                    }
                    TransactionPayload::Null => Err(CompressionError::Unsupported),
                },
                _ => Err(CompressionError::Unsupported),
            },
            _ => Err(CompressionError::Unsupported),
        }
    }
}
