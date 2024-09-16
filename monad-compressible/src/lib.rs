use monad_compress::lz4::Lz4Compression;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
pub trait Compressible {
    type Output;
    fn compress(&self) -> Self::Output;
}

impl<ST, SCT> Compressible for monad_state::MonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Output = monad_state::MonadMessage<ST, SCT>;

    fn compress(&self) -> Self::Output {
        todo!()
    }
}

impl<ST, SCT> Compressible for monad_state::VerifiedMonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Output = monad_state::VerifiedMonadMessage<ST, SCT>;

    fn compress(&self) -> Self::Output {
        let compress = Lz4Compression::new(0);
        todo!()
    }
}
