use std::{marker::PhantomData, task::Poll};

use futures::Stream;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_executor_glue::MonadEvent;

pub struct MockIpcReceiver<ST, SCT> {
    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> Default for MockIpcReceiver<ST, SCT> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT> Stream for MockIpcReceiver<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // TODO: generate user tx
        Poll::Ready(None)
    }
}
