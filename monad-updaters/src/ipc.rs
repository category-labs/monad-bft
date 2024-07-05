use std::{
    marker::PhantomData,
    mem,
    ops::DerefMut,
    task::{Poll, Waker},
};

use bytes::Bytes;
use futures::Stream;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_executor_glue::{MempoolEvent, MonadEvent};

pub struct MockIpcReceiver<ST, SCT> {
    transactions: Vec<Bytes>,

    waker: Option<Waker>,
    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> Default for MockIpcReceiver<ST, SCT> {
    fn default() -> Self {
        Self {
            transactions: Default::default(),
            waker: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT> MockIpcReceiver<ST, SCT> {
    pub fn add_transaction(&mut self, txn: Bytes) {
        self.transactions.push(txn);

        if let Some(waker) = self.waker.take() {
            waker.wake()
        };
    }

    pub fn ready(&self) -> bool {
        !self.transactions.is_empty()
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
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.transactions.is_empty() {
            this.waker = Some(cx.waker().clone());
            Poll::Pending
        } else {
            let txn_bytes = mem::take(&mut this.transactions);
            Poll::Ready(Some(MonadEvent::MempoolEvent(MempoolEvent::UserTxns(
                txn_bytes,
            ))))
        }
    }
}
