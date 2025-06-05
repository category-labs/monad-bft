use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use futures::Stream;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::MonadEvent;
use monad_types::ExecutionProtocol;
use tokio::time::{Duration, Interval};

pub struct TokioTimestamp<ST, SCT, EPT> {
    /// create timestamp events at this interval
    interval: Interval,
    _phantom: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> TokioTimestamp<ST, SCT, EPT> {
    pub fn new(period: Duration) -> Self {
        Self {
            interval: tokio::time::interval(period),
            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT, EPT> Stream for TokioTimestamp<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        match this.interval.poll_tick(cx) {
            Poll::Ready(_) => {
                let start = SystemTime::now();
                let epoch_time = start
                    .duration_since(UNIX_EPOCH)
                    .expect("Clock may have gone backwards");
                let t = epoch_time.as_nanos() as u64;
                Poll::Ready(Some(MonadEvent::TimestampUpdateEvent(t)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
