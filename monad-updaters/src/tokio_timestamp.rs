use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use futures::Stream;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_executor_glue::MonadEvent;
use tokio::time::{Duration, Interval};

pub struct TokioTimestamp<ST, SCT> {
    /// create timestamp events at this interval
    interval: Interval,
    _phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> TokioTimestamp<ST, SCT> {
    pub fn new(period: Duration) -> Self {
        Self {
            interval: tokio::time::interval(period),
            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT> Stream for TokioTimestamp<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Item = MonadEvent<ST, SCT>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        match this.interval.poll_tick(cx) {
            Poll::Ready(_) => Poll::Ready(Some(MonadEvent::TimestampUpdateEvent(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    .try_into()
                    .unwrap(),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}
