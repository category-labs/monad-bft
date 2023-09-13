use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_executor_glue::MonadEvent;

pub struct MockEpoch<ST, SCT> {
    state: Option<()>,
    _marker: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> MockEpoch<ST, SCT> {
    pub fn ready(&self) -> bool {
        self.state.is_some()
    }
}

impl<ST, SCT> Default for MockEpoch<ST, SCT> {
    fn default() -> Self {
        Self {
            state: None,
            _marker: PhantomData,
        }
    }
}

impl<ST, SCT> Stream for MockEpoch<ST, SCT>
where
    Self: Unpin,
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    type Item = MonadEvent<ST, SCT>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}
