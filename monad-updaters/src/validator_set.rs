use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
    validator_data::ValidatorData,
};
use monad_executor_glue::MonadEvent;
use monad_types::SeqNum;

pub struct ValidatorSetUpdater<ST, SCT: SignatureCollection> {
    // TODO: Copy the latest validator set into this after receiving
    // exeuction updates every 'update_duration' blocks
    // Should be None between UpdateNextValSet event and receiving
    // delta for the next 'update_duration'.
    validator_data: Option<ValidatorData<SCT>>,
    // TODO: call waker.wake() when exeuction sends (or already sent)
    // deltas every 'update_duration' blocks.
    _update_duration: SeqNum,
    waker: Option<Waker>,
    _marker: PhantomData<ST>,
}

impl<ST, SCT: SignatureCollection> ValidatorSetUpdater<ST, SCT> {
    pub fn ready(&self) -> bool {
        self.validator_data.is_some()
    }
}

impl<ST, SCT: SignatureCollection> Default for ValidatorSetUpdater<ST, SCT> {
    fn default() -> Self {
        Self {
            validator_data: None,
            _update_duration: SeqNum(100),
            waker: None,
            _marker: PhantomData,
        }
    }
}

impl<ST, SCT> Stream for ValidatorSetUpdater<ST, SCT>
where
    Self: Unpin,
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    type Item = MonadEvent<ST, SCT>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.waker = Some(cx.waker().clone());

        let this = self.get_mut();

        // if woken up, there should be a ValidatorData object
        assert!(this.validator_data.is_some());

        Poll::Ready(Some(MonadEvent::ConsensusEvent(
            monad_executor_glue::ConsensusEvent::UpdateNextValSet(
                this.validator_data
                    .take()
                    .expect("there should be a ValidatorData object"),
            ),
        )))
    }
}
