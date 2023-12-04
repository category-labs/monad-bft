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
use monad_executor::Executor;
use monad_executor_glue::{MonadEvent, ValidatorSetCommand};
use monad_types::SeqNum;

pub struct ValidatorSetUpdater<ST, SCT: SignatureCollection> {
    // TODO: Copy the latest validator set into this after execution
    // sends delta after executing block 'epoch_boundary'.
    // Should be None between UpdateNextValSet event and receiving
    // delta for the next 'epoch_boundary'.
    validator_set: Option<ValidatorData<SCT>>,
    // TODO: call waker.wake() when exeuction sends or if execution
    // already sent the validator set updates after executing this block.
    epoch_boundary: SeqNum,
    waker: Option<Waker>,
    _marker: PhantomData<ST>,
}

impl<ST, SCT: SignatureCollection> Default for ValidatorSetUpdater<ST, SCT> {
    fn default() -> Self {
        Self {
            validator_set: None,
            epoch_boundary: SeqNum(0),
            waker: None,
            _marker: PhantomData,
        }
    }
}

impl<ST, SCT: SignatureCollection> Executor for ValidatorSetUpdater<ST, SCT> {
    type Command = ValidatorSetCommand;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                ValidatorSetCommand::EpochEnd(seq_num) => {
                    self.epoch_boundary = seq_num;
                }
            }
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
        assert!(this.validator_set.is_some());

        Poll::Ready(Some(MonadEvent::ConsensusEvent(
            monad_executor_glue::ConsensusEvent::UpdateNextValSet(
                this.validator_set
                    .take()
                    .expect("there should be a ValidatorData object"),
            ),
        )))
    }
}
