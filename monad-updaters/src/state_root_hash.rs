use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::{
    block::BlockType, message_signature::MessageSignature,
    signature_collection::SignatureCollection, validator_data::ValidatorData,
};
use monad_crypto::hasher::Hash;
use monad_executor::Executor;
use monad_executor_glue::{MonadEvent, StateRootHashCommand};
use monad_types::SeqNum;
use rand::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};

pub struct MockStateRootHash<O, ST, SCT: SignatureCollection> {
    // state updates
    update: Option<(SeqNum, Hash)>,

    // validator set updates
    init_val_set: ValidatorData<SCT>,
    next_val_set: Option<ValidatorData<SCT>>,
    val_set_update_interval: SeqNum,

    waker: Option<Waker>,
    phantom: PhantomData<(O, ST)>,
}

impl<O, ST, SCT: SignatureCollection> MockStateRootHash<O, ST, SCT> {
    pub fn ready(&self) -> bool {
        self.update.is_some() || self.next_val_set.is_some()
    }
}

impl<O, ST, SCT: SignatureCollection> MockStateRootHash<O, ST, SCT> {
    pub fn new(init_val_set: ValidatorData<SCT>) -> Self {
        Self {
            update: None,
            init_val_set,
            next_val_set: None,
            // TODO: Make this configurable
            val_set_update_interval: SeqNum(2000),
            waker: None,
            phantom: PhantomData,
        }
    }
}

impl<O: BlockType, ST, SCT: SignatureCollection> Executor for MockStateRootHash<O, ST, SCT> {
    type Command = StateRootHashCommand<O>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::LedgerCommit(block) => {
                    let seq_num = block.get_seq_num();
                    let mut gen = ChaChaRng::seed_from_u64(seq_num.0);
                    let mut hash = Hash([0; 32]);
                    gen.fill_bytes(&mut hash.0);

                    self.update = Some((seq_num, hash));

                    if block.get_seq_num() % self.val_set_update_interval == SeqNum(0) {
                        self.next_val_set = Some(self.init_val_set.clone());
                    }

                    wake = true;
                }
            }
        }
        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            };
        }
    }
}

impl<O, ST, SCT> Stream for MockStateRootHash<O, ST, SCT>
where
    Self: Unpin,
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    type Item = MonadEvent<ST, SCT>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        let event = if let Some((seqnum, hash)) = this.update.take() {
            Poll::Ready(Some(MonadEvent::ConsensusEvent(
                monad_executor_glue::ConsensusEvent::<ST, SCT>::StateUpdate((seqnum, hash)),
            )))
        } else if let Some(next_val_set) = this.next_val_set.take() {
            Poll::Ready(Some(MonadEvent::ConsensusEvent(
                monad_executor_glue::ConsensusEvent::<ST, SCT>::UpdateNextValSet(next_val_set),
            )))
        } else {
            Poll::Pending
        };

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if this.ready() {
            this.waker.take().unwrap().wake();
        }

        event
    }
}
