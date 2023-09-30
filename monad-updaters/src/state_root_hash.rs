use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::{
    block::BlockType, message_signature::MessageSignature,
    signature_collection::SignatureCollection,
};
use monad_executor::Executor;
use monad_executor_glue::{MonadEvent, StateRootHashCommand};
use monad_types::Hash;
use rand::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};

pub struct MockStateRootHash<O, ST, SCT> {
    update: Option<(u64, Hash)>,
    waker: Option<Waker>,
    phantom: PhantomData<(O, ST, SCT)>,
}

impl<O, ST, SCT> MockStateRootHash<O, ST, SCT> {
    pub fn ready(&self) -> bool {
        self.update.is_some()
    }
}

impl<O, ST, SCT> Default for MockStateRootHash<O, ST, SCT> {
    fn default() -> Self {
        Self {
            update: None,
            waker: None,
            phantom: PhantomData,
        }
    }
}

impl<O: BlockType, ST, SCT> Executor for MockStateRootHash<O, ST, SCT> {
    type Command = StateRootHashCommand<O>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::LedgerCommit(block) => {
                    let seq_num = block.get_seq_num();
                    let mut gen = ChaChaRng::seed_from_u64(seq_num);
                    let mut hash = Hash([0; 32]);
                    gen.fill_bytes(&mut hash.0);

                    self.update = Some((seq_num, hash));
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

        if let Some((seqnum, hash)) = this.update.take() {
            return Poll::Ready(Some(MonadEvent::ConsensusEvent(
                monad_executor_glue::ConsensusEvent::StateUpdate((seqnum, hash)),
            )));
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
