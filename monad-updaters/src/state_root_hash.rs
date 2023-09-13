use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::block::BlockType;
use monad_executor::Executor;
use monad_executor_glue::StateRootHashCommand;
use monad_types::Hash;
use rand::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};

pub struct MockStateRootHash<O, E> {
    update: Option<(u64, Hash)>,
    generate_event: Option<Box<dyn (FnOnce(u64, Hash) -> E) + Send + Sync>>,
    waker: Option<Waker>,
    phantom: PhantomData<O>,
}

impl<O, E> MockStateRootHash<O, E> {
    pub fn ready(&self) -> bool {
        self.update.is_some()
    }
}

impl<O, E> Default for MockStateRootHash<O, E> {
    fn default() -> Self {
        Self {
            update: None,
            generate_event: None,
            waker: None,
            phantom: PhantomData,
        }
    }
}

impl<O: BlockType, E> Executor for MockStateRootHash<O, E> {
    type Command = StateRootHashCommand<O, E>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::LedgerCommit(block, func) => {
                    let seq_num = block.get_seq_num();
                    let mut gen = ChaChaRng::seed_from_u64(seq_num);
                    let mut hash = Hash([0; 32]);
                    gen.fill_bytes(&mut hash.0);

                    self.update = Some((seq_num, hash));
                    self.generate_event = Some(func);
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

impl<O, E> Stream for MockStateRootHash<O, E>
where
    Self: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some((seqnum, hash)) = this.update.take() {
            if let Some(cb) = this.generate_event.take() {
                return Poll::Ready(Some(cb(seqnum, hash)));
            }
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
