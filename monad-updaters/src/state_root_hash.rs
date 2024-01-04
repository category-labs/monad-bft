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
use monad_types::{Epoch, SeqNum, Stake};
use rand::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};

pub trait MockableStateRootHash:
    Executor<Command = StateRootHashCommand<Self::Block>> + Stream<Item = Self::Event> + Unpin
{
    type Block: BlockType;
    type Event;
    type SignatureCollection: SignatureCollection;

    fn new(
        init_val_data: ValidatorData<Self::SignatureCollection>,
        val_set_update_interval: SeqNum,
    ) -> Self;
    fn ready(&self) -> bool;
}

/// An updater that immediately creates a StateRootHash update and
/// the ValidatorData for the next epoch when it receives a
/// ledger commit command.
/// Goal is to mimic the behaviour of execution receiving a commit
/// and generating the state root hash and updating the staking contract,
/// and sending it back to consensus.
pub struct MockStateRootHashNop<O, ST, SCT: SignatureCollection> {
    state_root_update: Option<(SeqNum, Hash)>,

    // validator set updates
    epoch: Epoch,
    init_val_data: ValidatorData<SCT>,
    next_val_data: Option<ValidatorData<SCT>>,
    val_set_update_interval: SeqNum,

    waker: Option<Waker>,
    phantom: PhantomData<(O, ST)>,
}

impl<O, ST, SCT> MockableStateRootHash for MockStateRootHashNop<O, ST, SCT>
where
    O: BlockType + Unpin,
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Block = O;
    type Event = MonadEvent<ST, SCT>;
    type SignatureCollection = SCT;

    fn new(init_val_data: ValidatorData<SCT>, val_set_update_interval: SeqNum) -> Self {
        Self {
            epoch: Epoch(1),
            state_root_update: None,
            init_val_data,
            next_val_data: None,
            val_set_update_interval,
            waker: None,
            phantom: PhantomData,
        }
    }

    fn ready(&self) -> bool {
        self.state_root_update.is_some() || self.next_val_data.is_some()
    }
}

impl<O, ST, SCT> Executor for MockStateRootHashNop<O, ST, SCT>
where
    O: BlockType,
    SCT: SignatureCollection,
{
    type Command = StateRootHashCommand<O>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::LedgerCommit(block) => {
                    // hash is pseudorandom seeded by the block's seq num to ensure
                    // that it is deterministic between nodes
                    let seq_num = block.get_seq_num();
                    let mut gen = ChaChaRng::seed_from_u64(seq_num.0);
                    let mut hash = Hash([0; 32]);
                    gen.fill_bytes(&mut hash.0);

                    self.state_root_update = Some((seq_num, hash));

                    if block.get_seq_num() % self.val_set_update_interval == SeqNum(0) {
                        self.next_val_data = Some(self.init_val_data.clone());
                        self.epoch = self.epoch + Epoch(1);
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

impl<O, ST, SCT> Stream for MockStateRootHashNop<O, ST, SCT>
where
    Self: Unpin,
    O: BlockType + Unpin,
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        let event = if let Some((seqnum, hash)) = this.state_root_update.take() {
            Poll::Ready(Some(MonadEvent::ConsensusEvent(
                monad_executor_glue::ConsensusEvent::<ST, SCT>::StateUpdate((seqnum, hash)),
            )))
        } else if let Some(next_val_data) = this.next_val_data.take() {
            Poll::Ready(Some(MonadEvent::ConsensusEvent(
                monad_executor_glue::ConsensusEvent::<ST, SCT>::UpdateValidators((
                    next_val_data,
                    this.epoch,
                )),
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

/// An updater that works the same as MockStateRootHashNop but switches
/// between two sets of validators every epoch.
/// Goal is to mimic new validators joining and old validators leaving.
pub struct MockStateRootHashSwap<O, ST, SCT: SignatureCollection> {
    state_root_update: Option<(SeqNum, Hash)>,

    // validator set updates
    epoch: Epoch,
    val_data_1: ValidatorData<SCT>,
    val_data_2: ValidatorData<SCT>,
    next_val_data: Option<ValidatorData<SCT>>,
    val_set_update_interval: SeqNum,

    waker: Option<Waker>,
    phantom: PhantomData<(O, ST)>,
}

impl<O, ST, SCT> MockableStateRootHash for MockStateRootHashSwap<O, ST, SCT>
where
    O: BlockType + Unpin,
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Block = O;
    type Event = MonadEvent<ST, SCT>;
    type SignatureCollection = SCT;

    fn new(init_val_data: ValidatorData<SCT>, val_set_update_interval: SeqNum) -> Self {
        let num_validators = init_val_data.0.len();
        let mut val_data_1 = init_val_data.0;
        let mut val_data_2 = val_data_1.clone();

        for validator in val_data_1.iter_mut().take(num_validators / 2) {
            validator.1 = Stake(0);
        }
        for validator in val_data_2
            .iter_mut()
            .take(num_validators)
            .skip(num_validators / 2)
        {
            validator.1 = Stake(0);
        }

        Self {
            state_root_update: None,
            epoch: Epoch(1),
            val_data_1: ValidatorData(val_data_1),
            val_data_2: ValidatorData(val_data_2),
            next_val_data: None,
            val_set_update_interval,
            waker: None,
            phantom: PhantomData,
        }
    }

    fn ready(&self) -> bool {
        self.state_root_update.is_some() || self.next_val_data.is_some()
    }
}

impl<O, ST, SCT> Executor for MockStateRootHashSwap<O, ST, SCT>
where
    O: BlockType,
    SCT: SignatureCollection,
{
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

                    self.state_root_update = Some((seq_num, hash));

                    if block.get_seq_num() % self.val_set_update_interval == SeqNum(0) {
                        self.next_val_data = if self.epoch.0 % 2 == 0 {
                            Some(self.val_data_1.clone())
                        } else {
                            Some(self.val_data_2.clone())
                        };
                        self.epoch = self.epoch + Epoch(1);
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

impl<O, ST, SCT> Stream for MockStateRootHashSwap<O, ST, SCT>
where
    Self: Unpin,
    O: BlockType + Unpin,
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        let event = if let Some((seqnum, hash)) = this.state_root_update.take() {
            Poll::Ready(Some(MonadEvent::ConsensusEvent(
                monad_executor_glue::ConsensusEvent::<ST, SCT>::StateUpdate((seqnum, hash)),
            )))
        } else if let Some(next_val_data) = this.next_val_data.take() {
            Poll::Ready(Some(MonadEvent::ConsensusEvent(
                monad_executor_glue::ConsensusEvent::<ST, SCT>::UpdateValidators((
                    next_val_data,
                    this.epoch,
                )),
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
