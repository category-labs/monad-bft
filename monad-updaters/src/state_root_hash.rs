use std::{
    collections::VecDeque,
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::{
    block::{Block, BlockType},
    signature_collection::SignatureCollection,
    state_root_hash::{StateRootHash, StateRootHashInfo},
    validator_data::ValidatorSetData,
};
use monad_crypto::{certificate_signature::CertificateSignatureRecoverable, hasher::Hash};
use monad_executor::Executor;
use monad_executor_glue::{MonadEvent, StateRootHashCommand};
use monad_types::{Epoch, SeqNum, Stake};
use rand::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use tracing::{debug, error};

pub trait MockableStateRootHash:
    Executor<
        Command = StateRootHashCommand<Block<Self::SignatureCollection>, Self::SignatureCollection>,
    > + Stream<Item = Self::Event>
    + Unpin
{
    type Event;
    type SignatureCollection: SignatureCollection;

    fn ready(&self) -> bool;
    fn get_validator_set_data(&self, epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection>;
    fn compute_state_root_hash(
        &self,
        block: &Block<Self::SignatureCollection>,
    ) -> StateRootHashInfo;
}

impl<T: MockableStateRootHash + ?Sized> MockableStateRootHash for Box<T> {
    type Event = T::Event;
    type SignatureCollection = T::SignatureCollection;

    fn ready(&self) -> bool {
        (**self).ready()
    }

    fn get_validator_set_data(&self, epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection> {
        (**self).get_validator_set_data(epoch)
    }

    fn compute_state_root_hash(
        &self,
        block: &Block<Self::SignatureCollection>,
    ) -> StateRootHashInfo {
        (**self).compute_state_root_hash(block)
    }
}

/// Validator set update prepared from staking contract/execution
pub(crate) struct ValidatorSetUpdate<SCT: SignatureCollection> {
    /// Epoch for which the validator set is prepared
    pub epoch: Epoch,
    pub validator_data: ValidatorSetData<SCT>,
}

/// An updater that immediately creates a StateRootHash update and
/// the ValidatorSetData for the next epoch when it receives a
/// ledger commit command.
/// Goal is to mimic the behaviour of execution receiving a commit
/// and generating the state root hash and updating the staking contract,
/// and sending it back to consensus.
pub struct MockStateRootHashNop<ST, SCT: SignatureCollection> {
    state_root_update: VecDeque<StateRootHashInfo>,

    // validator set updates
    genesis_validator_data: ValidatorSetData<SCT>,
    last_val_data: Option<ValidatorSetUpdate<SCT>>,
    next_val_data: Option<ValidatorSetUpdate<SCT>>,
    val_set_update_interval: SeqNum,
    calc_state_root: fn(SeqNum) -> StateRootHash,

    waker: Option<Waker>,
    phantom: PhantomData<ST>,
}

impl<ST, SCT: SignatureCollection> MockStateRootHashNop<ST, SCT> {
    /// Defines how an honest mock execution calculates state root hash
    fn state_root_honest(seq_num: SeqNum) -> StateRootHash {
        let mut gen = ChaChaRng::seed_from_u64(seq_num.0);
        let mut hash = StateRootHash(Hash([0; 32]));
        gen.fill_bytes(&mut hash.0 .0);
        hash
    }

    pub fn new(
        genesis_validator_data: ValidatorSetData<SCT>,
        val_set_update_interval: SeqNum,
    ) -> Self {
        let validator_data = genesis_validator_data.clone();
        Self {
            state_root_update: Default::default(),
            genesis_validator_data,
            last_val_data: None,
            next_val_data: None,
            val_set_update_interval,
            calc_state_root: Self::state_root_honest,

            waker: None,
            phantom: PhantomData,
        }
    }

    /// Change how state root hash is calculated
    pub fn inject_byzantine_srh(&mut self, calc_srh: fn(SeqNum) -> StateRootHash) {
        self.calc_state_root = calc_srh;
    }
}

impl<ST, SCT> MockableStateRootHash for MockStateRootHashNop<ST, SCT>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Event = MonadEvent<ST, SCT>;
    type SignatureCollection = SCT;

    fn ready(&self) -> bool {
        !self.state_root_update.is_empty() || self.next_val_data.is_some()
    }

    fn get_validator_set_data(&self, _epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection> {
        self.genesis_validator_data.clone()
    }

    fn compute_state_root_hash(&self, block: &Block<SCT>) -> StateRootHashInfo {
        // hash is pseudorandom seeded by the block's seq num to ensure
        // that it is deterministic between nodes
        let seq_num = block.get_seq_num();
        let state_root_hash = (self.calc_state_root)(seq_num);
        StateRootHashInfo {
            state_root_hash,
            seq_num,
        }
    }
}

impl<ST, SCT> Executor for MockStateRootHashNop<ST, SCT>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Command = StateRootHashCommand<Block<SCT>, SCT>;

    fn replay(&mut self, mut commands: Vec<Self::Command>) {
        commands.retain(|cmd| match cmd {
            // we match on all commands to be explicit
            StateRootHashCommand::LedgerCommit(..) => true,
            StateRootHashCommand::UpdateValidators(..) => true,
        });
        self.exec(commands)
    }

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::LedgerCommit(block) => {
                    self.state_root_update
                        .push_back(self.compute_state_root_hash(&block));

                    if block
                        .get_seq_num()
                        .is_epoch_end(self.val_set_update_interval)
                    {
                        if self.next_val_data.is_some() {
                            error!("Validator set data is not consumed");
                        }
                        let locked_epoch = block
                            .get_seq_num()
                            .get_locked_epoch(self.val_set_update_interval);
                        assert_eq!(
                            locked_epoch,
                            block.get_seq_num().to_epoch(self.val_set_update_interval) + Epoch(2)
                        );

                        match &self.last_val_data {
                            None => {
                                debug!(
                                locked_epoch = %locked_epoch.0,
                                num_validators = %self.genesis_validator_data.0.len(),
                                "last validator update not initialized, re-using genessis validator set");
                                self.next_val_data = Some(ValidatorSetUpdate {
                                    epoch: locked_epoch,
                                    validator_data: self.genesis_validator_data.clone(),
                                });
                            }
                            Some(ValidatorSetUpdate {
                                epoch,
                                validator_data,
                            }) => {
                                if epoch == &locked_epoch {
                                    debug!(
                                    locked_epoch = %locked_epoch.0,
                                    last_val_data_epoch = %epoch.0,
                                    num_validators = %validator_data.0.len(),
                                    "last validator update epoch matched locked epoch");
                                    self.next_val_data = Some(ValidatorSetUpdate {
                                        epoch: locked_epoch,
                                        validator_data: validator_data.clone(),
                                    });
                                } else {
                                    debug!(
                                        epoch = %epoch.0,
                                        locked_epoch = %locked_epoch.0,
                                        num_validators = %validator_data.0.len(),
                                    "last validator update initialized but did not match locked epoch, re-using validator set from last_val_data anyways");
                                    self.next_val_data = Some(ValidatorSetUpdate {
                                        epoch: locked_epoch,
                                        validator_data: validator_data.clone(),
                                    });
                                }
                            }
                        }
                    }

                    wake = true;
                }
                StateRootHashCommand::UpdateValidators((validator_data, epoch)) => {
                    debug!(num_validators = ?validator_data.0.len(), epoch = %epoch.0, "UpdateValidators");
                    self.last_val_data = Some(ValidatorSetUpdate {
                        epoch,
                        validator_data,
                    });
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

impl<ST, SCT> Stream for MockStateRootHashNop<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        let event = if let Some(info) = this.state_root_update.pop_front() {
            Poll::Ready(Some(MonadEvent::AsyncStateVerifyEvent(
                monad_executor_glue::AsyncStateVerifyEvent::LocalStateRoot(info),
            )))
        } else if let Some(next_val_data) = this.next_val_data.take() {
            Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::<SCT>::UpdateValidators((
                    next_val_data.validator_data,
                    next_val_data.epoch,
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
pub struct MockStateRootHashSwap<ST, SCT: SignatureCollection> {
    state_root_update: VecDeque<StateRootHashInfo>,

    // validator set updates
    epoch: Epoch,
    genesis_val_data: ValidatorSetData<SCT>,
    val_data_1: ValidatorSetData<SCT>,
    val_data_2: ValidatorSetData<SCT>,
    next_val_data: Option<ValidatorSetUpdate<SCT>>,
    val_set_update_interval: SeqNum,

    waker: Option<Waker>,
    phantom: PhantomData<ST>,
}

impl<ST, SCT: SignatureCollection> MockStateRootHashSwap<ST, SCT> {
    pub fn new(
        genesis_validator_data: ValidatorSetData<SCT>,
        val_set_update_interval: SeqNum,
    ) -> Self {
        let num_validators = genesis_validator_data.0.len();
        let mut val_data_1 = genesis_validator_data.0.clone();
        let mut val_data_2 = val_data_1.clone();

        for validator in val_data_1.iter_mut().take(num_validators / 2) {
            validator.stake = Stake(0);
        }
        for validator in val_data_2
            .iter_mut()
            .take(num_validators)
            .skip(num_validators / 2)
        {
            validator.stake = Stake(0);
        }

        Self {
            state_root_update: Default::default(),
            epoch: Epoch(1),
            genesis_val_data: genesis_validator_data,
            val_data_1: ValidatorSetData(val_data_1),
            val_data_2: ValidatorSetData(val_data_2),
            next_val_data: None,
            val_set_update_interval,
            waker: None,
            phantom: PhantomData,
        }
    }
}

impl<ST, SCT> MockableStateRootHash for MockStateRootHashSwap<ST, SCT>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Event = MonadEvent<ST, SCT>;
    type SignatureCollection = SCT;

    fn ready(&self) -> bool {
        !self.state_root_update.is_empty() || self.next_val_data.is_some()
    }

    fn get_validator_set_data(&self, epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection> {
        assert!(
            epoch <= self.epoch,
            "requesting epoch higher than seen in ledger"
        );

        // genesis epoch
        if epoch == Epoch(1) {
            return self.genesis_val_data.clone();
        }
        // in exec implementation
        // at the end of Epoch(even), next validator set is val_data_1
        // odd epoch number <> val_data_1
        // even epoch number <> val_data_2
        if epoch.0 % 2 == 0 {
            self.val_data_2.clone()
        } else {
            self.val_data_1.clone()
        }
    }

    fn compute_state_root_hash(&self, block: &Block<SCT>) -> StateRootHashInfo {
        let seq_num = block.get_seq_num();
        let mut gen = ChaChaRng::seed_from_u64(seq_num.0);
        let mut hash = StateRootHash(Hash([0; 32]));
        gen.fill_bytes(&mut hash.0 .0);

        StateRootHashInfo {
            state_root_hash: hash,
            seq_num,
        }
    }
}

impl<ST, SCT> Executor for MockStateRootHashSwap<ST, SCT>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Command = StateRootHashCommand<Block<SCT>, SCT>;

    fn replay(&mut self, mut commands: Vec<Self::Command>) {
        commands.retain(|cmd| match cmd {
            // we match on all commands to be explicit
            StateRootHashCommand::LedgerCommit(..) => true,
            StateRootHashCommand::UpdateValidators(..) => true,
        });
        self.exec(commands)
    }

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::LedgerCommit(block) => {
                    self.state_root_update
                        .push_back(self.compute_state_root_hash(&block));

                    if block
                        .get_seq_num()
                        .is_epoch_end(self.val_set_update_interval)
                    {
                        if self.next_val_data.is_some() {
                            error!("Validator set data is not consumed");
                        }
                        let locked_epoch = block
                            .get_seq_num()
                            .get_locked_epoch(self.val_set_update_interval);
                        assert_eq!(
                            locked_epoch,
                            block.get_seq_num().to_epoch(self.val_set_update_interval) + Epoch(2)
                        );
                        self.next_val_data = if locked_epoch.0 % 2 == 0 {
                            Some(ValidatorSetUpdate {
                                epoch: locked_epoch,
                                validator_data: self.val_data_1.clone(),
                            })
                        } else {
                            Some(ValidatorSetUpdate {
                                epoch: locked_epoch,
                                validator_data: self.val_data_2.clone(),
                            })
                        };
                    }

                    wake = true;
                }
                StateRootHashCommand::UpdateValidators(_) => {
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

impl<ST, SCT> Stream for MockStateRootHashSwap<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection + Unpin,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        let event = if let Some(info) = this.state_root_update.pop_front() {
            Poll::Ready(Some(MonadEvent::StateRootEvent(info)))
        } else if let Some(next_val_data) = this.next_val_data.take() {
            Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::<SCT>::UpdateValidators((
                    next_val_data.validator_data,
                    next_val_data.epoch,
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
