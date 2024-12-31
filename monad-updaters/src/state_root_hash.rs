use std::{
    collections::VecDeque,
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::Stream;
use monad_consensus_types::{
    block::{ExecutionProtocol, ExecutionResult, MockableFinalizedHeader, ProposedExecutionResult},
    signature_collection::SignatureCollection,
    state_root_hash::{StateRootHash, StateRootHashInfo},
    validator_data::ValidatorSetData,
};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    hasher::Hash,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, StateRootHashCommand};
use monad_types::{Epoch, SeqNum, Stake};
use rand::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use tracing::{debug, error};

pub trait MockableStateRootHash:
    Executor<Command = StateRootHashCommand<Self::SignatureCollection>>
    + Stream<Item = Self::Event>
    + Unpin
{
    type Event;
    type SignatureCollection: SignatureCollection;

    fn ready(&self) -> bool;
    fn get_validator_set_data(&self, epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection>;
    fn set_state_root_poll(&mut self, keep_update: bool);
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

    fn set_state_root_poll(&mut self, keep_update: bool) {
        (**self).set_state_root_poll(keep_update)
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
pub struct MockStateRootHashNop<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    state_root_update: VecDeque<ExecutionResult<EPT>>,

    // validator set updates
    genesis_validator_data: ValidatorSetData<SCT>,
    next_val_data: Option<ValidatorSetUpdate<SCT>>,
    val_set_update_interval: SeqNum,
    calc_state_root: fn(&SeqNum) -> StateRootHash,

    last_sent_epoch: Option<SeqNum>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    phantom: PhantomData<ST>,
}

impl<ST, SCT, EPT> MockStateRootHashNop<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    /// Defines how an honest mock execution calculates state root hash
    fn state_root_honest(seq_num: &SeqNum) -> StateRootHash {
        let mut gen = ChaChaRng::seed_from_u64(seq_num.0);
        let mut hash = StateRootHash(Hash([0; 32]));
        gen.fill_bytes(&mut hash.0 .0);
        hash
    }

    pub fn new(
        genesis_validator_data: ValidatorSetData<SCT>,
        val_set_update_interval: SeqNum,
    ) -> Self {
        Self {
            state_root_update: Default::default(),
            genesis_validator_data,
            next_val_data: None,
            val_set_update_interval,
            calc_state_root: Self::state_root_honest,

            last_sent_epoch: None,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }

    /// Change how state root hash is calculated
    pub fn inject_byzantine_srh(&mut self, calc_srh: fn(&SeqNum) -> StateRootHash) {
        self.calc_state_root = calc_srh;
    }

    fn jank_update_valset(&mut self, seq_num: SeqNum) {
        if seq_num.is_epoch_end(self.val_set_update_interval)
            && self.last_sent_epoch != Some(seq_num)
        {
            if self.next_val_data.is_some() {
                error!("Validator set data is not consumed");
            }
            let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
            assert_eq!(
                locked_epoch,
                seq_num.to_epoch(self.val_set_update_interval) + Epoch(2)
            );
            self.next_val_data = Some(ValidatorSetUpdate {
                epoch: locked_epoch,
                validator_data: self.genesis_validator_data.clone(),
            });
            self.last_sent_epoch = Some(seq_num);
        }
    }
}

impl<ST, SCT, EPT> MockableStateRootHash for MockStateRootHashNop<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    type Event = MonadEvent<ST, SCT, EPT>;
    type SignatureCollection = SCT;

    fn ready(&self) -> bool {
        !self.state_root_update.is_empty() || self.next_val_data.is_some()
    }

    fn get_validator_set_data(&self, _epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection> {
        self.genesis_validator_data.clone()
    }

    fn set_state_root_poll(&mut self, keep_update: bool) {
    }
}

impl<ST, SCT, EPT> Executor for MockStateRootHashNop<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    type Command = StateRootHashCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::CancelBelow(cancel_below) => {
                    while self
                        .state_root_update
                        .front()
                        .is_some_and(|state_root| state_root.seq_num() < cancel_below)
                    {
                        self.state_root_update.pop_front().unwrap();
                    }
                }
                StateRootHashCommand::RequestProposed(block_id, seq_num, round) => {
                    self.state_root_update.push_back(ExecutionResult::Proposed(
                        ProposedExecutionResult {
                            block_id,
                            seq_num,
                            round,
                            result: EPT::FinalizedHeader::from_seq_num(seq_num),
                        },
                    ));
                    self.jank_update_valset(seq_num);
                    wake = true;
                }
                StateRootHashCommand::RequestFinalized(seq_num) => {
                    self.state_root_update.push_back(ExecutionResult::Finalized(
                        seq_num,
                        EPT::FinalizedHeader::from_seq_num(seq_num),
                    ));
                    self.jank_update_valset(seq_num);
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

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT, EPT> Stream for MockStateRootHashNop<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        let event = if let Some(event) = this.state_root_update.pop_front() {
            Poll::Ready(Some(MonadEvent::ExecutionResultEvent(event)))
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
pub struct MockStateRootHashSwap<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    state_root_update: VecDeque<ExecutionResult<EPT>>,

    // validator set updates
    epoch: Epoch,
    genesis_val_data: ValidatorSetData<SCT>,
    val_data_1: ValidatorSetData<SCT>,
    val_data_2: ValidatorSetData<SCT>,
    next_val_data: Option<ValidatorSetUpdate<SCT>>,
    val_set_update_interval: SeqNum,

    last_sent_epoch: Option<SeqNum>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    phantom: PhantomData<ST>,
}

impl<ST, SCT, EPT> MockStateRootHashSwap<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
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

            last_sent_epoch: None,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }

    fn jank_update_valset(&mut self, seq_num: SeqNum) {
        if seq_num.is_epoch_end(self.val_set_update_interval)
            && self.last_sent_epoch != Some(seq_num)
        {
            if self.next_val_data.is_some() {
                error!("Validator set data is not consumed");
            }
            let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
            assert_eq!(
                locked_epoch,
                seq_num.to_epoch(self.val_set_update_interval) + Epoch(2)
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
        self.last_sent_epoch = Some(seq_num);
    }
}

impl<ST, SCT, EPT> MockableStateRootHash for MockStateRootHashSwap<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    type Event = MonadEvent<ST, SCT, EPT>;
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

    fn set_state_root_poll(&mut self, keep_update: bool) {
    }
}

impl<ST, SCT, EPT> Executor for MockStateRootHashSwap<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    type Command = StateRootHashCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::CancelBelow(cancel_below) => {
                    while self
                        .state_root_update
                        .front()
                        .is_some_and(|state_root| state_root.seq_num() < cancel_below)
                    {
                        self.state_root_update.pop_front().unwrap();
                    }
                }
                StateRootHashCommand::RequestProposed(block_id, seq_num, round) => {
                    self.state_root_update.push_back(ExecutionResult::Proposed(
                        ProposedExecutionResult {
                            block_id,
                            seq_num,
                            round,
                            result: EPT::FinalizedHeader::from_seq_num(seq_num),
                        },
                    ));
                    self.jank_update_valset(seq_num);
                    wake = true;
                }
                StateRootHashCommand::RequestFinalized(seq_num) => {
                    self.state_root_update.push_back(ExecutionResult::Finalized(
                        seq_num,
                        EPT::FinalizedHeader::from_seq_num(seq_num),
                    ));
                    self.jank_update_valset(seq_num);
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

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT, EPT> Stream for MockStateRootHashSwap<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        let event = if let Some(event) = this.state_root_update.pop_front() {
            Poll::Ready(Some(MonadEvent::ExecutionResultEvent(event)))
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

/// An updater that works the same as MockStateRootHashNop but allows
/// to pause and start ExecutionResult updates.
/// The goal is to mimic the delay in ExecutionResults on "slow" validators.
pub struct MockStateRootHashScheduler<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    state_root_update: VecDeque<ExecutionResult<EPT>>,

    // validator set updates
    genesis_validator_data: ValidatorSetData<SCT>,
    next_val_data: Option<ValidatorSetUpdate<SCT>>,
    val_set_update_interval: SeqNum,
    calc_state_root: fn(&SeqNum) -> StateRootHash,

    last_sent_epoch: Option<SeqNum>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    phantom: PhantomData<ST>,
    // enables ExecutionResult updates
    keep_update: bool,
}

impl<ST, SCT, EPT> MockStateRootHashScheduler<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    /// Defines how an honest mock execution calculates state root hash
    fn state_root_honest(seq_num: &SeqNum) -> StateRootHash {
        let mut gen = ChaChaRng::seed_from_u64(seq_num.0);
        let mut hash = StateRootHash(Hash([0; 32]));
        gen.fill_bytes(&mut hash.0 .0);
        hash
    }

    pub fn new(
        genesis_validator_data: ValidatorSetData<SCT>,
        val_set_update_interval: SeqNum,
    ) -> Self {
        Self {
            state_root_update: Default::default(),
            genesis_validator_data,
            next_val_data: None,
            val_set_update_interval,
            calc_state_root: Self::state_root_honest,

            last_sent_epoch: None,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
            keep_update: true,
        }
    }

    /// Change how state root hash is calculated
    pub fn inject_byzantine_srh(&mut self, calc_srh: fn(&SeqNum) -> StateRootHash) {
        self.calc_state_root = calc_srh;
    }

    fn jank_update_valset(&mut self, seq_num: SeqNum) {
        if seq_num.is_epoch_end(self.val_set_update_interval)
            && self.last_sent_epoch != Some(seq_num)
        {
            if self.next_val_data.is_some() {
                error!("Validator set data is not consumed");
            }
            let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
            assert_eq!(
                locked_epoch,
                seq_num.to_epoch(self.val_set_update_interval) + Epoch(2)
            );
            self.next_val_data = Some(ValidatorSetUpdate {
                epoch: locked_epoch,
                validator_data: self.genesis_validator_data.clone(),
            });
            self.last_sent_epoch = Some(seq_num);
        }
    }
}

impl<ST, SCT, EPT> MockableStateRootHash for MockStateRootHashScheduler<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    type Event = MonadEvent<ST, SCT, EPT>;
    type SignatureCollection = SCT;

    fn ready(&self) -> bool {
        self.keep_update && (!self.state_root_update.is_empty() || self.next_val_data.is_some())
    }

    fn get_validator_set_data(&self, _epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection> {
        self.genesis_validator_data.clone()
    }

    fn set_state_root_poll(&mut self, keep_update: bool) {
        self.keep_update = keep_update;
    }
}

impl<ST, SCT, EPT> Executor for MockStateRootHashScheduler<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    type Command = StateRootHashCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                StateRootHashCommand::CancelBelow(cancel_below) => {
                    while self
                        .state_root_update
                        .front()
                        .is_some_and(|state_root| state_root.seq_num() < cancel_below)
                    {
                        self.state_root_update.pop_front().unwrap();
                    }
                }
                StateRootHashCommand::RequestProposed(block_id, seq_num, round) => {
                    self.state_root_update.push_back(ExecutionResult::Proposed(
                        ProposedExecutionResult {
                            block_id,
                            seq_num,
                            round,
                            result: EPT::FinalizedHeader::from_seq_num(seq_num),
                        },
                    ));
                    self.jank_update_valset(seq_num);
                    wake = true;
                }
                StateRootHashCommand::RequestFinalized(seq_num) => {
                    self.state_root_update.push_back(ExecutionResult::Finalized(
                        seq_num,
                        EPT::FinalizedHeader::from_seq_num(seq_num),
                    ));
                    self.jank_update_valset(seq_num);
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

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT, EPT> Stream for MockStateRootHashScheduler<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    EPT::FinalizedHeader: MockableFinalizedHeader,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if !this.keep_update {
            if this.waker.is_none() {
                this.waker = Some(cx.waker().clone());
            }

            if this.ready() {
                this.waker.take().unwrap().wake();
            }
            return Poll::Pending;
        }

        let event = if let Some(event) = this.state_root_update.pop_front() {
            Poll::Ready(Some(MonadEvent::ExecutionResultEvent(event)))
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