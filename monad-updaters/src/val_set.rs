use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use alloy_primitives::U256;
use futures::Stream;
use monad_consensus_types::validator_data::{ValidatorSetData, ValidatorSetDataWithEpoch};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, ValSetCommand};
use monad_types::{Epoch, ExecutionProtocol, SeqNum, Stake};
use monad_validator::signature_collection::SignatureCollection;
use tracing::error;

pub trait MockableValSetUpdater:
    Executor<Command = ValSetCommand> + Stream<Item = Self::Event> + Unpin
{
    type Event;
    type SignatureCollection: SignatureCollection;

    fn ready(&self) -> bool;
    fn get_validator_set_data(&self, epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection>;
}

impl<T: MockableValSetUpdater + ?Sized> MockableValSetUpdater for Box<T> {
    type Event = T::Event;
    type SignatureCollection = T::SignatureCollection;

    fn ready(&self) -> bool {
        (**self).ready()
    }

    fn get_validator_set_data(&self, epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection> {
        (**self).get_validator_set_data(epoch)
    }
}

/// An updater that immediately creates a Validator Set update and
/// the ValidatorSetData for the next epoch when it receives a
/// ledger commit command.
/// Goal is to mimic the behaviour of constant validator set between
/// epochs
pub struct MockValSetUpdaterNop<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // validator set updates
    genesis_validator_data: ValidatorSetData<SCT>,
    next_val_data: Option<ValidatorSetDataWithEpoch<SCT>>,
    val_set_update_interval: SeqNum,

    enable_updates: bool,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    phantom: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> MockValSetUpdaterNop<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        genesis_validator_data: ValidatorSetData<SCT>,
        val_set_update_interval: SeqNum,
    ) -> Self {
        Self {
            genesis_validator_data,
            next_val_data: None,
            val_set_update_interval,

            enable_updates: true,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }

    pub fn with_updates_enabled(mut self, on: bool) -> Self {
        self.enable_updates = on;
        self
    }

    fn jank_update_valset(&mut self, seq_num: SeqNum) {
        if seq_num.is_epoch_end(self.val_set_update_interval) {
            if self.next_val_data.is_some() {
                error!("Validator set data is not consumed");
            }
            let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
            assert_eq!(
                locked_epoch,
                seq_num.to_epoch(self.val_set_update_interval) + Epoch(1)
            );
            self.next_val_data = Some(ValidatorSetDataWithEpoch {
                epoch: locked_epoch,
                validators: self.genesis_validator_data.clone(),
            });
        }
    }
}

impl<ST, SCT, EPT> MockableValSetUpdater for MockValSetUpdaterNop<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Event = MonadEvent<ST, SCT, EPT>;
    type SignatureCollection = SCT;

    fn ready(&self) -> bool {
        if !self.enable_updates {
            return false;
        }
        self.next_val_data.is_some()
    }

    fn get_validator_set_data(&self, _epoch: Epoch) -> ValidatorSetData<Self::SignatureCollection> {
        self.genesis_validator_data.clone()
    }
}

impl<ST, SCT, EPT> Executor for MockValSetUpdaterNop<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Command = ValSetCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                ValSetCommand::NotifyFinalized(seq_num) => {
                    self.jank_update_valset(seq_num);
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

impl<ST, SCT, EPT> Stream for MockValSetUpdaterNop<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if !this.enable_updates {
            return Poll::Pending;
        }

        let event = if let Some(next_val_data) = this.next_val_data.take() {
            Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::<SCT>::UpdateValidators(next_val_data),
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

/// An updater that works the same as MockValSetUpdaterNop but switches
/// between two sets of validators every epoch.
/// Goal is to mimic new validators joining and old validators leaving.
pub struct MockValSetUpdaterSwap<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // validator set updates
    epoch: Epoch,
    genesis_val_data: ValidatorSetData<SCT>,
    val_data_1: ValidatorSetData<SCT>,
    val_data_2: ValidatorSetData<SCT>,
    next_val_data: Option<ValidatorSetDataWithEpoch<SCT>>,
    val_set_update_interval: SeqNum,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    phantom: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> MockValSetUpdaterSwap<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        genesis_validator_data: ValidatorSetData<SCT>,
        val_set_update_interval: SeqNum,
    ) -> Self {
        let num_validators = genesis_validator_data.0.len();
        let mut val_data_1 = genesis_validator_data.0.clone();
        let mut val_data_2 = val_data_1.clone();

        // TODO: remove zero staked validators
        for validator in val_data_1
            .iter_mut()
            .take(num_validators)
            .skip(num_validators / 2)
        {
            validator.stake = Stake(U256::ZERO);
        }
        for validator in val_data_2.iter_mut().take(num_validators / 2) {
            validator.stake = Stake(U256::ZERO);
        }

        Self {
            epoch: Epoch(1),
            genesis_val_data: genesis_validator_data,
            val_data_1: ValidatorSetData(val_data_1),
            val_data_2: ValidatorSetData(val_data_2),
            next_val_data: None,
            val_set_update_interval,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }

    fn jank_update_valset(&mut self, seq_num: SeqNum) {
        if seq_num.is_epoch_end(self.val_set_update_interval) {
            if self.next_val_data.is_some() {
                error!("Validator set data is not consumed");
            }
            let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
            assert_eq!(
                locked_epoch,
                seq_num.to_epoch(self.val_set_update_interval) + Epoch(1)
            );
            self.next_val_data = if locked_epoch.0 % 2 == 0 {
                Some(ValidatorSetDataWithEpoch {
                    epoch: locked_epoch,
                    validators: self.val_data_1.clone(),
                })
            } else {
                Some(ValidatorSetDataWithEpoch {
                    epoch: locked_epoch,
                    validators: self.val_data_2.clone(),
                })
            };
        }
    }
}

impl<ST, SCT, EPT> MockableValSetUpdater for MockValSetUpdaterSwap<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Event = MonadEvent<ST, SCT, EPT>;
    type SignatureCollection = SCT;

    fn ready(&self) -> bool {
        self.next_val_data.is_some()
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
}

impl<ST, SCT, EPT> Executor for MockValSetUpdaterSwap<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Command = ValSetCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                ValSetCommand::NotifyFinalized(seq_num) => {
                    self.jank_update_valset(seq_num);
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

impl<ST, SCT, EPT> Stream for MockValSetUpdaterSwap<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        let event = if let Some(next_val_data) = this.next_val_data.take() {
            Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::<SCT>::UpdateValidators(next_val_data),
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
