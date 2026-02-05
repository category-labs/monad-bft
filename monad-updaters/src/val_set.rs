// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::validator_data::{ValidatorSetData, ValidatorSetDataWithEpoch};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, ValSetCommand};
use monad_types::{Epoch, ExecutionProtocol, SeqNum};
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

/// An updater that immediately creates a StateRootHash update and
/// the ValidatorSetData for the next epoch when it receives a
/// ledger commit command.
/// Goal is to mimic the behaviour of execution receiving a commit
/// and generating the state root hash and updating the staking contract,
/// and sending it back to consensus.
pub struct MockValSetUpdaterNop<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // validator set updates
    genesis_validator_data: ValidatorSetData<SCT>,
    next_val_data: Option<ValidatorSetDataWithEpoch<SCT>>,
    epoch_length: SeqNum,

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
    pub fn new(genesis_validator_data: ValidatorSetData<SCT>, epoch_length: SeqNum) -> Self {
        Self {
            genesis_validator_data,
            next_val_data: None,
            epoch_length,

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
        if seq_num.is_epoch_end(self.epoch_length) {
            if self.next_val_data.is_some() {
                error!("Validator set data is not consumed");
            }
            let locked_epoch = seq_num.get_locked_epoch(self.epoch_length);
            assert_eq!(locked_epoch, seq_num.to_epoch(self.epoch_length) + Epoch(1));
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
        for command in commands {
            match command {
                ValSetCommand::NotifyFinalized(seq_num) => {
                    self.jank_update_valset(seq_num);
                }
            }
        }

        if self.ready() {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            };
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
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

        if let Some(next_val_data) = this.next_val_data.take() {
            return Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::<SCT>::UpdateValidators(next_val_data),
            )));
        }

        if let Some(waker) = this.waker.as_mut() {
            waker.clone_from(cx.waker());
        } else {
            this.waker = Some(cx.waker().clone());
        }

        Poll::Pending
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
    epoch_length: SeqNum,

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
    pub fn new(genesis_validator_data: ValidatorSetData<SCT>, epoch_length: SeqNum) -> Self {
        let num_validators = genesis_validator_data.0.len();
        let mut val_data_1 = genesis_validator_data.0.clone();
        let val_data_2 = val_data_1.split_off(num_validators / 2);

        Self {
            epoch: Epoch(1),
            genesis_val_data: genesis_validator_data,
            val_data_1: ValidatorSetData(val_data_1),
            val_data_2: ValidatorSetData(val_data_2),
            next_val_data: None,
            epoch_length,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }

    fn jank_update_valset(&mut self, seq_num: SeqNum) {
        if seq_num.is_epoch_end(self.epoch_length) {
            if self.next_val_data.is_some() {
                error!("Validator set data is not consumed");
            }
            let locked_epoch = seq_num.get_locked_epoch(self.epoch_length);
            assert_eq!(locked_epoch, seq_num.to_epoch(self.epoch_length) + Epoch(1));
            self.next_val_data = if locked_epoch.0.is_multiple_of(2) {
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
        if epoch.0.is_multiple_of(2) {
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
        for command in commands {
            match command {
                ValSetCommand::NotifyFinalized(seq_num) => {
                    self.jank_update_valset(seq_num);
                }
            }
        }

        if self.ready() {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            };
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
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

        if let Some(next_val_data) = this.next_val_data.take() {
            return Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::<SCT>::UpdateValidators(next_val_data),
            )));
        }

        if let Some(waker) = this.waker.as_mut() {
            waker.clone_from(cx.waker());
        } else {
            this.waker = Some(cx.waker().clone());
        }

        Poll::Pending
    }
}
