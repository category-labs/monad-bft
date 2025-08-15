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
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::validator_data::{ValidatorSetDataWithEpoch, ValidatorsConfig};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, ValSetCommand};
use monad_types::{Epoch, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use tracing::error;

/// Updater that gets state root hash updates by polling triedb
pub struct ValSetUpdater<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    validators_path: PathBuf,

    next_val_data: Option<ValidatorSetDataWithEpoch<SCT>>,
    last_emitted_val_data: Option<SeqNum>,
    epoch_length: SeqNum,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    phantom: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> ValSetUpdater<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(triedb_path: &Path, validators_path: &Path, epoch_length: SeqNum) -> Self {
        // assert that validators_path is accessible
        let _: ValidatorsConfig<SCT> = ValidatorsConfig::read_from_path(validators_path)
            .expect("failed to read validators_path");

        // TODO read validator set from triedb
        let _path = triedb_path.to_path_buf();

        Self {
            validators_path: validators_path.to_owned(),

            next_val_data: None,
            last_emitted_val_data: None,
            epoch_length,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }

    fn valset_update(&mut self, seq_num: SeqNum) {
        if seq_num.is_epoch_end(self.epoch_length) && self.last_emitted_val_data != Some(seq_num) {
            if self.next_val_data.is_some() {
                error!("Validator set data is not consumed");
            }
            let locked_epoch = seq_num.get_locked_epoch(self.epoch_length);
            assert_eq!(locked_epoch, seq_num.to_epoch(self.epoch_length) + Epoch(1));
            self.next_val_data = Some(ValidatorSetDataWithEpoch {
                epoch: locked_epoch,
                validators: ValidatorsConfig::read_from_path(&self.validators_path)
                    // I'm hesitant to provide any fallback for this, because
                    // having the wrong validator set can be catastrophic.
                    //
                    // This file should never be manually edited anyways.
                    .expect("failed to read validators_path")
                    .get_validator_set(&locked_epoch)
                    .clone(),
            });
            self.last_emitted_val_data = Some(seq_num);
        }
    }
}

impl<ST, SCT> Stream for ValSetUpdater<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if let Some(next_val_data) = this.next_val_data.take() {
            return Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::UpdateValidators(next_val_data),
            )));
        }

        Poll::Pending
    }
}

impl<ST, SCT> Executor for ValSetUpdater<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = ValSetCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                ValSetCommand::NotifyFinalized(seq_num) => {
                    self.valset_update(seq_num);
                    wake = true;
                }
            }
        }

        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
